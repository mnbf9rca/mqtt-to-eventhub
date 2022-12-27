"""
Purpose:
  To collect data pushed to MQTT by a Hildebrand Glow Stick, hubitat 'homie' events,
  and emonhub and push it to an Azure EventHub
  From there, it can be picked up and pushed to a timescale database
author: rob aleck github.com/mnbf9rca
inspired by: 
  https://github.com/Energy-Sparks/energy-sparks_analytics/blob/782865e108e5c61a5b4bae647ba8d0c32ba3c6ef/script/meters/glow_mqtt_example.py
licence: MIT
"""
import asyncio
import importlib
import json
import logging
import os
import time

import asyncio_mqtt as aiomqtt
import paho.mqtt as mqtt
import requests

from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventDataBatch
from azure.eventhub.aio import EventHubProducerClient as EventHubProducerClientAsync
from azure.eventhub.exceptions import EventHubError


# load dotenv only if it's available, otherwise assume environment variables are set
dotenv_spec = importlib.util.find_spec("dotenv_vault")
if dotenv_spec is not None:
    print("loading dotenv")
    from dotenv_vault import load_dotenv

    load_dotenv()

# MQTT configuration
MQTT_LOGIN = os.environ.get("MQTT_LOGIN", None)
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", None)
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_BASE_TOPIC = os.environ.get("MQTT_BASE_TOPIC", "#")

# Azure Eventhub configuration
EVENTHUB_CONN_STR = os.environ["EVENTHUB_CONN_STR"]
EVENTHUB_NAME = os.environ["EVENTHUB_NAME"]
MAX_EVENT_BATCH_SIZE_BYTES = int(os.environ.get("MAX_EVENT_BATCH_SIZE", 5120))

# optional configuraiton - if not set, will not poll
HEALTHCHECK_URL = os.environ.get("HEALTHCHECK_URL", None)
HEALTHCHECK_INTERVAL = int(os.environ.get("HEALTHCHECK_INTERVAL", 60))
HEALTHCHECK_METHOD = os.environ.get("HEALTHCHECK_METHOD", "GET")
# if set to true, use HTTP POST to send error data to healthcheck
HEALTHCHECK_REPORT_ERRORS = (
    os.environ.get("HEALTHCHECK_REPORT_ERRORS", "True") == "True"
)


def on_connect(client: aiomqtt.Client, _userdata, _flags, result_code: int):
    if result_code != mqtt.MQTT_ERR_SUCCESS:
        logging.error("Error connecting: %d", result_code)
        return
    result_code, _message_id = client.subscribe(MQTT_BASE_TOPIC)

    if result_code != mqtt.MQTT_ERR_SUCCESS:
        logging.error("Couldn't subscribe: %d", result_code)
        return
    logging.info("Connected and subscribed")


async def on_message_async(
    _client: aiomqtt.Client,
    this_event_batch: EventDataBatch,
    message: aiomqtt.Message,
):
    """
    This is the callback function that is called when a message is received
    receives any message from the MQTT broker
    creates a json object with the data and metadata and sends
    it to azure event hub
    """
    try:
        message_data = extract_data_from_message(message)
        json_data = json.dumps(message_data)

    except Exception as e:
        log_error("Error sending message", e)
    try:
        this_event_batch.add(EventData(json_data))
    except ValueError:
        # batch is full, send it and start a new one
        await send_message_to_eventhub_async(eventhub_producer_async, this_event_batch)
        event_batch: EventDataBatch = await eventhub_producer_async.create_batch(
            max_size_in_bytes=MAX_EVENT_BATCH_SIZE_BYTES
        )
        event_batch.add(EventData(json_data))
        return event_batch
    logging.info("total size of messages in queue %i", this_event_batch.size_in_bytes)
    return this_event_batch


async def send_message_to_eventhub_async(producer: EventHubProducerClient, message_batch: EventDataBatch):
    """
    Checks if event_buffer.size_in_bytes   Sends a message to the Azure Event Hub
    """

    # async with producer:
    # event_data_batch = await producer.create_batch()
    # event_data_batch.add(EventData(message))
    try:
        logging.info("Sending queue of size %i", message_batch.size_in_bytes)
        await producer.send_batch(message_batch)

    except EventHubError as e:
        log_error("Error sending message to event hub", e)


def extract_data_from_message(message: aiomqtt.Message) -> dict:
    logging.info("Received message: %s", message.payload)
    logging.info("Topic: %s", message.topic)
    logging.info("QoS: %s", message.qos)
    logging.info("Retain flag: %s", message.retain)

    # create a json object with the data and metadata
    data = {
        "topic": message.topic.value,
        "payload": message.payload.decode(),
        "qos": message.qos,
        "retain": message.retain,
        "timestamp": time.time(),
    }
    return data


async def asyncLoop(eventhub_producer: EventHubProducerClient, client: aiomqtt.Client):
    # create a new event batch
    event_batch = await eventhub_producer.create_batch(
        max_size_in_bytes=MAX_EVENT_BATCH_SIZE_BYTES
    )
    async with client:
        await client.subscribe(MQTT_BASE_TOPIC)
        async with client.messages() as messages:
            await client.subscribe(MQTT_BASE_TOPIC)
            async for message in messages:
                event_batch = await on_message_async(client, event_batch, message)


def log_error(error: Exception, *args) -> None:
    # handle the error
    full_error = f"{error} {args}"
    logger.error(full_error)
    if HEALTHCHECK_URL:
        if HEALTHCHECK_REPORT_ERRORS:
            requests.post(HEALTHCHECK_URL, data={"error": full_error})


def poll_healthcheck():
    if HEALTHCHECK_URL:
        try:
            if HEALTHCHECK_METHOD == "GET":
                requests.get(HEALTHCHECK_URL)
            elif HEALTHCHECK_METHOD == "POST":
                requests.post(HEALTHCHECK_URL)
            else:
                logger.error("Unknown healthcheck method: %s", HEALTHCHECK_METHOD)
        except Exception as e:
            log_error(e)


async def on_success_async(events, pid):
    # sending succeeded
    logger.info(events, pid)


def on_error(events, pid, error):
    # sending failed
    log_error(events, pid, error)


# create an event hub producer client and mqtt client


eventhub_producer_async: EventHubProducerClient = (
    EventHubProducerClientAsync.from_connection_string(
        conn_str=EVENTHUB_CONN_STR,
        eventhub_name=EVENTHUB_NAME,
        buffered_mode=False,
        on_success=on_success_async,
        on_error=on_error,
    )
)

eventhub_producer: EventHubProducerClient = (
    EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONN_STR,
        eventhub_name=EVENTHUB_NAME,
        buffered_mode=False,
        on_success=on_success_async,
        on_error=on_error,
    )
)
event_batch: EventDataBatch = eventhub_producer.create_batch(
    max_size_in_bytes=MAX_EVENT_BATCH_SIZE_BYTES
)
print("initial event batch created: ", type(event_batch))
eventhub_producer.close()

client = aiomqtt.Client(
    hostname=MQTT_HOST,
    port=MQTT_PORT,
    username=MQTT_LOGIN,
    password=MQTT_PASSWORD,
)

logger = logging.getLogger("azure.eventhub")

if __name__ == "__main__":
    logger.setLevel(logging.WARNING)

    try:
        run_loop = asyncio.get_event_loop()
        run_loop.run_until_complete(
            asyncLoop(eventhub_producer_async, client)
        )  # , event_batch))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log_error(e)
    finally:
        run_loop.close()
        asyncio.run(eventhub_producer_async.close())
