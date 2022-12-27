"""
Purpose:
  To collect data pushed to MQTT (e.g. by a Hildebrand Glow Stick, hubitat 'homie' events,
  and emonhub) and push it to an Azure EventHub
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
    print(f"loading dotenv from {os.getcwd()}")
    from dotenv_vault import load_dotenv
    load_dotenv(verbose=True)

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
HEALTCHECK_FAILURE_URL = os.environ.get("HEALTCHECK_FAILURE_URL", HEALTHCHECK_URL)
HEALTHCHECK_INTERVAL = int(os.environ.get("HEALTHCHECK_INTERVAL", 60))
HEALTHCHECK_METHOD = os.environ.get("HEALTHCHECK_METHOD", "GET")
# if set to true, use HTTP POST to send error data to healthcheck
HEALTHCHECK_REPORT_ERRORS = (
    os.environ.get("HEALTHCHECK_REPORT_ERRORS", "True") == "True"
)


def on_connect(client: aiomqtt.Client, _userdata, _flags, result_code: int):
    """This is the callback function that is called when the client connects to the MQTT server."""
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
    existing_event_batch: EventDataBatch,
    message: aiomqtt.Message,
) -> EventDataBatch:
    """
    This is the callback function that is called when a message is received
    receives any message from the MQTT broker
    creates a json object with the data and metadata and adds it to the event batch
    if the batch is full, sends it to the event hub and creates a new batch

    @param client: the MQTT client
    @param this_event_batch: the current batch of messages to send to the event hub
    @param message: the message received from the MQTT broker
    @return: the updated or new batch of messages to send to the event hub
    """
    try:
        message_data = extract_data_from_message(message)
        json_data = json.dumps(message_data)

    except Exception as e:
        log_error("Error sending message", e)
    try:
        existing_event_batch.add(EventData(json_data))
    except ValueError:
        # batch is full, send it and start a new one
        await send_message_to_eventhub_async(eventhub_producer_async, existing_event_batch)
        new_event_batch: EventDataBatch = await eventhub_producer_async.create_batch(
            max_size_in_bytes=MAX_EVENT_BATCH_SIZE_BYTES
        )
        new_event_batch.add(EventData(json_data))
        return new_event_batch
    logging.info("total size of messages in queue %i", existing_event_batch.size_in_bytes)
    return existing_event_batch


async def send_message_to_eventhub_async(
    producer: EventHubProducerClient, message_batch: EventDataBatch
):
    """
    sends a batch of messages to the event hub
    @param producer: the event hub producer
    @param message_batch: the batch of messages to send
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
    """
    creates a json object with the data and metadata
    @param message: the message received from the MQTT broker
    @return: the json object
    """

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
    """asyncLoop executes two async functions in parallel. The first function is the message loop
    that receives messages from the MQTT broker and sends them to the event hub.
    The second function is the healthcheck loop that polls a healthcheck endpoint if configured.

    @param eventhub_producer: the event hub producer
    @param client: the MQTT client

    """
    await asyncio.gather(
        message_loop(eventhub_producer, client),
        poll_healthceck_if_needed(),
    )


async def message_loop(
    eventhub_producer: EventHubProducerClient, client: aiomqtt.Client
):
    """
    message_loop receives messages from the MQTT broker and sends them to the event hub.
    @param eventhub_producer: the event hub producer
    @param client: the MQTT client
    """
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
    """
    logs an error and reports it to the healthcheck endpoint if configured
    @param error: the error to log
    @param args: additional arguments to log
    """
    # handle the error
    full_error = f"{error} {args}"
    logger.error(full_error)
    if HEALTCHECK_FAILURE_URL:
        if HEALTHCHECK_REPORT_ERRORS:
            requests.post(HEALTCHECK_FAILURE_URL, data={"error": full_error})


def poll_healthcheck():
    """
    polls the healthcheck endpoint if configured
    """
    if HEALTHCHECK_URL:
        try:
            if HEALTHCHECK_METHOD.upper() == "GET":
                requests.get(HEALTHCHECK_URL)
            elif HEALTHCHECK_METHOD.upper() == "POST":
                requests.post(HEALTHCHECK_URL)
            else:
                raise Exception("Unknown healthcheck method: %s", HEALTHCHECK_METHOD)
            logger.info("Healthcheck successful")
        except Exception as e:
            log_error(e)


async def poll_healthceck_if_needed():
    """
    polls the healthcheck endpoint if configured
    then sleeps for HEALTHCHECK_INTERVAL seconds
    """
    if HEALTHCHECK_URL:
        while True:
            poll_healthcheck()
            await asyncio.sleep(HEALTHCHECK_INTERVAL)


async def on_success_async(events, pid):
    """
    this is the success handler for the event hub producer
    """
    # sending succeeded
    logger.info(events, pid)


def on_error(events, pid, error):
    """
    this is the error handler for the event hub producer
    """
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


client = aiomqtt.Client(
    hostname=MQTT_HOST,
    port=MQTT_PORT,
    username=MQTT_LOGIN,
    password=MQTT_PASSWORD,
)

logger = logging.getLogger()

if __name__ == "__main__":
    """
    main function
    """

    # set up logging - reduce the log level to WARNING to reduce excessive logging i.e. SD wear
    logger.setLevel(logging.WARNING)
    logging.getLogger("uamqp").setLevel(logging.WARNING)  # Low level uAMQP are logged only for critical
    logging.getLogger("azure").setLevel(logging.WARNING)  # All azure clients are logged only for critical

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
