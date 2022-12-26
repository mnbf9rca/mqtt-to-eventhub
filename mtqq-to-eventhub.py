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

# import typing

import asyncio_mqtt as aiomqtt
import paho.mqtt as mqtt

from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient
from azure.eventhub.aio import EventHubProducerClient as AsyncEventHubProducerClient
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


def on_connect(client: aiomqtt.Client, _userdata, _flags, result_code: int):
    if result_code != mqtt.MQTT_ERR_SUCCESS:
        logging.error("Error connecting: %d", result_code)
        return
    result_code, _message_id = client.subscribe(MQTT_BASE_TOPIC)

    if result_code != mqtt.MQTT_ERR_SUCCESS:
        logging.error("Couldn't subscribe: %d", result_code)
        return
    logging.info("Connected and subscribed")


def on_message(_client: aiomqtt.Client, _userdata, message: aiomqtt.Message):
    """
    This is the callback function that is called when a message is received
    receives any message from the MQTT broker
    creates a json object with the data and metadata and sends
    it to azure event hub
    """
    message_data = extract_data_from_message(message)
    json_data = json.dumps(message_data)
    send_message_to_eventhub(eventhub_producer, json_data)


def send_message_to_eventhub(producer: AsyncEventHubProducerClient, message: str):
    """
    Sends a message to the Azure Event Hub
    """

    with producer:
        # event_data_batch = await producer.create_batch()
        # event_data_batch.add(EventData(message))
        try:
            logging.info("Sending message to event hub: %s", message)
            producer.send_event(EventData(message))
            logging.info(
                "total messages in queue %i", producer.total_buffered_event_count
            )
            # producer.send_batch(event_data_batch)
        except EventHubError as e:
            logging.error("Error sending message to event hub: %s", e)


async def async_end_message_to_eventhub(
    producer: AsyncEventHubProducerClient, message: str
):
    """
    Sends a message to the Azure Event Hub
    """

    async with producer:
        # event_data_batch = await producer.create_batch()
        # event_data_batch.add(EventData(message))
        try:
            logging.info("Sending message to event hub: %s", message)
            await producer.send_event(EventData(message))
            logging.info(
                "total messages in queue %i", producer.total_buffered_event_count
            )
            # producer.send_batch(event_data_batch)
        except EventHubError as e:
            logging.error("Error sending message to event hub: %s", e)


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


def loop():
    logging.basicConfig(level=logging.DEBUG, format="%(message)s")
    client = mqtt.Client()

    if MQTT_LOGIN and MQTT_PASSWORD:
        client.username_pw_set(MQTT_LOGIN, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.subscribe(MQTT_BASE_TOPIC)
    if MQTT_PORT:
        client.connect(MQTT_HOST, MQTT_PORT)
    else:
        client.connect(MQTT_HOST)
    client.loop_forever()


async def asyncLoop(client: aiomqtt.Client):
    async with client:
        await client.subscribe(MQTT_BASE_TOPIC)
        async with client.messages() as messages:
            await client.subscribe(MQTT_BASE_TOPIC)
            async for message in messages:
                on_message(client, None, message)


def on_success(events, pid):
    # sending succeeded
    logger.info(events, pid)


def on_error(events, pid, error):
    # sending failed
    logger.error(events, pid, error)


eventhub_producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENTHUB_CONN_STR,
    eventhub_name=EVENTHUB_NAME,
    buffered_mode=True,
    max_wait_time=10,
    on_success=on_success,
    on_error=on_error,
)


client = aiomqtt.Client(
    hostname=MQTT_HOST,
    port=MQTT_PORT,
    username=MQTT_LOGIN,
    password=MQTT_PASSWORD,
)

logger = logging.getLogger("azure.eventhub")

if __name__ == "__main__":
    logger.setLevel(logging.WARNING)
    # loop()
    run_loop = asyncio.get_event_loop()
    run_loop.run_until_complete(asyncLoop(client))

# asyncio.run()
