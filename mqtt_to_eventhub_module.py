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

# import importlib
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional, AsyncIterator

import aiomqtt
import paho.mqtt.client as mqtt_client
import requests
from azure.eventhub import EventData, EventDataBatch, EventHubProducerClient
from azure.eventhub.aio import EventHubProducerClient as EventHubProducerClientAsync
from azure.eventhub.exceptions import EventHubError
from dotenv_vault import load_dotenv

print(f"loading dotenv from {os.getcwd()}")
load_dotenv(verbose=True)
print("dotenv loaded")

# MQTT configuration
MQTT_LOGIN = os.environ.get("MQTT_LOGIN", None)
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", None)
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_BASE_TOPIC = os.environ.get("MQTT_BASE_TOPIC", "#")

# Azure Eventhub configuration
EVENTHUB_CONN_STR = os.environ["EVENTHUB_CONN_STR"]
EVENTHUB_NAME = os.environ["EVENTHUB_NAME"]

# check we have some envvars
assert EVENTHUB_NAME

MAX_EVENT_BATCH_SIZE_BYTES = int(os.environ.get("MAX_EVENT_BATCH_SIZE", 5120))

# optional configuraiton - if not set, will not poll
HEALTHCHECK_URL = os.environ.get("HEALTHCHECK_URL", None)
HEALTCHECK_FAILURE_URL = os.environ.get("HEALTCHECK_FAILURE_URL", HEALTHCHECK_URL)
HEALTHCHECK_INTERVAL = int(os.environ.get("HEALTHCHECK_INTERVAL", 60))
HEALTHCHECK_METHOD = os.environ.get("HEALTHCHECK_METHOD", "GET")
# if set to true, use HTTP POST to send error data to healthcheck
HEALTHCHECK_REPORT_ERRORS = (
    os.environ.get("HEALTHCHECK_REPORT_ERRORS", "true").lower() == "true"
)
# maximum time between between MQTT messages before we consider the source dead and signal an error
MQTT_TIMEOUT = int(os.environ.get("MQTT_TIMEOUT", 120))  # seconds

# time of last message received from MQTT; default to startup time to avoid false alarms
last_mqtt_message_time: datetime = time.time()


eventhub_producer_async: EventHubProducerClient = None


def setup_logger(logger_name: str, log_level: str) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.info("created logger")
    # Set the log level based on the environment variable value
    logger.setLevel(log_level)
    return logger


# Create a new logger
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logger = setup_logger(__name__, LOG_LEVEL)


def on_connect(client: aiomqtt.Client, _userdata, _flags, result_code: int):
    """This is the callback function that is called when the client connects to the MQTT server."""
    if result_code != mqtt_client.MQTT_ERR_SUCCESS:
        logger.error("Error connecting: %d", result_code)
        return
    logger.info("Connected - calling subscribe")
    subscribe(client)


def subscribe(client: aiomqtt.Client):
    result_code, _message_id = client.subscribe(MQTT_BASE_TOPIC)

    if result_code != mqtt_client.MQTT_ERR_SUCCESS:
        logger.error("Couldn't subscribe: %d", result_code)
        return
    logger.info("Connected and subscribed")


def process_message(message: aiomqtt.Message) -> Optional[dict]:
    try:
        logger.debug("attempting to extract message data")
        message_data = extract_data_from_message(message)
        json_data = json.dumps(message_data) if message_data else None
        logger.debug(f"data extracted: {json_data}")
        return json_data
    except Exception as e:
        log_error("Error extracting message", e)
        return None


async def add_to_batch(
    event_batch: EventDataBatch, serialized_object: str
) -> EventDataBatch:
    try:
        logger.debug("attempting to add to existing batch")
        event_batch.add(EventData(serialized_object))
        logger.debug("added to existing batch")
        return event_batch
    except ValueError:
        logger.debug("batch full, adding new batch")
        return await send_batch_and_create_new(event_batch)


async def send_batch_and_create_new(existing_batch: EventDataBatch) -> EventDataBatch:
    logger.debug("calling send_message_to_eventhub_async")
    await send_message_to_eventhub_async(eventhub_producer_async, existing_batch)
    logger.debug("creating new batch")
    new_batch = await eventhub_producer_async.create_batch(
        max_size_in_bytes=MAX_EVENT_BATCH_SIZE_BYTES
    )
    logger.debug("new batch created")
    return new_batch


async def on_message_async(
    _client: aiomqtt.Client,
    existing_event_batch: EventDataBatch,
    message: aiomqtt.Message,
) -> EventDataBatch:
    global last_mqtt_message_time
    last_mqtt_message_time = time.time()

    json_data = process_message(message)
    if not json_data:
        logger.error("json_data is empty")
        return existing_event_batch

    updated_batch = await add_to_batch(existing_event_batch, json_data)
    logger.info(f"total size of messages in queue {updated_batch.size_in_bytes}")
    return updated_batch


async def send_message_to_eventhub_async(
    producer: EventHubProducerClient, message_batch: EventDataBatch
):
    """
    sends a batch of messages to the event hub
    @param producer: the event hub producer
    @param message_batch: the batch of messages to send
    """
    try:
        logger.info("Sending queue of size %i", message_batch.size_in_bytes)
        await producer.send_batch(message_batch)
        logger.debug("batch sent successfully")
    except EventHubError as e:
        log_error("Error sending message to event hub", e)


def extract_data_from_message(message: aiomqtt.Message) -> Optional[dict]:
    """
    Creates a json object with the data and metadata.
    Raises exceptions for missing or invalid fields.
    @param message: the message received from the MQTT broker
    @return: the json object or None if extraction failed
    """

    if message is None:
        logger.error("Received null message")
        raise ValueError("Received null message")

    if message.topic is None or message.topic.value is None:
        error_message = f"Message topic or topic.value is missing: {serialize_message(message)}"
        logger.error(error_message)
        raise ValueError(error_message)
    if message.payload is None:
        error_message = f"Message payload is missing: {serialize_message(message)}"
        logger.error(error_message)
        raise ValueError(error_message)

    try:
        decoded_payload = message.payload.decode()
    except Exception as e:
        logger.error("Failed to decode payload: %s", e)
        raise

    if message.qos is None:
        error_message = f"Message qos is missing: {serialize_message(message)}"
        log_error(error_message)
            # raise ValueError(error_message)

    if message.retain is None:
        error_message = f"Message retain is missing: {serialize_message(message)}"
        log_error(error_message)
            # raise ValueError(error_message)

    data = {
        "topic": message.topic.value,
        "payload": decoded_payload,
        "qos": message.qos,
        "retain": message.retain,
        "timestamp": time.time(),
    }

    logger.debug(
        f"Extracted data: {data}",
    )
    return data


async def asyncLoop(eventhub_producer: EventHubProducerClient, client: aiomqtt.Client):
    """asyncLoop executes two async functions in parallel. The first function is the message loop
    that receives messages from the MQTT broker and sends them to the event hub.
    The second function is the healthcheck loop that polls a healthcheck endpoint if configured.

    @param eventhub_producer: the event hub producer
    @param client: the MQTT client

    """
    logger.debug("initiating asyncLoop")
    await asyncio.gather(
        message_loop(eventhub_producer, client),  # check for messages from MQTT broker
        poll_healthcheck_if_needed(
            {"continue": True}
        ),  # poll healthcheck endpoint if configured
        check_mqtt_timeout(),  # check for timeout of MQTT messages
    )


async def create_event_batch(
    producer: EventHubProducerClient, max_size_in_bytes: int, logger: logging.Logger
) -> EventDataBatch:
    logger.debug("Creating new event batch")
    return await producer.create_batch(max_size_in_bytes=max_size_in_bytes)


async def process_message_batch(
    client: aiomqtt.Client,
    producer: EventHubProducerClient,
    messages: AsyncIterator[aiomqtt.Message],
    logger: logging.Logger,
) -> None:
    event_batch = await create_event_batch(producer, MAX_EVENT_BATCH_SIZE_BYTES, logger)
    async for message in messages:
        event_batch = await on_message_async(client, event_batch, message)


async def message_loop(
    eventhub_producer: EventHubProducerClient, client: aiomqtt.Client
) -> None:
    logger.debug("Starting message loop")
    async with client:
        logger.debug(f"Subscribing to base topic: {MQTT_BASE_TOPIC}")
        await client.subscribe(MQTT_BASE_TOPIC)
        # async with client.messages() as messages:
        # see https://sbtinstruments.github.io/aiomqtt/migration-guide-v2.html#changes-to-the-message-queue
        await process_message_batch(client, eventhub_producer, client.messages, logger)


def log_error(error: Exception, *args) -> None:
    """
    logs an error and reports it to the healthcheck endpoint if configured
    @param error: the error to log
    @param args: additional arguments to log
    """
    # handle the error
    logger.debug("entered log_error")
    logger.debug("HEALTCHECK_FAILURE_URL: %s", HEALTCHECK_FAILURE_URL)
    logger.debug("HEALTHCHECK_REPORT_ERRORS: %s", HEALTHCHECK_REPORT_ERRORS)
    full_error = f"{error} {args}"
    logger.error(full_error)
    if HEALTCHECK_FAILURE_URL and HEALTHCHECK_REPORT_ERRORS:
        requests.post(HEALTCHECK_FAILURE_URL, data={"error": full_error})
        logger.debug("sent error report")


def poll_healthcheck():
    """
    polls the healthcheck endpoint if configured
    """
    logger.debug("entering poll_healthcheck")
    logger.debug(f"HEALTHCHECK_URL is set to: {HEALTHCHECK_URL}")
    if HEALTHCHECK_URL and HEALTHCHECK_METHOD:
        logger.debug(f"HEALTHCHECK_METHOD is set to: {HEALTHCHECK_METHOD}")
        if HEALTHCHECK_METHOD.upper() == "GET":
            requests.get(HEALTHCHECK_URL)
        elif HEALTHCHECK_METHOD.upper() == "POST":
            requests.post(HEALTHCHECK_URL)
        else:
            error_message = f"Unknown healthcheck method: {HEALTHCHECK_METHOD}"
            logger.error(error_message)
            raise Exception(error_message)
        logger.info("Healthcheck successful")


async def poll_healthcheck_if_needed(running: dict = {"continue": True}):
    """
    Polls the healthcheck endpoint if configured, then sleeps for HEALTHCHECK_INTERVAL seconds.
    The loop runs while the 'running' flag is set to True.
    we only add the running flag so that we can test this code.
    """
    logger.debug("Entering poll_healthcheck_if_needed")
    logger.debug(f"HEALTHCHECK_URL is set to: {HEALTHCHECK_URL}")
    if HEALTHCHECK_URL:
        while running["continue"]:
            poll_healthcheck()
            logger.debug(f"Sleeping for: {HEALTHCHECK_INTERVAL} seconds")
            await asyncio.sleep(HEALTHCHECK_INTERVAL)


async def check_mqtt_timeout(running: dict = {"continue": True}):
    """
    checks the last time that the MQTT client received a message
    if it is longer ago than MQTT_TIMEOUT, then the MQTT connection is considered to be timed out
    so we log an error. Note that we don't actually close the MQTT connection or try to reconnect
    or fix the problem. We just log an error.
    """
    while running["continue"]:
        if time.time() - last_mqtt_message_time > MQTT_TIMEOUT:
            log_error(
                f"No message received via MQTT for more than {MQTT_TIMEOUT} seconds - last message received at {last_mqtt_message_time}",
            )
        await asyncio.sleep(MQTT_TIMEOUT)


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


def reduce_log_level():
    # set up logging - reduce the log level to WARNING to reduce excessive logging i.e. SD wear
    logging.getLogger("uamqp").setLevel(
        logging.WARNING
    )  # Low level uAMQP are logged only for critical
    logging.getLogger("azure").setLevel(
        logging.WARNING
    )  # All azure clients are logged only for critical


def main():
    """
    main function
    """
    reduce_log_level()

    try:
        run_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(run_loop)
        client = get_client()

        # create an event hub producer client and mqtt client
        global eventhub_producer_async
        eventhub_producer_async = get_producer()

        logger.debug("created client")
        run_loop.run_until_complete(asyncLoop(eventhub_producer_async, client))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log_error(e)
    finally:
        logging.debug("closing run_loop")
        run_loop.close()
        logging.debug("run_loop closed")
        logging.debug("closing eventhub_producer_async")
        asyncio.run(eventhub_producer_async.close())
        logging.debug("eventhub_producer_async closed")
        logging.info("shutdown complete")


def get_producer():
    eventhub_producer_async = EventHubProducerClientAsync.from_connection_string(
        conn_str=EVENTHUB_CONN_STR,
        eventhub_name=EVENTHUB_NAME,
        buffered_mode=False,
        on_success=on_success_async,
        on_error=on_error,
    )

    logger.debug("created eventhub_producer_async")
    return eventhub_producer_async


def get_client():
    return aiomqtt.Client(
        hostname=MQTT_HOST,
        port=MQTT_PORT,
        username=MQTT_LOGIN,
        password=MQTT_PASSWORD,
    )


def serialize_message(message: aiomqtt.Message) -> str:
    message_info = {}

    for key, value in message.__dict__.items():
        # Skip private attributes
        if key[0] == "_":
            continue
        # Convert datetime instances to strings
        elif isinstance(value, datetime):
            message_info[key] = value.isoformat()
        # Decode payload if it's a bytes object
        elif key == "payload" and isinstance(value, bytes):
            message_info[key] = value.decode("utf-8", errors="ignore")
        # Convert topic to string
        elif key == "topic" and isinstance(value, aiomqtt.Topic):
            message_info[key] = value.value
        else:
            message_info[key] = value

    return json.dumps(message_info)
