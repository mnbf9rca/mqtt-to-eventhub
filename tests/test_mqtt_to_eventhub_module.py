import asyncio
import json
import time
import pytest
import aiomqtt
import datetime
from logging import Logger, WARNING
from unittest.mock import Mock, AsyncMock, MagicMock, patch, call, ANY

import paho.mqtt.client as mqtt_client
from azure.eventhub import EventData, EventDataBatch, EventHubProducerClient

from azure.eventhub.exceptions import EventHubError
from freezegun import freeze_time

import mqtt_to_eventhub_module


class TestBasicConnectivity:
    @patch("mqtt_to_eventhub_module.aiomqtt.Client")
    @patch("mqtt_to_eventhub_module.subscribe")
    @patch("mqtt_to_eventhub_module.logger")
    def test_on_connect(self, mock_logger, mock_subscribe, mock_client):
        mqtt_to_eventhub_module.on_connect(
            mock_client, None, None, mqtt_client.MQTT_ERR_SUCCESS
        )
        mock_logger.info.assert_called_with("Connected - calling subscribe")
        mock_subscribe.assert_called_with(mock_client)

    @patch("mqtt_to_eventhub_module.aiomqtt.Client")
    @patch("mqtt_to_eventhub_module.subscribe")
    @patch("mqtt_to_eventhub_module.logger")
    def test_on_connect_failed(self, mock_logger, mock_subscribe, mock_client):
        mqtt_to_eventhub_module.on_connect(
            mock_client, None, None, mqtt_client.MQTT_ERR_PROTOCOL
        )
        mock_logger.error.assert_called()
        mock_subscribe.assert_not_called()

    @patch("mqtt_to_eventhub_module.logger")
    def test_successful_subscribe(self, mock_logger):
        # Arrange
        mock_client = MagicMock()
        mock_client.subscribe.return_value = (mqtt_client.MQTT_ERR_SUCCESS, None)

        # Act
        mqtt_to_eventhub_module.subscribe(mock_client)

        # Assert
        mock_logger.info.assert_called_with("Connected and subscribed")

    @patch("mqtt_to_eventhub_module.logger")
    def test_failed_subscribe(self, mock_logger):
        # Arrange
        mock_client = MagicMock()
        mock_client.subscribe.return_value = (mqtt_client.MQTT_ERR_PROTOCOL, None)

        # Act
        mqtt_to_eventhub_module.subscribe(mock_client)

        # Assert
        mock_logger.error.assert_called()


class TestActualConnections:
    @pytest.mark.asyncio
    async def test_real_mqtt_connection(self):
        # Call the function you are testing
        client = mqtt_to_eventhub_module.get_client()

        # Async function to handle connection
        async def on_connect_async(client, _userdata, _flags, rc):
            assert rc == 0  # 0 means successful connection

        client.on_connect = on_connect_async

        await client.connect()  # .connect_async(MQTT_HOST, MQTT_PORT)
        await asyncio.sleep(1)  # give it a second to connect

        # Disconnect
        await client.disconnect()

    @pytest.mark.asyncio
    @patch("mqtt_to_eventhub_module.on_success_async", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.on_error")
    async def test_real_eventhub_connection(self, mock_on_error, mock_on_success):
        # Call the function you are testing
        producer = mqtt_to_eventhub_module.get_producer()

        try:
            # Create an event batch
            event_batch: EventDataBatch = await producer.create_batch()

            # Add an event to the batch
            test_event = EventData("Sample Event Data")
            event_batch.add(test_event)

            # Send the batch of events to the event hub
            await producer.send_batch(event_batch)
            assert mock_on_error.call_count == 0
            assert mock_on_success.call_count == 1
            assert mock_on_success.call_args[0][0] == [test_event]

        except Exception as e:
            pytest.fail(f"Failed to send event batch: {e}")

        finally:
            # Close the producer
            await producer.close()




class TestAsyncLoop:
    @pytest.mark.asyncio
    @patch("mqtt_to_eventhub_module.message_loop", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.poll_healthcheck_if_needed", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.check_mqtt_timeout", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.asyncio.gather", new_callable=AsyncMock)
    async def test_asyncLoop_calls_gather_with_functions(
        self,
        mock_gather,
        _mock_check_mqtt_timeout,
        _mock_poll_healthcheck_if_needed,
        _mock_message_loop,
    ):
        # Mocked eventhub_producer and client
        mock_eventhub_producer = "mock_eventhub_producer"
        mock_client = "mock_client"

        # Call the function
        await mqtt_to_eventhub_module.asyncLoop(mock_eventhub_producer, mock_client)

        # Verify that asyncio.gather is called
        assert mock_gather.call_count == 1

        # Verify it is called with the correct number of coroutines
        # Have to do this rather than compare the objects passed
        # because the mock coroutine objects are not
        # identical to the ones that asyncio.gather is called with. This
        # is expected because every time you 'await' a coroutine,
        # a new coroutine object is created.
        actual_coroutines = mock_gather.call_args[0]
        assert len(actual_coroutines) == 3


class TestExtractDataFromMessage:
    @pytest.fixture
    def default_message(self) -> aiomqtt.Message:
        return aiomqtt.Message(
            topic="test_topic",
            payload=b"test_payload",
            qos=0,
            retain=False,
            mid=0,
            properties={},
        )

    @pytest.mark.parametrize("attribute, error_message", [
        ("topic", "Message topic or topic.value is missing"),
        ("payload", "Message payload is missing"),
        ("qos", "Message QoS is missing"),
        ("retain", "Message retain flag is missing"),
    ])
    def test_attribute_is_none(self, default_message: aiomqtt.Message, attribute: str, error_message: str):
        setattr(default_message, attribute, None)
        with pytest.raises(ValueError, match=error_message):
            mqtt_to_eventhub_module.extract_data_from_message(default_message)

    def test_all_attributes_present(self, default_message: aiomqtt.Message):
        result = mqtt_to_eventhub_module.extract_data_from_message(default_message)
        assert result["topic"] == default_message.topic.value
        assert result["payload"] == default_message.payload.decode("utf-8")
        assert result["qos"] == default_message.qos
        assert result["retain"] is default_message.retain
        assert "timestamp" in result

    def test_message_is_none(self):
        with pytest.raises(ValueError, match="Received null message"):
            mqtt_to_eventhub_module.extract_data_from_message(None)

    def test_payload_decoding_failure(self, default_message):
        default_message.payload = b"\x80abc"  # Non-decodable bytes
        with pytest.raises(UnicodeDecodeError):
            mqtt_to_eventhub_module.extract_data_from_message(default_message)


class TestProcessMessage:
    @pytest.mark.asyncio
    @freeze_time("2023-09-16 15:56:00")
    @patch("mqtt_to_eventhub_module.eventhub_producer_async", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.send_message_to_eventhub_async")
    @patch("mqtt_to_eventhub_module.aiomqtt.Client")
    @patch("mqtt_to_eventhub_module.aiomqtt.Message")
    async def test_on_message_async_with_empty_batch(
        self, mock_message, mock_client, mock_send_message, mock_producer
    ):
        mock_event_data_batch = mock_client.create_batch(
            max_size_in_bytes=mqtt_to_eventhub_module.MAX_EVENT_BATCH_SIZE_BYTES
        )
        mock_message.payload = b"message_payload"
        mock_message.topic.value = "topic_name"
        mock_message.qos = 0
        mock_message.retain = 1

        expected_event_body = {
            "topic": mock_message.topic.value,
            "payload": mock_message.payload.decode("utf-8"),
            "qos": mock_message.qos,
            "retain": mock_message.retain,
            "timestamp": 1694879760.0,
        }

        expected_event_data = EventData(json.dumps(expected_event_body))
        expected_event_data_json = expected_event_data.body_as_json("utf-8")

        await mqtt_to_eventhub_module.on_message_async(
            mock_client, mock_event_data_batch, mock_message
        )

        assert mock_send_message.call_count == 0
        actual_call = mock_event_data_batch.add.call_args
        actual_event_data = actual_call[0][
            0
        ]  # Assuming add is called with one positional argument
        actual_event_data_json = actual_event_data.body_as_json("utf-8")
        assert expected_event_data_json == actual_event_data_json

    @pytest.mark.asyncio
    @freeze_time("2023-09-16 15:56:00")
    @patch("mqtt_to_eventhub_module.extract_data_from_message")
    @patch("mqtt_to_eventhub_module.logger")
    @patch("mqtt_to_eventhub_module.eventhub_producer_async", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.send_message_to_eventhub_async")
    @patch("mqtt_to_eventhub_module.aiomqtt.Client")
    async def test_on_message_async_with_empty_message(
        self,
        mock_client,
        mock_send_message,
        mock_producer,
        mock_logger,
        mock_extractor,
    ):
        mock_event_data_batch = mock_client.create_batch(
            max_size_in_bytes=mqtt_to_eventhub_module.MAX_EVENT_BATCH_SIZE_BYTES
        )
        dummy_message = None

        mock_extractor.return_value = None

        await mqtt_to_eventhub_module.on_message_async(
            mock_client, mock_event_data_batch, dummy_message
        )

        assert mock_send_message.call_count == 0
        assert mock_logger.error.call_count == 1
        assert mock_logger.error.call_args[0][0] == "json_data is empty"
        assert mock_producer.create_batch.call_count == 0

    @pytest.mark.asyncio
    @freeze_time("2023-09-16 15:56:00")
    @patch("mqtt_to_eventhub_module.logger")
    @patch("mqtt_to_eventhub_module.eventhub_producer_async", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.send_message_to_eventhub_async")
    @patch("mqtt_to_eventhub_module.aiomqtt.Client")
    @patch("mqtt_to_eventhub_module.aiomqtt.Message")
    async def test_on_message_async_with_malformed_message(
        self, mock_message, mock_client, mock_send_message, mock_producer, mock_logger
    ):
        mock_event_data_batch = mock_client.create_batch(
            max_size_in_bytes=mqtt_to_eventhub_module.MAX_EVENT_BATCH_SIZE_BYTES
        )
        mock_message.payload = b"message_payload"
        mock_message.topic.value = "topic_name"
        mock_message.qos = 0
        mock_message.retain = None

        await mqtt_to_eventhub_module.on_message_async(
            mock_client, mock_event_data_batch, mock_message
        )

        assert mock_send_message.call_count == 0
        assert (
            mock_logger.error.call_count == 2
        )  # 1 inside extract_data_from_message and 1 here
        assert (
            "Error extracting message (ValueError('Message retain flag is missing: " in mock_logger.error.call_args[0][0]
        )
        assert mock_producer.create_batch.call_count == 0

    @pytest.mark.asyncio
    @freeze_time("2023-09-16 15:56:00")
    @patch("mqtt_to_eventhub_module.eventhub_producer_async", new_callable=AsyncMock)
    @patch(
        "mqtt_to_eventhub_module.send_message_to_eventhub_async", new_callable=AsyncMock
    )
    @patch("mqtt_to_eventhub_module.aiomqtt.Client")
    @patch("mqtt_to_eventhub_module.aiomqtt.Message")
    async def test_on_message_async_with_full_batch(
        self, mock_message, mock_client, mock_send_message, mock_producer
    ):
        mock_event_data_batch = Mock()
        mock_event_data_batch.add.side_effect = ValueError("Batch is full")

        # Create a mock for the new batch
        new_mock_event_data_batch = mock_client.create_batch(
            max_size_in_bytes=mqtt_to_eventhub_module.MAX_EVENT_BATCH_SIZE_BYTES
        )

        # Patch the create_batch method to return the new mock batch
        mock_producer.create_batch.return_value = new_mock_event_data_batch

        mock_message.payload = b"message_payload"
        mock_message.topic.value = "topic_name"
        mock_message.qos = 0
        mock_message.retain = 1

        expected_event_body = {
            "topic": mock_message.topic.value,
            "payload": mock_message.payload.decode("utf-8"),
            "qos": mock_message.qos,
            "retain": mock_message.retain,
            "timestamp": 1694879760.0,
        }

        expected_event_data = EventData(json.dumps(expected_event_body))
        expected_event_data_json = expected_event_data.body_as_json("utf-8")

        await mqtt_to_eventhub_module.on_message_async(
            mock_client, mock_event_data_batch, mock_message
        )

        # check existing patch was called
        assert mock_event_data_batch.add.call_count == 1

        # check that send_message_to_eventhub_async was called
        mock_send_message.assert_called_with(mock_producer, mock_event_data_batch)

        # Assert that a new batch was created
        assert mock_producer.create_batch.call_count == 1

        actual_call = new_mock_event_data_batch.add.call_args
        # Assuming add is called with one positional argument
        actual_event_data = actual_call[0][0]
        actual_event_data_json = actual_event_data.body_as_json("utf-8")
        assert expected_event_data_json == actual_event_data_json


class TestSendToEventHub:
    @pytest.mark.asyncio
    @patch("mqtt_to_eventhub_module.EventHubProducerClient", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.EventDataBatch")
    async def test_send_message_to_eventhub_async_succeeds(
        self, mock_event_data_batch, mock_producer
    ):
        await mqtt_to_eventhub_module.send_message_to_eventhub_async(
            mock_producer, mock_event_data_batch
        )

        mock_producer.send_batch.assert_called_with(mock_event_data_batch)

    @pytest.mark.asyncio
    @patch("mqtt_to_eventhub_module.log_error")
    @patch("mqtt_to_eventhub_module.EventHubProducerClient", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.EventDataBatch")
    async def test_send_message_to_eventhub_async_fails(
        self, mock_event_data_batch, mock_producer, mock_log_error
    ):
        mock_producer.send_batch.side_effect = EventHubError("Test EventHubError")

        # Check if the exception is caught and log_error is called
        await mqtt_to_eventhub_module.send_message_to_eventhub_async(
            mock_producer, mock_event_data_batch
        )

        mock_producer.send_batch.assert_called_with(mock_event_data_batch)
        mock_log_error.assert_called_with(
            "Error sending message to event hub", mock_producer.send_batch.side_effect
        )


class TestHealthCheck:
    @pytest.mark.parametrize(
        "healthcheck_url, healthcheck_method, requests_func, expected_call, expected_exception",
        [
            ("http://healthcheck", "GET", "get", True, False),
            ("http://healthcheck", "POST", "post", True, False),
            ("http://healthcheck", "get", "get", True, False),
            ("http://healthcheck", "post", "post", True, False),            
            ("http://healthcheck", "PUT", None, False, True),  # Invalid method, should raise an exception
            (None, "GET", "get", False, False),  # No URL set, should not call any request function
            ("http://healthcheck", None, "get", False, False),  # No METHOD set, should not call any request function
        ]
    )
    @patch("mqtt_to_eventhub_module.requests.get")
    @patch("mqtt_to_eventhub_module.requests.post")
    @patch("mqtt_to_eventhub_module.logger")
    def test_poll_healthcheck(self,
                              mock_logger,
                              mock_post,
                              mock_get,
                              healthcheck_url,
                              healthcheck_method,
                              requests_func,
                              expected_call,
                              expected_exception):
        mqtt_to_eventhub_module.HEALTHCHECK_URL = healthcheck_url
        mqtt_to_eventhub_module.HEALTHCHECK_METHOD = healthcheck_method
        
        if expected_exception:
            with pytest.raises(Exception) as exc_info:
                mqtt_to_eventhub_module.poll_healthcheck()
            assert str(exc_info.value) == f"Unknown healthcheck method: {healthcheck_method}"
        else:
            mqtt_to_eventhub_module.poll_healthcheck()
            if expected_call:
                getattr(mqtt_to_eventhub_module.requests, requests_func).assert_called_with(healthcheck_url)
                mock_logger.info.assert_called_with("Healthcheck successful")
            else:
                mock_get.assert_not_called()
                mock_post.assert_not_called()
                mock_logger.info.assert_not_called()

        mock_logger.debug.assert_called()


class TestPollHealthCheckIfNeeded:
    @pytest.mark.asyncio
    @patch('mqtt_to_eventhub_module.asyncio.sleep', new_callable=AsyncMock)
    @patch('mqtt_to_eventhub_module.poll_healthcheck')
    @patch('mqtt_to_eventhub_module.logger')
    async def test_poll_healthcheck_if_needed(self, mock_logger, mock_poll_healthcheck, mock_sleep):
        mqtt_to_eventhub_module.HEALTHCHECK_URL = "http://healthcheck"
        mqtt_to_eventhub_module.HEALTHCHECK_INTERVAL = 1
    
        running = {"continue": True}

        # Define a side effect for the mock_sleep to stop the loop after first call
        async def sleep_side_effect(*args, **kwargs):
            running["continue"] = False
            return True
        
        mock_sleep.side_effect = sleep_side_effect
    
        # Run poll_healthcheck_if_needed and await its completion
        await mqtt_to_eventhub_module.poll_healthcheck_if_needed(running)
    
        # Assert that poll_healthcheck was called at least once
        mock_poll_healthcheck.assert_called()

        # Assert that asyncio.sleep was called with the correct interval
        mock_sleep.assert_called_with(mqtt_to_eventhub_module.HEALTHCHECK_INTERVAL)


        # Check the logs
        mock_logger.debug.assert_has_calls([
            call("Entering poll_healthcheck_if_needed"),
            call(f"HEALTHCHECK_URL is set to: {mqtt_to_eventhub_module.HEALTHCHECK_URL}"),
            call(f"Sleeping for: {mqtt_to_eventhub_module.HEALTHCHECK_INTERVAL} seconds")
        ], any_order=True)


class TestCheckMqttTimeout:
    @pytest.mark.asyncio
    @patch("mqtt_to_eventhub_module.log_error")
    @patch(
        "mqtt_to_eventhub_module.MQTT_TIMEOUT", new=1
    )  # Override the MQTT_TIMEOUT to 1 second for testing
    async def test_check_mqtt_timeout(self, mock_log_error):
        # Given that the last message time is far in the past
        mqtt_to_eventhub_module.last_mqtt_message_time = (
            time.time() - 1000
        )  # 1000 seconds ago

        # Run the function for a short amount of time (e.g., 1.1 seconds)
        task = asyncio.create_task(mqtt_to_eventhub_module.check_mqtt_timeout())
        await asyncio.sleep(1.1)
        task.cancel()

        # Verify that log_error was called with the appropriate message
        mock_log_error.assert_called_with(
            "No message received via MQTT for more than %s seconds - last message received at %s",
            1,  # MQTT_TIMEOUT
            mqtt_to_eventhub_module.last_mqtt_message_time,
        )

class TestSerializeMessage:
    @pytest.mark.parametrize("topic, payload, qos, retain, expected", [
        (aiomqtt.Topic("test/topic"), b"test payload", 0, False, '{"mid": 0, "properties": {}, "topic": "test/topic", "payload": "test payload", "qos": 0, "retain": false}'),
        (aiomqtt.Topic("test/topic"), "test payload", 1, True, '{"mid": 0, "properties": {}, "topic": "test/topic", "payload": "test payload", "qos": 1, "retain": true}'),
        (aiomqtt.Topic("test/empty"), b"", 0, False, '{"mid": 0, "properties": {}, "topic": "test/empty", "payload": "", "qos": 0, "retain": false}'),
    ])
    def test_serialize_message(self, topic, payload, qos, retain, expected):
        message = aiomqtt.Message(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            mid=0,
            properties={},
        )

        result = mqtt_to_eventhub_module.serialize_message(message)
        assert json.loads(result) == json.loads(expected)

    def test_serialize_message_with_datetime(self):
        timestamp = datetime.datetime(2021, 1, 1, 12, 0)
        message = MagicMock(spec=aiomqtt.Message)
        message.__dict__ = {
            'timestamp': timestamp
        }
        result = mqtt_to_eventhub_module.serialize_message(message)
        expected = '{"timestamp": "' + timestamp.isoformat() + '"}'
        assert json.loads(result) == json.loads(expected)

    def test_serialize_message_skips_private_attributes(self):
        message = MagicMock(spec=aiomqtt.Message)
        message.__dict__ = {
            '_private': 'should not be serialized',
            'public': 'should be serialized'
        }
        result = mqtt_to_eventhub_module.serialize_message(message)
        assert '_private' not in json.loads(result)
        assert 'public' in json.loads(result)


# Constants used in message_loop


class TestMessageLoop:
    PATCHED_BASE_TOPIC = "test/topic"
    @patch('mqtt_to_eventhub_module.MQTT_BASE_TOPIC', PATCHED_BASE_TOPIC)
    @patch('mqtt_to_eventhub_module.process_message_batch')
    @patch('mqtt_to_eventhub_module.logger')
    @pytest.mark.asyncio
    async def test_message_loop(self, logger_mock, process_message_batch_mock):
        # Mock the EventHubProducerClient
        # not used in this function so dont need to do more than mock it
        eventhub_producer_mock = AsyncMock()

        # Mock the aiomqtt.Client
        client_mock = AsyncMock(spec=aiomqtt.Client)
        client_mock.subscribe = AsyncMock()

        # Mock the messages context manager to be an async generator
        messages_mock = AsyncMock()
        async def messages_async_gen():
            for msg in [
                AsyncMock()
                # aiomqtt.Message(topic=self.PATCHED_BASE_TOPIC, payload=b"test", qos=0, retain=False, mid=0, properties={}),
            ]:
                yield msg
        expected_messages_generator = messages_async_gen()
        # Use AsyncMock to return an async generator when entering the context manager
        messages_mock.__aenter__.return_value = expected_messages_generator
        client_mock.messages.return_value = messages_mock

        # Run the message loop with the mocked objects
        await mqtt_to_eventhub_module.message_loop(eventhub_producer_mock, client_mock)

        # Assert that subscribe was called correctly
        client_mock.subscribe.assert_awaited_once_with(self.PATCHED_BASE_TOPIC)

        # check that process_message_batch is called with the correct arguments
        process_message_batch_mock.assert_awaited_once_with(client_mock, eventhub_producer_mock, ANY, logger_mock)

        # for the async generator, we need to check that the actual generator passed to process_message_batch is the same as the one we expect
        actual_call_args = process_message_batch_mock.await_args
        actual_messages_arg = actual_call_args[0][2]  # This should be the async generator
        # Now check if actual_messages_arg is the async generator we expect
        assert actual_messages_arg is expected_messages_generator, "process_message_batch was not called with the expected async generator"

        # If you need to check log messages
        logger_mock.debug.assert_called() 

class TestProcessMessageBatch:
    PATCHED_MAX_EVENT_SIZE_BYTES = 1234
    @pytest.mark.asyncio
    @patch('mqtt_to_eventhub_module.MAX_EVENT_BATCH_SIZE_BYTES', PATCHED_MAX_EVENT_SIZE_BYTES)
    @patch('mqtt_to_eventhub_module.create_event_batch')
    @patch('mqtt_to_eventhub_module.on_message_async')
    @patch('mqtt_to_eventhub_module.logger')
    async def test_process_message_batch(self, logger_mock, on_message_async_mock, create_event_batch_mock):
        # Mock the EventHubProducerClient and its create_batch method
        eventhub_producer_mock = AsyncMock(spec=EventHubProducerClient)
        event_batch_mock = AsyncMock(spec=EventDataBatch)
        create_event_batch_mock.return_value = event_batch_mock

        # Make on_message_async return the same event batch mock
        on_message_async_mock.return_value = event_batch_mock

        # Mock the aiomqtt.Client
        client_mock = AsyncMock(spec=aiomqtt.Client)

        # Create a MagicMock to mock the async generator context manager
        message1 = aiomqtt.Message(topic="test/topic", payload=b"test1", qos=0, retain=False, mid=0, properties={})
        message2 = aiomqtt.Message(topic="test/topic", payload=b"test2", qos=0, retain=False, mid=0, properties={})
        messages = [message1, message2] 

        # Asynchronous generator function for messages
        async def messages_async_generator():
            for msg in messages:
                yield msg

        # Run the process_message_batch function with the mocked objects
        with patch('mqtt_to_eventhub_module.asyncio.ensure_future', new=lambda x: x):
            await mqtt_to_eventhub_module.process_message_batch(client_mock, eventhub_producer_mock, messages_async_generator(), logger_mock)

        # Assert create_event_batch was called
        create_event_batch_mock.assert_awaited_once_with(eventhub_producer_mock, self.PATCHED_MAX_EVENT_SIZE_BYTES, logger_mock)

        # Assert on_message_async was called for each message
        assert on_message_async_mock.await_count == len(messages)
        on_message_async_mock.assert_has_awaits([
            call(client_mock, event_batch_mock, message) for message in messages
        ])

class TestCreateEventBatch:
    @pytest.mark.asyncio
    async def test_create_event_batch(self):
        # Given
        max_size_in_bytes = 1234
        producer_mock = AsyncMock(spec=EventHubProducerClient)
        event_data_batch_mock = AsyncMock(spec=EventDataBatch)
        logger_mock = AsyncMock(spec=Logger)

        # Setting return_value to a coroutine that returns event_data_batch_mock
        async def create_batch_coro(*args, **kwargs):
            return event_data_batch_mock

        producer_mock.create_batch.side_effect = create_batch_coro

        # When
        result = await mqtt_to_eventhub_module.create_event_batch(producer_mock, max_size_in_bytes, logger_mock)

        # Then
        producer_mock.create_batch.assert_called_once_with(max_size_in_bytes=max_size_in_bytes)
        logger_mock.debug.assert_called_once_with("Creating new event batch")
        assert result == event_data_batch_mock


class TestReduceLogLevel:
    @patch('mqtt_to_eventhub_module.logging.getLogger')
    def test_reduce_log_level(self, mock_get_logger):
        # Arrange
        logger_mock_uamqp = MagicMock()
        logger_mock_azure = MagicMock()
        mock_get_logger.side_effect = [logger_mock_uamqp, logger_mock_azure]

        # Act
        mqtt_to_eventhub_module.reduce_log_level()

        # Assert
        mock_get_logger.assert_any_call("uamqp")
        mock_get_logger.assert_any_call("azure")
        logger_mock_uamqp.setLevel.assert_called_once_with(WARNING)
        logger_mock_azure.setLevel.assert_called_once_with(WARNING)

# class TestMainFunction:
#     @patch('mqtt_to_eventhub_module.asyncio.run')
#     @patch('mqtt_to_eventhub_module.log_error')
#     @patch('mqtt_to_eventhub_module.asyncLoop')
#     @patch('mqtt_to_eventhub_module.get_producer')
#     @patch('mqtt_to_eventhub_module.get_client')
#     @patch('mqtt_to_eventhub_module.reduce_log_level')
#     @patch('asyncio.new_event_loop')
#     @patch('asyncio.set_event_loop')
#     @pytest.mark.asyncio
#     async def test_main_normal_flow(
#         self,
#         mock_set_event_loop,
#         mock_new_event_loop,
#         mock_reduce_log_level,
#         mock_get_client,
#         mock_get_producer,
#         mock_async_loop,
#         mock_log_error,
#         mock_asyncio_run
#     ):
#         # Mocks
#         mock_event_loop = MagicMock()
#         mock_eventhub_producer_async = AsyncMock()
#         mock_eventhub_producer_async.close.return_value = asyncio.Future()

#         mock_client = MagicMock()

#         # Setting return values for mocks
#         mock_new_event_loop.return_value = mock_event_loop
#         mock_get_client.return_value = mock_client
#         mock_get_producer.return_value = mock_eventhub_producer_async

#         # Act
#         mqtt_to_eventhub_module.main()

#         # Assert
#         mock_reduce_log_level.assert_called_once()
#         mock_new_event_loop.assert_called_once()
#         mock_set_event_loop.assert_called_once()
#         mock_get_client.assert_called_once()
#         mock_get_producer.assert_called_once()
#         mock_event_loop.run_until_complete.assert_called_once()
#         mock_asyncio_run.assert_called_once_with(mock_eventhub_producer_async.close.return_value)



