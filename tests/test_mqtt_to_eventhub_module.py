import asyncio
import json
import time
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock, patch

import paho.mqtt.client as mqtt_client
from azure.eventhub import EventData
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


class TestAsyncLoop:
    @pytest.mark.asyncio
    @patch('mqtt_to_eventhub_module.message_loop', new_callable=AsyncMock)
    @patch('mqtt_to_eventhub_module.poll_healthceck_if_needed', new_callable=AsyncMock)
    @patch('mqtt_to_eventhub_module.check_mqtt_timeout', new_callable=AsyncMock)
    @patch('asyncio.gather', new_callable=AsyncMock)
    async def test_asyncLoop_calls_gather_with_functions(
            self, mock_gather, mock_check_mqtt_timeout,
            mock_poll_healthcheck_if_needed, mock_message_loop):

        # Mocked eventhub_producer and client
        mock_eventhub_producer = 'mock_eventhub_producer'
        mock_client = 'mock_client'

        # Call the function
        await mqtt_to_eventhub_module.asyncLoop(mock_eventhub_producer, mock_client)

        # Verify that asyncio.gather is called
        assert mock_gather.call_count == 1

        # Verify it is called with the correct number of coroutines
        # Have to do this because the mock coroutine objects are not 
        # identical to the ones that asyncio.gather is called with. This 
        # is expected because every time you 'await' a coroutine, 
        # a new coroutine object is created.
        actual_coroutines = mock_gather.call_args[0]
        assert len(actual_coroutines) == 3


class TestProcessMessage:
    @pytest.mark.asyncio
    @freeze_time("2023-09-16 15:56:00")
    @patch("mqtt_to_eventhub_module.eventhub_producer_async", new_callable=AsyncMock)    
    @patch("mqtt_to_eventhub_module.send_message_to_eventhub_async")
    @patch("mqtt_to_eventhub_module.aiomqtt.Client")
    @patch("mqtt_to_eventhub_module.aiomqtt.Message")
    async def test_on_message_async_with_empty_batch(self, mock_message, mock_client, mock_send_message, mock_producer):
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
            "timestamp": 1694879760.0
        }

        expected_event_data = EventData(json.dumps(expected_event_body))
        expected_event_data_json = expected_event_data.body_as_json("utf-8")

        await mqtt_to_eventhub_module.on_message_async(
                mock_client, mock_event_data_batch, mock_message
        )

        assert mock_send_message.call_count == 0
        actual_call = mock_event_data_batch.add.call_args
        actual_event_data = actual_call[0][0]  # Assuming add is called with one positional argument
        actual_event_data_json = actual_event_data.body_as_json("utf-8")
        assert expected_event_data_json == actual_event_data_json

    @pytest.mark.asyncio
    @freeze_time("2023-09-16 15:56:00")
    @patch("mqtt_to_eventhub_module.eventhub_producer_async", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.send_message_to_eventhub_async", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.aiomqtt.Client")
    @patch("mqtt_to_eventhub_module.aiomqtt.Message")
    async def test_on_message_async_with_full_batch(self, mock_message, mock_client, mock_send_message, mock_producer):
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
            "timestamp": 1694879760.0
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
        actual_event_data = actual_call[0][0]  # Assuming add is called with one positional argument
        actual_event_data_json = actual_event_data.body_as_json("utf-8")
        assert expected_event_data_json == actual_event_data_json


class TestSendToEventHub:
    @pytest.mark.asyncio
    @patch("mqtt_to_eventhub_module.EventHubProducerClient", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.EventDataBatch")
    async def test_send_message_to_eventhub_async_succeeds(self, mock_event_data_batch, mock_producer):

        await mqtt_to_eventhub_module.send_message_to_eventhub_async(
            mock_producer, mock_event_data_batch
        )

        mock_producer.send_batch.assert_called_with(mock_event_data_batch)

    @pytest.mark.asyncio
    @patch("mqtt_to_eventhub_module.log_error")
    @patch("mqtt_to_eventhub_module.EventHubProducerClient", new_callable=AsyncMock)
    @patch("mqtt_to_eventhub_module.EventDataBatch")
    async def test_send_message_to_eventhub_async_fails(self, mock_event_data_batch, mock_producer, mock_log_error):
        mock_producer.send_batch.side_effect = EventHubError("Test EventHubError")
        
        # Check if the exception is caught and log_error is called
        await mqtt_to_eventhub_module.send_message_to_eventhub_async(
            mock_producer, mock_event_data_batch
        )

        mock_producer.send_batch.assert_called_with(mock_event_data_batch)
        mock_log_error.assert_called_with("Error sending message to event hub", mock_producer.send_batch.side_effect)


class TestHealthCheck:
    @patch("mqtt_to_eventhub_module.requests.post")
    def test_log_error(self, mock_post):
        mqtt_to_eventhub_module.HEALTCHECK_FAILURE_URL = "http://healthcheck"
        mqtt_to_eventhub_module.HEALTHCHECK_REPORT_ERRORS = True
        mqtt_to_eventhub_module.log_error("Test Error")
        mock_post.assert_called_with(
            "http://healthcheck", data={"error": "Test Error ()"}
        )

    @patch("mqtt_to_eventhub_module.requests.get")
    def test_poll_healthcheck(self, mock_get):
        mqtt_to_eventhub_module.HEALTHCHECK_URL = "http://healthcheck"
        mqtt_to_eventhub_module.HEALTHCHECK_METHOD = "GET"
        mqtt_to_eventhub_module.poll_healthcheck()
        mock_get.assert_called_with("http://healthcheck")


class TestCheckMqttTimeout:
    @pytest.mark.asyncio
    @patch("mqtt_to_eventhub_module.log_error")
    @patch("mqtt_to_eventhub_module.MQTT_TIMEOUT", new=1)  # Override the MQTT_TIMEOUT to 1 second for testing
    async def test_check_mqtt_timeout(self, mock_log_error):
        # Given that the last message time is far in the past
        mqtt_to_eventhub_module.last_mqtt_message_time = time.time() - 1000  # 1000 seconds ago

        # Run the function for a short amount of time (e.g., 1.1 seconds)
        task = asyncio.create_task(mqtt_to_eventhub_module.check_mqtt_timeout())
        await asyncio.sleep(1.1)
        task.cancel()

        # Verify that log_error was called with the appropriate message
        mock_log_error.assert_called_with(
            "No message received via MQTT for more than %s seconds - last message received at %s",
            1,  # MQTT_TIMEOUT
            mqtt_to_eventhub_module.last_mqtt_message_time
        )

