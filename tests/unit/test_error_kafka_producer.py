from unittest.mock import patch, MagicMock
from src.confluent_utils.error_kafka_producer import ErrorKafkaProducer
import types


class DummyError(Exception):
    def __init__(self, code="ERR_X"):
        self.error_code = MagicMock(value=code)


@patch("src.confluent_utils.error_kafka_producer.BaseKafkaProducer")
def test_init_calls_base_kafka_producer(mock_base):
    settings = types.SimpleNamespace()
    ErrorKafkaProducer("error_producer_name", settings)
    mock_base.assert_called_once_with(
        producer_id="error_producer_name", settings=settings
    )


@patch("src.confluent_utils.error_kafka_producer.logger")
def test_produce_sends_string_message(mock_logger):
    mock_producer = MagicMock()
    mock_producer.send.return_value = {
        "status": "success",
        "topic": "err_topic",
        "partition": 0,
        "offset": 123,
    }
    with patch(
        "src.confluent_utils.error_kafka_producer.BaseKafkaProducer",
        return_value=mock_producer,
    ):
        cfg = MagicMock(error_topic="err_topic", error_value_serializer="str")
        settings = types.SimpleNamespace()
        producer = ErrorKafkaProducer(cfg, settings)
        dummy_error = DummyError(code="ERR_X")
        # Act
        producer.produce("key1", "test message", dummy_error)
        # Assert
        mock_producer.send.assert_called_once()
        args, kwargs = mock_producer.send.call_args
        assert kwargs["key"] == "key1"
        assert kwargs["value"] == "test message"
        assert "error_code" in kwargs["headers"]
        mock_logger.info.assert_called()


@patch("src.confluent_utils.error_kafka_producer.logger")
def test_produce_constructs_headers_and_sends(mock_logger):
    mock_producer = MagicMock()
    mock_producer.send.return_value = {"status": "success"}
    with patch(
        "src.confluent_utils.error_kafka_producer.BaseKafkaProducer",
        return_value=mock_producer,
    ):
        settings = types.SimpleNamespace()
        producer = ErrorKafkaProducer("err_prod", settings)
        dummy_error = DummyError(code="ERR_TEST")
        producer.produce("k1", {"foo": "bar"}, dummy_error)
        args, kwargs = mock_producer.send.call_args
        assert kwargs["headers"]["error_code"] == "ERR_TEST"
        assert "stacktrace" in kwargs["headers"]
        mock_logger.info.assert_called()


@patch("src.confluent_utils.error_kafka_producer.logger")
def test_produce_handles_send_failure(mock_logger):
    mock_producer = MagicMock()
    mock_producer.send.side_effect = Exception("Kafka send failed")
    with patch(
        "src.confluent_utils.error_kafka_producer.BaseKafkaProducer",
        return_value=mock_producer,
    ):
        settings = types.SimpleNamespace()
        producer = ErrorKafkaProducer("err_prod", settings)
        dummy_error = DummyError(code="ERR_FAIL")
        producer.produce("k2", "fail msg", dummy_error)
        mock_logger.error.assert_called()


@patch("src.confluent_utils.error_kafka_producer.logger")
def test_produce_handles_missing_error_code(mock_logger):
    class NoCodeError(Exception):
        pass

    mock_producer = MagicMock()
    mock_producer.send.return_value = {"status": "success"}
    with patch(
        "src.confluent_utils.error_kafka_producer.BaseKafkaProducer",
        return_value=mock_producer,
    ):
        settings = types.SimpleNamespace()
        producer = ErrorKafkaProducer("err_prod", settings)
        error = NoCodeError("no code")
        producer.produce("k3", "msg", error)
        args, kwargs = mock_producer.send.call_args
        assert kwargs["headers"]["error_code"] == "UNKNOWN"
        mock_logger.info.assert_called()


@patch("src.confluent_utils.error_kafka_producer.logger")
def test_produce_str_message_triggers_str_branch(mock_logger):
    mock_producer = MagicMock()
    mock_producer.send.return_value = {
        "status": "success",
        "topic": "err_topic",
        "partition": 0,
        "offset": 1,
    }
    with patch(
        "src.confluent_utils.error_kafka_producer.BaseKafkaProducer",
        return_value=mock_producer,
    ):
        settings = types.SimpleNamespace()
        producer = ErrorKafkaProducer("err_prod", settings)
        dummy_error = DummyError(code="ERR_TEST")
        # Pass a non-bytes message to trigger message_value = str(message)
        producer.produce("k1", 12345, dummy_error)
        args, kwargs = mock_producer.send.call_args
        assert kwargs["value"] == "12345"
        mock_logger.info.assert_called()
