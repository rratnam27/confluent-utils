from unittest.mock import patch, MagicMock
from src.confluent_utils.dlt_kafka_producer import DltKafkaProducer
import types


class DummyError(Exception):
    def __init__(self, code="ERR_X"):
        self.error_code = MagicMock(value=code)


@patch("src.confluent_utils.dlt_kafka_producer.BaseKafkaProducer")
def test_init_calls_base_kafka_producer(mock_base):
    settings = types.SimpleNamespace()
    DltKafkaProducer("dlt_producer_name", settings)
    mock_base.assert_called_once_with(
        producer_id="dlt_producer_name", settings=settings
    )


@patch("src.confluent_utils.dlt_kafka_producer.logger")
def test_produce_sends_string_message(mock_logger):
    cfg = MagicMock(
        dlt_topic="dlt_topic", dlt_value_serializer="str", avro_schema_path=None
    )
    with patch("src.confluent_utils.dlt_kafka_producer.BaseKafkaProducer"):
        settings = types.SimpleNamespace()
        producer = DltKafkaProducer(cfg, settings)
        producer.kafka_producer = MagicMock()
        error = DummyError("ERR_123")
        producer.produce("k", "msg", error)
        args, kwargs = producer.kafka_producer.send.call_args
        call_args = producer.kafka_producer.send.call_args
        assert call_args.kwargs["key"] == "k"
        assert call_args.kwargs["value"] == "msg"
        assert "error_code" in call_args.kwargs["headers"]
        assert call_args.kwargs["headers"]["error_code"] == "ERR_123"
        assert "stacktrace" in call_args.kwargs["headers"]


@patch("src.confluent_utils.dlt_kafka_producer.logger")
def test_produce_constructs_headers_and_sends(mock_logger):
    with patch("src.confluent_utils.dlt_kafka_producer.BaseKafkaProducer"):
        settings = types.SimpleNamespace()
        producer = DltKafkaProducer("dlt_prod", settings)
        producer.kafka_producer = MagicMock()
        producer.kafka_producer.send.return_value = {
            "status": "success",
            "topic": "dlt_topic",
            "partition": 0,
            "offset": 1,
        }
        error = DummyError("ERR_DLT")
        producer.produce("k2", {"bar": 1}, error)
        args, kwargs = producer.kafka_producer.send.call_args
        assert kwargs["headers"]["error_code"] == "ERR_DLT"
        assert "stacktrace" in kwargs["headers"]
        mock_logger.info.assert_called()


@patch("src.confluent_utils.dlt_kafka_producer.logger")
def test_produce_handles_send_failure(mock_logger):
    with patch("src.confluent_utils.dlt_kafka_producer.BaseKafkaProducer"):
        settings = types.SimpleNamespace()
        producer = DltKafkaProducer("dlt_prod", settings)
        producer.kafka_producer = MagicMock()
        producer.kafka_producer.send.side_effect = Exception("DLT send failed")
        error = DummyError("ERR_FAIL")
        try:
            producer.produce("k3", "fail msg", error)
        except AttributeError as e:
            assert "ERR_4008" in str(e)
        else:
            mock_logger.error.assert_called()


@patch("src.confluent_utils.dlt_kafka_producer.logger")
def test_produce_handles_missing_error_code(mock_logger):
    class NoCodeError(Exception):
        pass

    with patch("src.confluent_utils.dlt_kafka_producer.BaseKafkaProducer"):
        settings = types.SimpleNamespace()
        producer = DltKafkaProducer("dlt_prod", settings)
        producer.kafka_producer = MagicMock()
        producer.kafka_producer.send.return_value = {
            "status": "success",
            "topic": "dlt_topic",
            "partition": 0,
            "offset": 1,
        }
        error = NoCodeError("no code")
        producer.produce("k4", "msg", error)
        args, kwargs = producer.kafka_producer.send.call_args
        assert kwargs["headers"]["error_code"] == "UNKNOWN"
        mock_logger.info.assert_called()


@patch("src.confluent_utils.dlt_kafka_producer.logger")
def test_produce_str_message_triggers_str_branch(mock_logger):
    with patch("src.confluent_utils.dlt_kafka_producer.BaseKafkaProducer"):
        settings = types.SimpleNamespace()
        producer = DltKafkaProducer("dlt_prod", settings)
        producer.kafka_producer = MagicMock()
        producer.kafka_producer.send.return_value = {
            "status": "success",
            "topic": "dlt_topic",
            "partition": 0,
            "offset": 1,
        }
        error = DummyError("ERR_DLT")
        # Pass a non-bytes message to trigger message_value = str(message)
        producer.produce("k2", 12345, error)
        args, kwargs = producer.kafka_producer.send.call_args
        assert kwargs["value"] == "12345"
        mock_logger.info.assert_called()
