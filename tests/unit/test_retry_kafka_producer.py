from unittest.mock import MagicMock, patch
from src.confluent_utils.retry_kafka_producer import RetryKafkaProducer


class DummySettings:
    pass


class DummyException(Exception):
    pass


def test_produce_success(monkeypatch):
    mock_send = MagicMock(
        return_value={
            "status": "success",
            "topic": "retry-topic",
            "partition": 0,
            "offset": 123,
        }
    )
    with patch(
        "src.confluent_utils.retry_kafka_producer.BaseKafkaProducer"
    ) as MockProducer:
        MockProducer.return_value.send = mock_send
        producer = RetryKafkaProducer("test-producer", DummySettings())
        producer.produce("key1", "msg", DummyException("err"))
        mock_send.assert_called_once()


def test_produce_failure(monkeypatch):
    mock_send = MagicMock(return_value={"status": "error", "error": "send failed"})
    with patch(
        "src.confluent_utils.retry_kafka_producer.BaseKafkaProducer"
    ) as MockProducer:
        MockProducer.return_value.send = mock_send
        producer = RetryKafkaProducer("test-producer", DummySettings())
        producer.produce("key2", "msg", DummyException("err"))
        mock_send.assert_called_once()


def test_produce_exception(monkeypatch):
    mock_send = MagicMock(side_effect=Exception("kafka down"))
    with patch(
        "src.confluent_utils.retry_kafka_producer.BaseKafkaProducer"
    ) as MockProducer:
        MockProducer.return_value.send = mock_send
        producer = RetryKafkaProducer("test-producer", DummySettings())
        try:
            producer.produce("key3", "msg", DummyException("err"))
        except Exception as e:
            assert str(e) == "kafka down"
        mock_send.assert_called_once()
