from unittest.mock import MagicMock, patch
from src.confluent_utils.kafka_batch_consumer import KafkaBatchConsumer


class DummySettings:
    # Add attributes matching KafkaSettingsEnum values (likely lowercase)
    bootstrap_servers = "localhost:9092"
    group_id = "test-group"
    auto_offset_reset = "earliest"
    security_protocol = "SASL_SSL"
    sasl_mechanism = "PLAIN"
    api_key = "dummy-key"
    api_secret = "dummy-secret"
    client_dns_lookup = "use_all"
    topic = "test-topic"
    partition = 0
    batch_timeout = 1
    enable_auto_commit = True


@patch("src.confluent_utils.kafka_batch_consumer.Consumer")
def test_consume_batch(mock_consumer):
    mock_instance = mock_consumer.return_value
    mock_instance.consume.return_value = ["msg1", "msg2"]
    consumer = KafkaBatchConsumer(DummySettings())
    result = consumer.consume_batch(2)
    assert result == ["msg1", "msg2"]
    mock_instance.consume.assert_called_with(num_messages=2, timeout=1)


@patch("src.confluent_utils.kafka_batch_consumer.Consumer")
def test_commit_and_close(mock_consumer):
    mock_instance = mock_consumer.return_value
    consumer = KafkaBatchConsumer(DummySettings())
    consumer.commit()
    mock_instance.commit.assert_called_once()
    consumer.close()
    mock_instance.close.assert_called_once()


@patch("src.confluent_utils.kafka_batch_consumer.TopicPartition")
@patch("src.confluent_utils.kafka_batch_consumer.Consumer")
def test_get_committed_offset(mock_consumer, mock_tp):
    mock_instance = mock_consumer.return_value
    mock_offset = MagicMock()
    mock_offset.offset = 5
    mock_instance.committed.return_value = [mock_offset]
    consumer = KafkaBatchConsumer(DummySettings())
    offset = consumer.get_committed_offset()
    mock_tp.assert_called_with("test-topic", 0)
    assert offset == 5
