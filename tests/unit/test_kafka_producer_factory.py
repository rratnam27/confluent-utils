import pytest
from unittest.mock import patch, MagicMock
from src.confluent_utils.kafka_producer_factory import KafkaProducerFactory


@pytest.fixture
def mock_settings():
    return MagicMock()


@patch("src.confluent_utils.kafka_producer_factory.DltKafkaProducer")
@patch("src.confluent_utils.kafka_producer_factory.ErrorKafkaProducer")
@patch("src.confluent_utils.kafka_producer_factory.RetryKafkaProducer")
def test_get_producer_creates_and_caches(
    mock_retry, mock_error, mock_dlt, mock_settings
):
    factory = KafkaProducerFactory()
    # DLT
    dlt_instance = MagicMock()
    mock_dlt.return_value = dlt_instance
    result = factory.get_producer("dlt", "id1", mock_settings)
    assert result is dlt_instance
    mock_dlt.assert_called_once_with("id1", mock_settings)
    # Should return cached
    result2 = factory.get_producer("dlt", "id1", mock_settings)
    assert result2 is dlt_instance
    # Error
    error_instance = MagicMock()
    mock_error.return_value = error_instance
    result3 = factory.get_producer("error", "id2", mock_settings)
    assert result3 is error_instance
    mock_error.assert_called_once_with("id2", mock_settings)
    # Retry
    retry_instance = MagicMock()
    mock_retry.return_value = retry_instance
    result4 = factory.get_producer("retry", "id3", mock_settings)
    assert result4 is retry_instance
    mock_retry.assert_called_once_with("id3", mock_settings)
    # Unknown type
    result5 = factory.get_producer("unknown", "id4", mock_settings)
    assert result5 is None
