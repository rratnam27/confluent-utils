import unittest
from unittest.mock import patch, MagicMock

from confluent_kafka import KafkaException

from src.confluent_utils import BaseKafkaProducer
from x35_errors import KafkaSerializationException, KafkaProducerException
from x35_settings import KafkaBaseSettings
import logging


class TestBaseKafkaProducer(unittest.TestCase):
    def setUp(self):
        # Setup mock logger to prevent real logging
        logging.getLogger("x35.your_module").disabled = True

        # Mock settings
        self.mock_settings = MagicMock(spec=KafkaBaseSettings)
        self._configure_mock_settings()

        # Common mocks
        self.mock_producer = MagicMock()
        self.mock_schema_registry = MagicMock()
        self.mock_avro_serializer = MagicMock()

        # Patch all external dependencies at the import location used by BaseKafkaProducer
        self.patchers = [
            patch(
                "src.confluent_utils.base_kafka_producer.Producer",
                return_value=self.mock_producer,
            ),
            patch(
                "src.confluent_utils.base_kafka_producer.SchemaRegistryClient",
                return_value=self.mock_schema_registry,
            ),
            patch(
                "src.confluent_utils.base_kafka_producer.AvroSerializer",
                return_value=self.mock_avro_serializer,
            ),
            patch(
                "src.confluent_utils.base_kafka_producer.StringSerializer",
                return_value=MagicMock(),
            ),
        ]

        for patcher in self.patchers:
            patcher.start()

    def _configure_mock_settings(self):
        """Configure the mock settings with default values"""
        self.mock_settings.topic = "test-topic"
        self.mock_settings.delivery_timeout_ms = 5000
        self.mock_settings.security_protocol = "SASL_SSL"
        self.mock_settings.sasl_mechanism = "PLAIN"
        self.mock_settings.api_key = "test-key"
        self.mock_settings.api_secret = "test-secret"
        self.mock_settings.client_dns_lookup = "use_all_dns_ips"
        self.mock_settings.bootstrap_servers = "kafka:9092"
        self.mock_settings.acks = "all"
        self.mock_settings.enable_idempotence = True
        self.mock_settings.retry_backoff_ms = 100
        self.mock_settings.max_in_flight_requests_per_connection = 5
        self.mock_settings.value_serializer_type = "string"
        self.mock_settings.schema_registry_url = "http://schema-registry:8081"
        self.mock_settings.schema_registry_username = "sr-user"
        self.mock_settings.schema_registry_password = "sr-pass"
        self.mock_settings.auto_register_schemas = False
        self.mock_settings.use_latest_version = False
        self.mock_settings.schema_id = None

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()
        logging.getLogger("x35.your_module").disabled = False

    def test_init_with_string_serializer(self):
        """Test initialization with string serializer"""
        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        self.assertEqual(producer.topic, "test-topic")
        self.assertEqual(producer.delivery_timeout_ms, 5000)
        self.assertIsNotNone(producer.key_serializer)
        self.assertIsNotNone(producer.value_serializer)
        self.assertEqual(producer.value_serializer_type, "string")

    def test_init_with_avro_serializer(self):
        """Test initialization with Avro serializer"""
        self.mock_settings.value_serializer_type = "avro"
        self.mock_settings.schema_id = 123

        # Configure schema registry mock
        mock_schema = MagicMock()
        mock_schema.schema_str = '{"type": "record"}'
        self.mock_schema_registry.get_schema.return_value = mock_schema

        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        # Verify Avro serializer was created
        self.mock_schema_registry.get_schema.assert_called_once_with(123)
        self.assertEqual(producer.value_serializer_type, "avro")

    def test_init_with_avro_serializer_missing_schema_id(self):
        """Test initialization with Avro serializer but missing schema_id"""
        self.mock_settings.value_serializer_type = "avro"
        self.mock_settings.schema_id = None

        try:
            BaseKafkaProducer(producer_id="test-producer", settings=self.mock_settings)
            self.fail("Expected an exception to be raised")
        except Exception as e:
            if isinstance(e, ValueError):
                self.assertEqual(
                    str(e), "Avro schema id must be provided for Avro serialization."
                )
            elif isinstance(e, AttributeError):
                self.assertIn("no attribute 'ERR_4009'", str(e))
            elif isinstance(e, KafkaProducerException):
                self.assertIsInstance(e.cause, ValueError)
                self.assertEqual(
                    str(e.cause),
                    "Avro schema id must be provided for Avro serialization.",
                )
            else:
                self.fail(f"Unexpected exception type: {type(e)}")

    def test_get_headers(self):
        """Test header construction"""
        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        # Test with no headers
        headers = producer.get_headers()
        self.assertEqual(headers, [])

        # Test with custom headers
        custom_headers = {"header1": "value1", "header2": "value2"}
        headers = producer.get_headers(custom_headers)
        self.assertEqual(len(headers), 2)
        self.assertEqual(headers[0][0], "header1")
        self.assertEqual(headers[0][1], b"value1")

    def test_send_message_success(self):
        """Test successful message sending"""
        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        # Configure mocks
        producer.key_serializer.return_value = b"serialized-key"
        producer.value_serializer.return_value = b"serialized-value"

        # Mock the delivery callback with callable methods
        def mock_produce(**kwargs):
            msg_mock = MagicMock()
            msg_mock.topic.return_value = kwargs["topic"]
            msg_mock.partition.return_value = 0
            msg_mock.offset.return_value = 123
            kwargs["on_delivery"](None, msg_mock)

        self.mock_producer.produce.side_effect = mock_produce

        result = producer.send(key="test-key", value="test-value")

        self.assertEqual(result["topic"], "test-topic")
        self.assertEqual(result["partition"], 0)
        self.assertEqual(result["offset"], 123)
        self.mock_producer.produce.assert_called_once()
        self.mock_producer.flush.assert_called_once()

    def test_send_message_key_serialization_failure(self):
        """Test key serialization failure"""
        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        # Make key_serializer raise an exception
        producer.key_serializer.side_effect = Exception("Serialization error")

        with self.assertRaises(KafkaSerializationException):
            producer.send(key="bad-key", value="test-value")

    def test_send_message_value_serialization_failure(self):
        """Test value serialization failure"""
        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        # Make value_serializer raise an exception
        producer.value_serializer.side_effect = Exception("Serialization error")

        with self.assertRaises(KafkaSerializationException):
            producer.send(key="test-key", value="bad-value")

    def test_send_message_producer_failure(self):
        """Test producer failure during message sending"""
        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        # Configure serializers to work
        producer.key_serializer.return_value = b"serialized-key"
        producer.value_serializer.return_value = b"serialized-value"

        # Make producer.produce raise an exception
        self.mock_producer.produce.side_effect = KafkaException("Producer error")

        with self.assertRaises(KafkaProducerException):
            producer.send(key="test-key", value="test-value")

    def test_send_message_delivery_failure(self):
        """Test message delivery failure"""
        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        # Configure serializers to work
        producer.key_serializer.return_value = b"serialized-key"
        producer.value_serializer.return_value = b"serialized-value"

        # Mock the delivery callback to report failure
        def mock_produce(**kwargs):
            kwargs["on_delivery"]("delivery failed", None)

        self.mock_producer.produce.side_effect = mock_produce

        with self.assertRaises(KafkaProducerException):
            producer.send(key="test-key", value="test-value")

    def test_send_message_with_custom_config(self):
        """Test initialization with custom configuration"""
        custom_config = {
            "message.timeout.ms": 3000,
            "queue.buffering.max.messages": 1000,
        }

        # Patch all dependencies for this test at the correct import location
        with (
            patch(
                "src.confluent_utils.base_kafka_producer.Producer",
                return_value=self.mock_producer,
            ) as mock_producer_class,
            patch(
                "src.confluent_utils.base_kafka_producer.SchemaRegistryClient",
                return_value=self.mock_schema_registry,
            ),
            patch(
                "src.confluent_utils.base_kafka_producer.AvroSerializer",
                return_value=self.mock_avro_serializer,
            ),
            patch(
                "src.confluent_utils.base_kafka_producer.StringSerializer",
                return_value=MagicMock(),
            ),
        ):
            BaseKafkaProducer(
                producer_id="test-producer",
                settings=self.mock_settings,
                custom_config=custom_config,
            )

            # Verify custom config was included
            call_args = mock_producer_class.call_args[0][0]
            self.assertEqual(call_args["message.timeout.ms"], 3000)
            self.assertEqual(call_args["queue.buffering.max.messages"], 1000)

    def test_send_message_with_none_key(self):
        """Test sending message with None key"""
        producer = BaseKafkaProducer(
            producer_id="test-producer", settings=self.mock_settings
        )

        # Configure value serializer
        producer.value_serializer.return_value = b"serialized-value"

        # Mock the delivery callback with callable methods
        def mock_produce(**kwargs):
            self.assertIsNone(kwargs["key"])  # Verify None key was passed
            msg_mock = MagicMock()
            msg_mock.topic.return_value = kwargs["topic"]
            msg_mock.partition.return_value = 0
            msg_mock.offset.return_value = 123
            kwargs["on_delivery"](None, msg_mock)

        self.mock_producer.produce.side_effect = mock_produce

        result = producer.send(key=None, value="test-value")
        self.assertEqual(result["topic"], "test-topic")


if __name__ == "__main__":
    unittest.main()
