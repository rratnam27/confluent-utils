from typing import Optional, Union, Any

import logging
import time
import json

from confluent_kafka import Producer
from concurrent.futures import Future
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from .enums import DeliveryResultEnum
from x35_settings import KafkaBaseSettings
from x35_errors import KafkaSerializationException, KafkaProducerException, ErrorCodes
from x35_observability import KafkaProducerMetrics, observability_settings
from opentelemetry import metrics

logger = logging.getLogger(f"x35.{__name__}")


class BaseKafkaProducer:
    """
    A base Kafka producer class supporting both string and Avro serialization.

    This class handles Kafka configuration, producer creation, serialization setup,
    and sending messages to Kafka topics. It supports optional message headers and
    integrates with Confluent's Schema Registry for Avro serialization.

    Attributes:
        config: Full configuration object loaded from a config loader.
        producer_config: Kafka producer-specific configuration section.
        kafka_cluster: Kafka cluster configuration.
        topic (str): The Kafka topic to which messages are published.
        producer (confluent_kafka.Producer): The Kafka producer instance.
        key_serializer: Serializer for message keys (always string).
        value_serializer: Serializer for message values (string or Avro).
    """

    def __init__(
        self,
        producer_id: str,
        settings: KafkaBaseSettings,
        custom_config: Optional[dict[str, Any]] = None,
    ):
        """
        Initializes the Kafka producer using the provided producer name.

        Args:
            producer_id (str): Identifier to load the corresponding producer config.

        Raises:
            ValueError: If Avro serialization is configured but schema path is not provided.
        """
        try:
            self.topic = getattr(settings, "topic")
            # Metrics setup based on observability settings
            if (
                observability_settings.otlp_metrics_exporter
                and observability_settings.otlp_traces_exporter
            ):
                meter = metrics.get_meter("app.kafka.producer")
                self._metrics = KafkaProducerMetrics(meter)
            else:
                self._metrics = None
            self.delivery_timeout_ms = getattr(settings, "delivery_timeout_ms")

            # Define security configuration in a separate dictionary
            security_config = {
                "security.protocol": getattr(settings, "security_protocol"),
                "sasl.mechanisms": getattr(settings, "sasl_mechanism"),
                "sasl.username": getattr(settings, "api_key"),
                "sasl.password": getattr(settings, "api_secret"),
                "client.dns.lookup": getattr(settings, "client_dns_lookup"),
            }
            # Define Kafka configuration and include security_config via unpacking
            kafka_config = {
                "bootstrap.servers": getattr(settings, "bootstrap_servers"),
                "acks": getattr(settings, "acks"),
                "enable.idempotence": getattr(settings, "enable_idempotence"),
                "delivery.timeout.ms": getattr(settings, "delivery_timeout_ms"),
                "retry.backoff.ms": getattr(settings, "retry_backoff_ms"),
                "max.in.flight.requests.per.connection": getattr(
                    settings, "max_in_flight_requests_per_connection"
                ),
                **security_config,  # Unpack the security config dictionary here
            }

            if custom_config is not None:
                kafka_config.update(custom_config)

            # Create the Kafka producer (no serializers here)
            self.producer = Producer(kafka_config)

            # Setup key and value serializers explicitly
            self.key_serializer = StringSerializer("utf_8")

            self.value_serializer_type = getattr(settings, "value_serializer_type")

            # Configure Avro serializer if needed
            if self.value_serializer_type == "avro":
                self.schema_id = getattr(settings, "schema_id")
                if not self.schema_id:
                    raise ValueError(
                        "Avro schema id must be provided for Avro serialization."
                    )

                # Schema Registry configuration
                self.schema_registry_username = getattr(
                    settings, "schema_registry_username"
                )
                self.schema_registry_password = getattr(
                    settings, "schema_registry_password"
                )
                schema_registry_config = {
                    "url": getattr(settings, "schema_registry_url"),
                    "basic.auth.user.info": f"{self.schema_registry_username}:{self.schema_registry_password}",
                }
                schema_registry_client = SchemaRegistryClient(schema_registry_config)

                schema_response = schema_registry_client.get_schema(self.schema_id)
                avro_schema_str = schema_response.schema_str

                serializer_config = {
                    "auto.register.schemas": getattr(settings, "auto_register_schemas"),
                    "use.latest.version": getattr(settings, "use_latest_version"),
                }

                # Set up Avro serializer
                self.value_serializer = AvroSerializer(
                    schema_registry_client=schema_registry_client,
                    schema_str=avro_schema_str,
                    to_dict=lambda obj, ctx: obj
                    if isinstance(obj, dict)
                    else obj.to_dict(),
                    conf=serializer_config,
                )
            elif self.value_serializer_type == "json":
                self.value_serializer = (
                    lambda obj, ctx=None: json.dumps(obj).encode("utf-8")
                    if obj is not None
                    else None
                )
            else:
                # Use string serializer for simple string messages
                self.value_serializer = StringSerializer("utf_8")

        except Exception as e:
            raise KafkaProducerException(error=ErrorCodes.ERR_4009, cause=e)

    def get_headers(self, headers: Optional[dict[str, str]] = None):
        """
        Constructs Kafka-compatible headers, including optional request ID.

        Args:
            headers (Optional[dict[str, str]]): Custom headers to include.

        Returns:
            List[Tuple[str, bytes]]: Kafka-compatible header list.
        """
        # Get request_id from contextvars
        # ctx = get_contextvars()
        # request_id = ctx.get("request_id")
        request_id = None
        # Prepare base headers dictionary with request_id if available
        base_headers = {}
        if request_id:
            base_headers["request-id"] = request_id
        # Merge additional headers passed by caller, if any (overwrites base headers if key clashes)
        if headers:
            base_headers.update(headers)
        # Convert headers dict to list of (str, bytes) tuples for Kafka
        kafka_headers = [
            (k, v.encode() if isinstance(v, str) else v)
            for k, v in base_headers.items()
        ]
        return kafka_headers

    def send_to_kafka(
        self,
        key: Optional[Union[str, bytes]] = None,
        value=None,
        headers: Optional[dict[str, str]] = None,
    ) -> dict:
        """
        Sends a message to the configured Kafka topic.

        Serializes the key and value according to the configured serializers,
        attaches headers, and sends the message with a delivery callback.

        Args:
            key (Optional[Union[str, bytes]]): The message key for partitioning.
            value: The message value (string or Avro-serializable object).
            headers (Optional[dict[str, str]]): Additional message headers.

        Returns:
            dict: Delivery result with topic, partition, and offset.

        Raises:
            KafkaSerializationException: If key or value serialization fails.
            KafkaProducerException: If message production or delivery fails.
        """
        delivery_result = Future()

        def delivery_report(err, msg):
            """
            Callback function to log the result of the message delivery.
            """
            if err is not None:
                raise KafkaProducerException(
                    error=ErrorCodes.ERR_4001, extra={"error_message": str(err)}
                )
            else:
                delivery_result.set_result(
                    {
                        DeliveryResultEnum.KEY_STATUS: DeliveryResultEnum.VALUE_SUCCESS,
                        DeliveryResultEnum.KEY_TOPIC: msg.topic(),
                        DeliveryResultEnum.KEY_PARTITION: msg.partition(),
                        DeliveryResultEnum.KEY_OFFSET: msg.offset(),
                    }
                )

        kafka_headers = self.get_headers(headers)

        # Handle None or empty key
        serialized_key = None
        if key:  # Only serialize if key is not None or empty string
            try:
                serialized_key = self.key_serializer(
                    key, SerializationContext(self.topic, MessageField.KEY)
                )
            except Exception as e:
                raise KafkaSerializationException(error=ErrorCodes.ERR_4002, cause=e)
        try:
            serialized_value = self.value_serializer(
                value, SerializationContext(self.topic, MessageField.VALUE)
            )
        except Exception as e:
            raise KafkaSerializationException(error=ErrorCodes.ERR_4003, cause=e)

        try:
            self.producer.produce(
                topic=self.topic,
                key=serialized_key,
                value=serialized_value,
                headers=kafka_headers,
                on_delivery=delivery_report,
            )
            self.producer.flush()
        except Exception as e:
            raise KafkaProducerException(error=ErrorCodes.ERR_4001, cause=e)

        return delivery_result.result(
            timeout=self.delivery_timeout_ms
        )  # Block until callback sets result

    def send(
        self,
        key: Optional[Union[str, bytes]] = None,
        value=None,
        headers: Optional[dict[str, str]] = None,
    ) -> dict:
        start = time.perf_counter()

        try:
            # with self._tracer.start_as_current_span("kafka.produce") as span:
            result = self.send_to_kafka(key=key, value=value, headers=headers)

            elapsed_ms = (time.perf_counter() - start) * 1000
            if self._metrics is not None:
                self._metrics.count.add(1, {"topic": self.topic})
                self._metrics.duration.record(
                    elapsed_ms, attributes={"topic": self.topic}
                )
                try:
                    serialized_value = self.value_serializer(
                        value, SerializationContext(self.topic, MessageField.VALUE)
                    )
                except Exception as e:
                    raise KafkaSerializationException(
                        error=ErrorCodes.ERR_4003, cause=e
                    )

                self._metrics.bytes.add(
                    len(serialized_value or b""), {"topic": self.topic}
                )
                self._metrics.update_last_ts()

            return result

        except (KafkaSerializationException, KafkaProducerException, Exception):
            if self._metrics is not None:
                self._metrics.errors.add(1, {"topic": self.topic})
            raise
