import traceback
import logging
import json
from typing import Any

from .base_kafka_producer import BaseKafkaProducer
from .enums import DeliveryResultEnum
from x35_settings import KafkaBaseSettings

from x35_errors import ErrorCodes

logger = logging.getLogger(f"x35.{__name__}")


class DltKafkaProducer:
    """
    Kafka producer for sending Dead Letter Topic (DLT) messages, typically used
    for capturing and analyzing failed message processing events.

    Attributes:
        kafka_producer (BaseKafkaProducer): The underlying Kafka producer configured for DLT.
    """

    def __init__(self, producer_id: str, settings: KafkaBaseSettings):
        """
        Initializes the DltKafkaProducer with the given producer name.

        Args:
            producer_id (str): Name used to configure the underlying BaseKafkaProducer instance.
        """

        self.kafka_producer = BaseKafkaProducer(
            producer_id=producer_id, settings=settings
        )

    def produce(self, key: str, message: Any, error: Exception):
        """
        Publishes a message to the DLT Kafka topic along with error metadata.

        Args:
            key (str): Key for partitioning the Kafka message.
            message (Any): The original message that failed processing.
            error (Exception): The exception that caused the failure.

        Logs:
            - Success log if the message is delivered successfully.
            - Error log if message delivery fails or an exception occurs during publishing.
        """

        error_code = "UNKNOWN"
        if hasattr(error, "error_code"):
            ec = getattr(error, "error_code")
            if hasattr(ec, "value"):
                error_code = ec.value
            else:
                error_code = ec

        headers = {
            "error_code": error_code,
            "stacktrace": "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            ),
        }

        try:
            # Check serializer type and handle message accordingly
            serializer_type = getattr(
                self.kafka_producer, "value_serializer_type", None
            )
            message_value = message
            if serializer_type == "avro":
                # If message is bytes, try to decode and parse as JSON
                if isinstance(message, bytes):
                    try:
                        message_value = json.loads(message.decode(errors="replace"))
                    except Exception:
                        message_value = message.decode(errors="replace")
                # If message is str, try to parse as JSON, else keep as str
                elif isinstance(message, str):
                    try:
                        message_value = json.loads(message)
                    except Exception:
                        pass  # keep as str if not JSON
                # If message is dict, use as-is
                elif isinstance(message, dict):
                    message_value = message
                # If message is a model (dataclass, etc.), use as-is (serializer will call .to_dict())
                else:
                    message_value = message
            else:
                # For non-avro, keep current logic (string or bytes)
                if isinstance(message, bytes):
                    message_value = message.decode(errors="replace")
                else:
                    message_value = str(message)

            result = self.kafka_producer.send(
                key=key, value=message_value, headers=headers
            )
            if (
                result.get(DeliveryResultEnum.KEY_STATUS)
                == DeliveryResultEnum.VALUE_SUCCESS
            ):
                logger.info(
                    f"Message delivered to {result.get(DeliveryResultEnum.KEY_TOPIC)} partition:[{result.get(DeliveryResultEnum.KEY_PARTITION)}] offset:[{result.get(DeliveryResultEnum.KEY_OFFSET)}]"
                )
        except Exception as e:
            logger.error(
                ErrorCodes.ERR_4008.message,
                exc_info=e,
                extra={
                    "error_code": ErrorCodes.ERR_4008,
                    "error_message": str(e),
                },
            )
