import logging
from typing import Any
import json
from x35_errors import AppException, ErrorCodes
from .base_kafka_producer import BaseKafkaProducer
from .enums import DeliveryResultEnum
from x35_settings import KafkaBaseSettings

logger = logging.getLogger(f"x35.{__name__}")


class RetryKafkaProducer:
    """
    A Kafka producer specialized for sending retry messages to a designated retry topic.

    Attributes:
        kafka_producer (BaseKafkaProducer): An instance of BaseKafkaProducer configured
                                            with retry topic settings.
    """

    def __init__(self, producer_id: str, settings: KafkaBaseSettings):
        """
        Initializes the RetryKafkaProducer with the given producer name.

        Args:
            producer_id (str): Name used to configure the underlying BaseKafkaProducer instance.
        """

        self.kafka_producer = BaseKafkaProducer(
            producer_id=producer_id, settings=settings
        )

    def produce(
        self, key: str, message: Any, error: AppException, headers: dict = None
    ):
        """
        Sends a retry message to the configured Kafka retry topic, including retry metadata and custom headers.

        Args:
            key (str): Key for the Kafka message.
            message (Any): Original message that caused the retry.
            error (AppException): The application exception to be logged and sent.
            headers (dict, optional): Custom headers to be sent with the Kafka message.

        Logs:
            - Success information if the message is delivered.
            - Error information if message delivery fails or sending to Kafka raises an exception.
        """

        try:
            # Check serializer type and handle message accordingly
            serializer_type = getattr(
                self.kafka_producer, "value_serializer_type", None
            )
            message_value = message
            if serializer_type in ("avro", "json"):
                if isinstance(message, bytes):
                    try:
                        message_value = json.loads(message.decode(errors="replace"))
                    except Exception:
                        message_value = message.decode(errors="replace")
                elif isinstance(message, str):
                    try:
                        message_value = json.loads(message)
                    except Exception:
                        message_value = message
                elif isinstance(message, dict):
                    message_value = message
                else:
                    message_value = message
            else:
                # For non-avro, non-json, keep current logic (string or bytes)
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
                ErrorCodes.ERR_4007.message,
                exc_info=e,
                extra={
                    "error_code": ErrorCodes.ERR_4007,
                    "error_message": str(e),
                },
            )
            raise
