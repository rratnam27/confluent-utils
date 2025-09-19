import traceback
import logging
from typing import Any

from x35_errors import AppException, ErrorCodes
from .base_kafka_producer import BaseKafkaProducer
from .enums import DeliveryResultEnum
from x35_settings import KafkaBaseSettings

logger = logging.getLogger(f"x35.{__name__}")


class ErrorKafkaProducer:
    """
    A Kafka producer specialized for sending error messages to a designated error topic.

    Attributes:
        kafka_producer (BaseKafkaProducer): An instance of BaseKafkaProducer configured
                                            with error topic settings.
    """

    def __init__(self, producer_id: str, settings: KafkaBaseSettings):
        """
        Initializes the ErrorKafkaProducer with the given producer name.

        Args:
            producer_id (str): Name used to configure the underlying BaseKafkaProducer instance.
        """

        self.kafka_producer = BaseKafkaProducer(
            producer_id=producer_id, settings=settings
        )

    def produce(self, key: str, message: Any, error: AppException):
        """
        Sends an error message to the configured Kafka error topic, including error metadata.

        Args:
            key (str): Key for the Kafka message.
            message (Any): Original message that caused the error.
            error (AppException): The application exception to be logged and sent.

        Logs:
            - Success information if the message is delivered.
            - Error information if message delivery fails or sending to Kafka raises an exception.
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
            # Convert original_message to string for kafka value
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
                ErrorCodes.ERR_4006.message,
                exc_info=e,
                extra={
                    "error_code": ErrorCodes.ERR_4006,
                    "error_message": str(e),
                },
            )
