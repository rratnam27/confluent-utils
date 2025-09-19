import logging
from .error_kafka_producer import ErrorKafkaProducer
from .dlt_kafka_producer import DltKafkaProducer
from .retry_kafka_producer import RetryKafkaProducer
from x35_settings import KafkaBaseSettings

logger = logging.getLogger(f"x35.{__name__}")


class KafkaProducerFactory:
    """
    Factory class for creating and caching Kafka producer instances (DLT, Error, Retry).
    """

    def __init__(self):
        self._cache = {}

    def get_producer(
        self, producer_type: str, producer_id: str, settings: KafkaBaseSettings
    ) -> DltKafkaProducer | ErrorKafkaProducer | RetryKafkaProducer:
        """
        Returns a cached or newly created Kafka producer instance based on type and id.

        Args:
            producer_type (str): The type of producer ("dlt", "error", "retry").
            producer_id (str): Unique identifier for the producer.
            settings (KafkaBaseSettings): Producer-specific settings.

        Returns:
            Union[DltKafkaProducer, ErrorKafkaProducer, RetryKafkaProducer]: The producer instance.
        """
        key = producer_id
        if key in self._cache:
            return self._cache[key]
        if producer_type == "dlt":
            producer = DltKafkaProducer(producer_id, settings)
        elif producer_type == "error":
            producer = ErrorKafkaProducer(producer_id, settings)
        elif producer_type == "retry":
            producer = RetryKafkaProducer(producer_id, settings)
        else:
            producer = None
        self._cache[key] = producer
        return producer


def _get_producer_settings(
    producer_id: str, other_settings_list: list
) -> KafkaBaseSettings | None:
    """
    Retrieves the producer settings object from a list by matching the producer_id.

    Args:
        producer_id (str): The producer's unique identifier.
        other_settings_list (list): List of settings objects.

    Returns:
        Optional[KafkaBaseSettings]: The matching settings object, or None if not found.
    """
    for producer_settings in other_settings_list:
        settings_producer_id = getattr(producer_settings, "producer_id")
        if producer_id == settings_producer_id:
            return producer_settings
    return None


def produce_to_error_topic(
    message,
    consumer,
    e,
    settings,
    other_settings_list=None,
):
    """
    Produces a message to the error topic using the error producer.

    Args:
        message: The Kafka message to be sent to the error topic.
        consumer: The Kafka consumer instance.
        e: The exception or error to attach to the message.
        settings: Kafka settings containing the error producer id.
        other_settings_list: List of other producer settings.
    """
    error_producer_id = getattr(settings, "error_producer", None)
    if error_producer_id is not None and error_producer_id.strip() != "":
        error_settings = _get_producer_settings(error_producer_id, other_settings_list)
        error_producer = producer_factory.get_producer(
            "error", error_producer_id, error_settings
        )
        error_producer.produce(
            key=str(message.key()) if message.key() is not None else "",
            message=message.value() if message else b"",
            error=e,
        )
    consumer.commit(message)


def produce_to_dlt_topic(
    message,
    consumer,
    e,
    settings,
    other_settings_list=None,
):
    """
    Produces a message to the Dead Letter Topic (DLT) using the DLT producer.

    Args:
        message: The Kafka message to be sent to the DLT.
        consumer: The Kafka consumer instance.
        e: The exception or error to attach to the message.
        settings: Kafka settings containing the DLT producer id.
        other_settings_list: List of other producer settings.
    """
    dlt_producer_id = getattr(settings, "dlt_producer", None)
    if dlt_producer_id is not None and dlt_producer_id.strip() != "":
        dlt_settings = _get_producer_settings(dlt_producer_id, other_settings_list)
        dlt_producer = producer_factory.get_producer(
            "dlt", dlt_producer_id, dlt_settings
        )
        dlt_producer.produce(
            key=str(message.key()) if message.key() is not None else "",
            message=message.value(),
            error=e,
        )
    consumer.commit(message)


def produce_to_retry_topic(
    message,
    consumer,
    e,
    settings,
    other_settings_list=None,
    headers=None,
):
    """
    Produces a message to the retry topic using the retry producer.

    Args:
        message: The Kafka message to be sent to the retry topic.
        consumer: The Kafka consumer instance.
        e: The exception or error to attach to the message.
        settings: Kafka settings containing the retry producer id.
        other_settings_list: List of other producer settings.
        headers: Optional headers to include with the message.
    """
    retry_producer_id = getattr(settings, "retry_producer", None)
    if retry_producer_id is not None and retry_producer_id.strip() != "":
        retry_settings = _get_producer_settings(retry_producer_id, other_settings_list)
        retry_producer = producer_factory.get_producer(
            "retry", retry_producer_id, retry_settings
        )
        retry_producer.produce(
            key=str(message.key()) if message.key() is not None else "",
            message=message.value(),
            error=e,
            headers=headers,
        )
    consumer.commit(message)


producer_factory = KafkaProducerFactory()
