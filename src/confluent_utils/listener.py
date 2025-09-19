import threading
import time
import logging
import uuid
import json

from confluent_kafka import DeserializingConsumer
from confluent_kafka import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from typing import Callable, Optional, Any

from x35_errors import (
    KafkaDeserializationException,
    NonBlockingRetryException,
    NonRetryableException,
    BlockingRetryException,
)
from x35_errors import ErrorCodes

from x35_settings import KafkaBaseSettings
from x35_json_logging import dynamic_context
from x35_observability import KafkaConsumerMetrics

from .kafka_producer_factory import (
    produce_to_error_topic,
    produce_to_dlt_topic,
    produce_to_retry_topic,
)
from .enums import KafkaSettingsEnum

logger = logging.getLogger(f"x35.{__name__}")

_listeners: dict[str, threading.Thread] = {}
_stop_events: dict[str, threading.Event] = {}

RETRY_TOPIC_SUFFIX = "retry"


def create_consumer(
    settings: KafkaBaseSettings, custom_config: dict[str, Any]
) -> DeserializingConsumer:
    """
    Creates and configures a Kafka DeserializingConsumer based on the listener configuration.

    Args:
        settings (KafkaBaseSettings): Listener-specific Kafka configuration.

    Returns:
        DeserializingConsumer: A configured Kafka consumer subscribed to the specified topic.
    """

    key_deserializer = StringDeserializer("utf_8")
    value_deserializer = StringDeserializer("utf_8")
    value_serializer_type = getattr(settings, "value_serializer_type")
    if value_serializer_type == "avro":
        schema_registry_username = getattr(settings, "schema_registry_username")
        schema_registry_password = getattr(settings, "schema_registry_password")
        schema_registry_client = SchemaRegistryClient(
            {
                "url": getattr(settings, "schema_registry_url"),
                "basic.auth.user.info": f"{schema_registry_username}:{schema_registry_password}",
            }
        )
        value_deserializer = AvroDeserializer(schema_registry_client, None)
    elif value_serializer_type == "json":

        def json_value_deserializer(obj, ctx=None):
            return json.loads(obj.decode("utf-8")) if obj is not None else None

        value_deserializer = json_value_deserializer

    # Define security configuration in a separate dictionary
    security_config = {
        "security.protocol": getattr(settings, "security_protocol"),
        "sasl.mechanisms": getattr(settings, "sasl_mechanism"),
        "sasl.username": getattr(settings, "api_key"),
        "sasl.password": getattr(settings, "api_secret"),
        "client.dns.lookup": getattr(settings, "client_dns_lookup"),
    }
    consumer_conf = {
        "bootstrap.servers": getattr(settings, "bootstrap_servers"),
        "key.deserializer": key_deserializer,
        "value.deserializer": value_deserializer,
        "group.id": getattr(settings, "group_id"),
        "auto.offset.reset": getattr(settings, "auto_offset_reset"),
        "enable.auto.commit": getattr(settings, "enable_auto_commit"),
        "session.timeout.ms": getattr(settings, KafkaSettingsEnum.SESSION_TIMEOUT_MS),
        "max.poll.interval.ms": getattr(
            settings, KafkaSettingsEnum.MAX_POLL_INTERVAL_MS
        ),
        **security_config,  # Unpack the security config dictionary here
    }

    if custom_config is not None:
        consumer_conf.update(custom_config)

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([getattr(settings, "topic")])
    return consumer


def kafka_listener(
    listener_id: str,
    settings: KafkaBaseSettings,
    on_message: Callable[[dict, dict], None],
    metrics: KafkaConsumerMetrics,
    custom_config: dict[str, Any],
    other_settings_list: Optional[list[KafkaBaseSettings]] = None,
):
    """
    Main Kafka listener loop. Polls Kafka for messages and dispatches them to the given handler.
    Handles errors, retries, and error/DLT routing.

    Args:
        listener_id (str): Unique identifier for the listener.
        settings (KafkaBaseSettings): Listener-specific Kafka settings.
        custom_config (dict[str, Any]): Custom Kafka settings.
        on_message (Callable[[dict, dict], None]): Function to process each valid message.
    """

    consumer = create_consumer(settings, custom_config)
    stop_event = _stop_events[listener_id]

    topic = getattr(settings, "topic")
    max_retries = getattr(settings, "max_retries")
    retry_delay_seconds = getattr(settings, "retry_delay_seconds")

    try:
        while not stop_event.is_set():
            try:
                poll_timeout = getattr(settings, KafkaSettingsEnum.POLL_TIMEOUT)
                msg = consumer.poll(timeout=poll_timeout)
                if msg is None:
                    continue

                # Handle Kafka-level errors (including deserialization)
                if msg.error():
                    err = msg.error()
                    logger.debug(f"Handling Kafka error: {err.code()}: {err.str()}")
                    deserialize_exception = KafkaDeserializationException(
                        message=f"{ErrorCodes.ERR_4004.message}: {err.str()}",
                        error=ErrorCodes.ERR_4004,
                    )
                    produce_to_error_topic(
                        msg,
                        consumer,
                        deserialize_exception,
                        settings,
                        other_settings_list,
                    )
                    logger.error(
                        str(deserialize_exception),
                        extra={
                            "error_code": deserialize_exception.error_code,
                            "kafka_error_code": err.code(),
                            "error_message": err.str(),
                            "original_topic": topic,
                        },
                    )
                    continue

                # Calculate consumer lag
                try:
                    watermarks = consumer.get_watermark_offsets(msg.topic_partition())
                    if watermarks:
                        low, high = watermarks
                        lag = high - msg.offset() - 1
                        if metrics is not None:
                            metrics.update_lag(lag)
                except Exception:
                    pass  # Skip lag update on error

                # Process normal messages
                try:
                    value = msg.value()
                    headers = {
                        k: v.decode() if isinstance(v, bytes) else v
                        for k, v in (msg.headers() or [])
                    }
                    trace_id = headers.get("trace_id") or str(uuid.uuid4())
                    with dynamic_context(trace_id=trace_id):
                        metadata = {
                            "topic": msg.topic(),
                            "key": msg.key(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "timestamp": msg.timestamp(),
                            "headers": headers,
                            "listener_id": listener_id,
                        }

                        # Set retries from x-retry-count header if present
                        retries = int(headers.get("x-retry-count", "0"))

                        # If message is from a retry topic, sleep before processing
                        if metadata["topic"].lower().endswith(RETRY_TOPIC_SUFFIX):
                            logger.info(
                                f"Sleeping for {retry_delay_seconds}s before processing retry topic message."
                            )
                            time.sleep(retry_delay_seconds)

                        start = time.perf_counter()
                        while True:
                            try:
                                on_message(metadata, value)
                                consumer.commit(msg)

                                elapsed = (time.perf_counter() - start) * 1000
                                if metrics is not None:
                                    metrics.count.add(1, {"topic": topic})
                                    metrics.duration.record(elapsed, {"topic": topic})
                                    metrics.bytes.add(
                                        len(msg.value() or b""), {"topic": topic}
                                    )
                                    metrics.update_last_ts(msg.timestamp()[1])

                                break
                            except NonRetryableException as e:
                                if metrics is not None:
                                    metrics.errors.add(1, {"topic": topic})
                                # Non-retryable - send to error topic
                                produce_to_error_topic(
                                    msg, consumer, e, settings, other_settings_list
                                )
                                break
                            except BlockingRetryException as e:
                                retries += 1  # Increment retries for both retry_until_success True/False
                                if e.retry_until_success:
                                    logger.info(
                                        f"Retry until success for message at offset {metadata['offset']} in partition {metadata['partition']} with retry count {retries}. Will retry after sleeping."
                                    )
                                else:
                                    if retries > max_retries:
                                        logger.error(
                                            f"Max retries reached for message at offset {metadata['offset']} in partition {metadata['partition']}. Sending to DLT."
                                        )
                                        produce_to_dlt_topic(
                                            msg,
                                            consumer,
                                            e,
                                            settings,
                                            other_settings_list,
                                        )
                                        break
                                    else:
                                        logger.info(
                                            f"Retry count {retries} for message at offset {metadata['offset']} in partition {metadata['partition']}. Will retry after sleeping."
                                        )
                                time.sleep(retry_delay_seconds)
                                continue
                            except NonBlockingRetryException as e:
                                _process_non_blocking_retry(
                                    msg,
                                    consumer,
                                    e,
                                    settings,
                                    other_settings_list,
                                    headers,
                                    topic,
                                    metadata,
                                    max_retries,
                                )
                                break
                            except Exception as e:
                                logger.error(
                                    f"Exception encountered for message at offset {metadata['offset']} in partition {metadata['partition']}. Error: {e}",
                                    exc_info=e,
                                )
                                produce_to_error_topic(
                                    msg, consumer, e, settings, other_settings_list
                                )
                                break
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}", exc_info=e)

                    produce_to_error_topic(
                        msg, consumer, e, settings, other_settings_list
                    )

            except KafkaException as ke:
                logger.error(
                    ErrorCodes.ERR_4005.message,
                    exc_info=ke,
                    extra={
                        "listener_id": listener_id,
                        "error_code": ErrorCodes.ERR_4005,
                        "error_message": str(ke),
                    },
                )
            except Exception as e:
                logger.error(
                    ErrorCodes.ERR_9002.message,
                    exc_info=e,
                    extra={
                        "listener_id": listener_id,
                        "error_code": ErrorCodes.ERR_9002,
                        "error_message": str(e),
                    },
                )
    finally:
        consumer.close()


def start_listener(
    listener_id: str,
    settings: KafkaBaseSettings,
    callback: Callable[[dict], None],
    metrics: KafkaConsumerMetrics,
    custom_config: Optional[dict[str, Any]] = None,
    other_settings_list: Optional[list[KafkaBaseSettings]] = None,
) -> None:
    """
    Starts a Kafka listener in a separate thread.

    Args:
        listener_id (str): Unique identifier for the listener instance.
        settings (KafkaBaseSettings): Kafka listener configuration.
        callback (Callable): Message processing function.
    """
    if listener_id in _listeners:
        logger.info(f"Listener {listener_id} already running.")
        return

    stop_event = threading.Event()
    _stop_events[listener_id] = stop_event

    thread = threading.Thread(
        target=kafka_listener,
        args=(
            listener_id,
            settings,
            callback,
            metrics,
            custom_config,
            other_settings_list,
        ),
        daemon=True,
    )
    _listeners[listener_id] = thread
    thread.start()


def stop_listener(listener_id: str) -> None:
    """
    Signals a running Kafka listener to stop and waits for its thread to finish.

    Args:
        listener_id (str): Unique identifier for the listener to stop.
    """
    if listener_id not in _listeners:
        logger.info(f"No listener found with ID {listener_id}")
        return

    _stop_events[listener_id].set()
    _listeners[listener_id].join()

    del _listeners[listener_id]
    del _stop_events[listener_id]


def _process_non_blocking_retry(
    msg: Any,
    consumer: DeserializingConsumer,
    non_blocking_exception: NonBlockingRetryException,
    settings: KafkaBaseSettings,
    other_settings_list: Optional[list[KafkaBaseSettings]],
    headers: dict,
    topic: str,
    metadata: dict,
    max_retries: int,
) -> bool:
    """
    Handles NonBlockingRetryException by sending the message to a retry topic or DLT as appropriate.

    Args:
        msg (Any): The Kafka message object.
        consumer (DeserializingConsumer): The Kafka consumer instance.
        non_blocking_exception (NonBlockingRetryException): The exception that triggered the retry.
        settings (KafkaBaseSettings): Kafka settings for the consumer.
        other_settings_list (Optional[list[KafkaBaseSettings]]): Additional settings for error/DLT producers.
        headers (dict): Message headers.
        topic (str): The original topic name.
        metadata (dict): Metadata about the message.
        max_retries (int): Maximum number of retry attempts allowed.

    Returns:
        bool: True if the message should break out of the processing loop, False otherwise.
    """
    current_retry_count = int(headers.get("x-retry-count", "0"))
    current_retry_count += 1

    # Always set x-original-topic to the original consumer topic
    original_consumer_topic = (
        headers["x-original-topic"] if "x-original-topic" in headers else topic
    )
    # Prepare new headers for retry
    new_headers = {k: v for k, v in headers.items()}
    new_headers["x-retry-count"] = str(current_retry_count)
    new_headers["x-original-topic"] = original_consumer_topic
    # Add exception header if present
    if non_blocking_exception.header:
        for k, v in non_blocking_exception.header.items():
            new_headers[k] = v

    if non_blocking_exception.retry_until_success:
        logger.info(
            f"NonBlockingRetryException: Retry until success for message at offset {metadata['offset']} in partition {metadata['partition']} with retry {current_retry_count}. Will retry after sleeping.",
            extra={
                "original_topic": original_consumer_topic,
                "offset": metadata["offset"],
                "partition": metadata["partition"],
                "retry_count": current_retry_count,
            },
        )
    else:
        if current_retry_count > max_retries:
            logger.error(
                f"NonBlockingRetryException: Max retries ({max_retries}) reached for message. Sending to DLT.",
                extra={
                    "original_topic": original_consumer_topic,
                    "offset": metadata["offset"],
                    "partition": metadata["partition"],
                    "retry_count": current_retry_count,
                },
            )
            produce_to_dlt_topic(
                msg,
                consumer,
                non_blocking_exception,
                settings,
                other_settings_list,
            )
            consumer.commit(msg)
            return True  # Break the loop
        else:
            logger.info(
                f"NonBlockingRetryException: Retry attempt {current_retry_count} of {max_retries}. Sending to retry topic.",
                extra={
                    "original_topic": original_consumer_topic,
                    "offset": metadata["offset"],
                    "partition": metadata["partition"],
                },
            )
    # Common retry/DLT handling for both cases
    try:
        produce_to_retry_topic(
            msg,
            consumer,
            non_blocking_exception,
            settings,
            other_settings_list,
            headers=new_headers,
        )
    except Exception:
        produce_to_dlt_topic(
            msg,
            consumer,
            non_blocking_exception,
            settings,
            other_settings_list,
        )
    consumer.commit(msg)
    return True  # Break the loop
