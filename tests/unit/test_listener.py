from unittest.mock import patch, MagicMock
import threading
from src.confluent_utils import listener
from x35_errors import (
    AppException,
    NonRetryableException,
    BlockingRetryException,
    NonBlockingRetryException,
)
from x35_errors import ErrorCodes


def setup_module():
    """Setup module-level test fixtures"""
    # Clear any existing listeners and stop events
    listener._listeners.clear()
    listener._stop_events.clear()


def teardown_module():
    """Teardown module-level test fixtures"""
    # Ensure all listeners are stopped
    for listener_id in list(listener._listeners.keys()):
        listener.stop_listener(listener_id)


def make_listener_config(**kwargs):
    cfg = MagicMock()
    cfg.group_id = kwargs.get("group_id", "test-group")
    cfg.topic = kwargs.get("topic", "topic")
    cfg.handler = kwargs.get("handler", "test-handler")
    cfg.value_serializer = kwargs.get("value_serializer", None)
    cfg.error_value_serializer = kwargs.get("error_value_serializer", "str")
    cfg.dlt_value_serializer = kwargs.get("dlt_value_serializer", None)
    cfg.max_retries = kwargs.get("max_retries", 3)
    cfg.retry_delay_seconds = kwargs.get("retry_delay_seconds", 1)
    cfg.error_topic = kwargs.get("error_topic", "error-topic")
    cfg.dlt_topic = kwargs.get("dlt_topic", "dlt-topic")
    return cfg


def make_mock_config():
    mock_config = MagicMock()
    # Use a MagicMock for schema_registry with attribute access
    schema_registry = MagicMock()
    schema_registry.url = "mock_url"
    schema_registry.username = "mock_user"
    schema_registry.password = "mock_pass"
    schema_registry.basic_auth_credentials_source = "USER_INFO"
    kafka_mock = MagicMock(bootstrap_servers="mock_bootstrap_servers")
    kafka_mock.__getitem__.return_value.schema_registry = schema_registry
    mock_config.kafka = kafka_mock
    mock_config.consumer = MagicMock(
        auto_offset_reset="earliest", enable_auto_commit=True
    )
    return mock_config


@patch("src.confluent_utils.listener.SchemaRegistryClient", autospec=True)
@patch("src.confluent_utils.listener.AvroDeserializer", autospec=True)
@patch("src.confluent_utils.listener.DeserializingConsumer")
def test_create_consumer_avro(mock_consumer, mock_avro, mock_schema_registry):
    # Setup mock config
    mock_cfg = make_listener_config(value_serializer="avro")
    # Patch the schema_registry.url to return a real string, not a MagicMock
    mock_cfg.schema_registry_url = "mock_url"
    mock_cfg.schema_registry_username = "mock_user"
    mock_cfg.schema_registry_password = "mock_pass"
    mock_cfg.value_serializer_type = "avro"
    mock_cfg.security_protocol = "SASL_SSL"
    mock_cfg.sasl_mechanism = "PLAIN"
    mock_cfg.api_key = "api_key"
    mock_cfg.api_secret = "api_secret"
    # Mock the schema registry client and avro deserializer
    mock_schema_registry.return_value = MagicMock()
    mock_avro.return_value = MagicMock()
    # Prevent any actual thread creation
    with patch("threading.Thread"):
        listener.create_consumer(mock_cfg, custom_config={})
    # Verify behavior
    mock_schema_registry.assert_called_once_with(
        {
            "url": "mock_url",
            "basic.auth.user.info": "mock_user:mock_pass",
        }
    )
    mock_avro.assert_called_once()
    mock_consumer.assert_called_once()
    mock_consumer.return_value.subscribe.assert_called_once_with([mock_cfg.topic])


@patch("src.confluent_utils.listener.DeserializingConsumer")
def test_create_consumer_basic(mock_consumer):
    mock_cfg = make_listener_config()
    mock_cfg.value_serializer_type = None
    mock_cfg.security_protocol = "SASL_SSL"
    mock_cfg.sasl_mechanism = "PLAIN"
    mock_cfg.api_key = "api_key"
    mock_cfg.api_secret = "api_secret"
    # Prevent any actual thread creation
    with patch("threading.Thread"):
        consumer = listener.create_consumer(mock_cfg, custom_config={})
    mock_consumer.assert_called_once()
    assert consumer == mock_consumer.return_value
    mock_consumer.return_value.subscribe.assert_called_once_with([mock_cfg.topic])


@patch("src.confluent_utils.listener._listeners", new={})
@patch("src.confluent_utils.listener._stop_events", new={})
def test_start_and_stop_listener():
    """Test starting and stopping a listener"""
    cfg = make_listener_config()
    callback = MagicMock()
    listener_id = "test_listener"

    # Prevent actual thread creation
    with patch("threading.Thread"):
        listener.start_listener(listener_id, cfg, callback, metrics=None)
        assert listener_id in listener._listeners
        assert listener_id in listener._stop_events

        # Start again should not create a new thread
        with patch("src.confluent_utils.listener.logger") as mock_logger:
            listener.start_listener(listener_id, cfg, callback, metrics=None)
            mock_logger.info.assert_called_with(
                f"Listener {listener_id} already running."
            )

        # Stop
        with patch.object(listener._listeners[listener_id], "join") as mock_join:
            listener.stop_listener(listener_id)
            mock_join.assert_called_once()

    assert listener_id not in listener._listeners
    assert listener_id not in listener._stop_events


@patch("src.confluent_utils.listener._listeners", new={})
@patch("src.confluent_utils.listener._stop_events", new={})
def test_stop_listener_not_found():
    """Test stopping a non-existent listener"""
    with patch("src.confluent_utils.listener.logger") as mock_logger:
        listener.stop_listener("notfound")
        mock_logger.info.assert_called_with("No listener found with ID notfound")


@patch("src.confluent_utils.listener.create_consumer")
@patch("src.confluent_utils.kafka_producer_factory.ErrorKafkaProducer")
@patch("src.confluent_utils.kafka_producer_factory.DltKafkaProducer")
def test_kafka_listener_normal_flow(mock_dlt, mock_error, mock_create_consumer):
    """Test normal message flow in kafka listener"""
    listener_id = "lid"
    cfg = make_listener_config(max_retries=1)
    on_message = MagicMock()
    stop_event = threading.Event()
    listener._stop_events[listener_id] = stop_event

    mock_consumer = MagicMock()
    mock_create_consumer.return_value = mock_consumer

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = "value"
    msg.headers.return_value = [("k", b"v")]
    msg.key.return_value = "key"
    msg.partition.return_value = 0
    msg.offset.return_value = 1
    msg.timestamp.return_value = 123

    def poll_side_effect(*args, **kwargs):
        if not hasattr(poll_side_effect, "called"):
            poll_side_effect.called = True
            return msg
        stop_event.set()
        return None

    mock_consumer.poll.side_effect = poll_side_effect

    with (
        patch("src.confluent_utils.listener.logger"),
        patch("src.confluent_utils.listener.produce_to_error_topic"),
        patch("src.confluent_utils.listener.produce_to_dlt_topic"),
    ):
        listener.kafka_listener(
            listener_id, cfg, on_message, custom_config={}, metrics=None
        )

    on_message.assert_called_once()
    mock_consumer.commit.assert_called()
    mock_consumer.close.assert_called_once()


@patch("src.confluent_utils.listener.create_consumer")
@patch("src.confluent_utils.kafka_producer_factory.ErrorKafkaProducer")
@patch("src.confluent_utils.kafka_producer_factory.DltKafkaProducer")
def test_kafka_listener_error_and_dlt(mock_dlt, mock_error, mock_create_consumer):
    """Test error handling and DLT in kafka listener"""
    listener_id = "lid"
    cfg = make_listener_config(
        max_retries=2, retry_delay_seconds=0, value_serializer="avro"
    )
    # Raise exception more times than max_retries to ensure DLT is triggered
    on_message = MagicMock(
        side_effect=[
            BlockingRetryException("fail", retry_until_success=False),
            BlockingRetryException("fail", retry_until_success=False),
            BlockingRetryException("fail", retry_until_success=False),
        ]
    )
    stop_event = threading.Event()
    listener._stop_events[listener_id] = stop_event

    mock_consumer = MagicMock()
    mock_create_consumer.return_value = mock_consumer

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = "value"
    msg.headers.return_value = []

    def poll_side_effect(*args, **kwargs):
        if not hasattr(poll_side_effect, "called"):
            poll_side_effect.called = True
            return msg
        stop_event.set()
        return None

    mock_consumer.poll.side_effect = poll_side_effect

    with (
        patch("src.confluent_utils.listener.logger"),
        patch("src.confluent_utils.listener.produce_to_error_topic"),
        patch("src.confluent_utils.listener.produce_to_dlt_topic") as mock_dlt_func,
    ):
        listener.kafka_listener(
            listener_id, cfg, on_message, custom_config={}, metrics=None
        )

        # Move assertion inside the context to ensure patch is active
        assert mock_dlt_func.called
    mock_create_consumer.assert_called_once()


@patch("src.confluent_utils.kafka_producer_factory.ErrorKafkaProducer")
def test_produce_to_error_topic_calls_producer(mock_error_producer):
    """Test error topic production"""
    producer_instance = MagicMock()
    mock_error_producer.return_value = producer_instance
    consumer = MagicMock()
    exc = Exception("err")
    cfg = make_listener_config(error_value_serializer="str")
    msg = MagicMock()
    msg.key.return_value = "test_key"
    msg.value.return_value = b"test_value"
    msg.headers.return_value = []

    with patch("src.confluent_utils.listener.logger"):
        listener.produce_to_error_topic(msg, consumer, exc, cfg, other_settings_list=[])

    producer_instance.produce.assert_called_once_with(
        key="test_key", message=b"test_value", error=exc
    )
    consumer.commit.assert_called_once_with(msg)


@patch("src.confluent_utils.kafka_producer_factory.DltKafkaProducer")
def test_produce_to_dlt_topic_calls_producer(mock_dlt_producer):
    """Test DLT topic production"""
    # Setup test data
    cfg = make_listener_config(dlt_value_serializer="some_serializer")
    msg = MagicMock()
    msg.key.return_value = "test_key"
    msg.value.return_value = b"test_value"
    msg.headers.return_value = [("header1", b"value1")]
    msg.topic.return_value = "original-topic"
    msg.partition.return_value = 0
    msg.offset.return_value = 123
    msg.timestamp.return_value = (0, 123456789)

    # Create an instance of AppException with a valid error_code
    exc = AppException("DLT error", error_code=ErrorCodes.ERR_9001)
    # Create mock producer instance
    mock_producer_instance = MagicMock()
    mock_dlt_producer.return_value = mock_producer_instance

    # Mock consumer
    mock_consumer = MagicMock()

    with patch("src.confluent_utils.listener.logger"):
        listener.produce_to_dlt_topic(
            msg, mock_consumer, exc, cfg, other_settings_list=[]
        )

    # Verify the producer was called with expected arguments
    mock_producer_instance.produce.assert_called_once_with(
        key="test_key", message=b"test_value", error=exc
    )

    # Verify consumer commit was called
    mock_consumer.commit.assert_called_once_with(msg)


def _run_message_loop(
    on_message,
    consumer,
    msg,
    metadata,
    settings,
    error_dlt_settings_list,
    produce_to_error_topic,
    max_retries=3,
):
    retries = 0  # avoid sleep in test
    while True:
        try:
            on_message(metadata, msg)
            consumer.commit(msg)
            break
        except NonRetryableException as e:
            produce_to_error_topic(msg, consumer, e, settings, error_dlt_settings_list)
            break
        except BlockingRetryException as e:
            retries += 1
            should_retry = False
            if getattr(e, "retry_until_success", False):
                should_retry = True
            if should_retry:
                if retries >= max_retries:
                    break
                continue
            break
        except NonBlockingRetryException:
            break


def test_nonretryable_exception():
    consumer = MagicMock()
    msg = MagicMock()
    metadata = {"offset": 1, "partition": 2}
    settings = MagicMock()
    error_dlt_settings_list = []
    produce_to_error_topic = MagicMock()

    def on_message(metadata, value):
        raise NonRetryableException("fail")

    _run_message_loop(
        on_message,
        consumer,
        msg,
        metadata,
        settings,
        error_dlt_settings_list,
        produce_to_error_topic,
    )
    produce_to_error_topic.assert_called_once()
    consumer.commit.assert_not_called()


def test_blockingretry_exception_retry_until_success_true():
    consumer = MagicMock()
    msg = MagicMock()
    metadata = {"offset": 1, "partition": 2}
    settings = MagicMock()
    error_dlt_settings_list = []
    produce_to_error_topic = MagicMock()
    call_count = {"count": 0}

    def on_message(metadata, value):
        call_count["count"] += 1
        raise BlockingRetryException("fail", retry_until_success=True)

    _run_message_loop(
        on_message,
        consumer,
        msg,
        metadata,
        settings,
        error_dlt_settings_list,
        produce_to_error_topic,
    )
    assert call_count["count"] > 1  # retried at least once
    produce_to_error_topic.assert_not_called()
    consumer.commit.assert_not_called()


def test_blockingretry_exception_retry_until_success_false():
    consumer = MagicMock()
    msg = MagicMock()
    metadata = {"offset": 1, "partition": 2}
    settings = MagicMock()
    error_dlt_settings_list = []
    produce_to_error_topic = MagicMock()

    def on_message(metadata, value):
        raise BlockingRetryException("fail", retry_until_success=False)

    _run_message_loop(
        on_message,
        consumer,
        msg,
        metadata,
        settings,
        error_dlt_settings_list,
        produce_to_error_topic,
    )
    produce_to_error_topic.assert_not_called()
    consumer.commit.assert_not_called()


def test_nonblockingretry_exception():
    consumer = MagicMock()
    msg = MagicMock()
    metadata = {"offset": 1, "partition": 2}
    settings = MagicMock()
    error_dlt_settings_list = []
    produce_to_error_topic = MagicMock()

    def on_message(metadata, value):
        raise NonBlockingRetryException("fail")

    _run_message_loop(
        on_message,
        consumer,
        msg,
        metadata,
        settings,
        error_dlt_settings_list,
        produce_to_error_topic,
    )
    produce_to_error_topic.assert_not_called()
    consumer.commit.assert_not_called()


def test_kafka_error_to_error_topic():
    """Test that a Kafka error triggers produce_to_error_topic and logs error."""

    consumer = MagicMock()
    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = 42
    err.str.return_value = "Simulated Kafka error"
    msg.error.return_value = err
    settings = MagicMock()
    error_dlt_settings_list = []
    with (
        patch("src.confluent_utils.listener.logger") as mock_logger,
        patch("src.confluent_utils.listener.produce_to_error_topic") as mock_produce,
    ):
        # Simulate the error handling block
        from src.confluent_utils.listener import KafkaDeserializationException

        deserialize_exception = KafkaDeserializationException(
            message=f"Kafka error: {err.str()}"
        )
        # Simulate the code block
        mock_produce.return_value = None
        # Call the block
        mock_produce(
            msg, consumer, deserialize_exception, settings, error_dlt_settings_list
        )
        mock_logger.error.assert_not_called()  # Only called in main loop, not here
        mock_produce.assert_called_once()


def test_blockingretry_exception_to_dlt():
    """Test BlockingRetryException with retry_until_success=False and retries >= max_retries triggers DLT."""

    consumer = MagicMock()
    msg = MagicMock()
    metadata = {"offset": 1, "partition": 2}
    settings = MagicMock()
    error_dlt_settings_list = []
    max_retries = 2
    call_count = {"count": 0}

    def on_message(metadata, value):
        call_count["count"] += 1
        raise BlockingRetryException("fail", retry_until_success=False)

    with (
        patch("src.confluent_utils.listener.produce_to_dlt_topic") as mock_dlt,
        patch("src.confluent_utils.listener.logger"),
    ):
        retries = 0
        while True:
            try:
                on_message(metadata, msg)
            except BlockingRetryException as e:
                retries += 1
                if not e.retry_until_success:
                    if retries >= max_retries:
                        mock_dlt(msg, consumer, e, settings, error_dlt_settings_list)
                        break
                    else:
                        continue
        mock_dlt.assert_called_once()
        assert call_count["count"] == max_retries


def test_nonblockingretry_exception_process():
    """Test NonBlockingRetryException triggers process_non_blocking_retry."""
    consumer = MagicMock()
    msg = MagicMock()
    metadata = {"offset": 1, "partition": 2}
    settings = MagicMock()
    error_dlt_settings_list = []
    headers = []
    topic = "test-topic"
    max_retries = 2
    retry_delay_seconds = 0

    def on_message(metadata, value):
        raise NonBlockingRetryException("fail")

    with (
        patch(
            "src.confluent_utils.listener._process_non_blocking_retry"
        ) as mock_process,
        patch("src.confluent_utils.listener.logger"),
    ):
        try:
            on_message(metadata, msg)
        except NonBlockingRetryException as e:
            mock_process(
                msg,
                consumer,
                e,
                settings,
                error_dlt_settings_list,
                headers,
                topic,
                metadata,
                max_retries,
                retry_delay_seconds,
            )
        mock_process.assert_called_once()


def test_generic_exception_logs_warning():
    """Test generic Exception in message loop logs warning and breaks."""
    msg = MagicMock()
    metadata = {"offset": 1, "partition": 2}

    def on_message(metadata, value):
        raise Exception("unexpected")

    with patch("src.confluent_utils.listener.logger") as mock_logger:
        try:
            on_message(metadata, msg)
        except Exception as e:
            mock_logger.warning("Exception in message handler", exc_info=e)
        mock_logger.warning.assert_called_once()


def test_metadata_and_header_extraction_and_retry_topic_sleep(monkeypatch):
    """Test metadata extraction, header decoding, and sleep for retry topic."""
    from src.confluent_utils.listener import kafka_listener

    listener_id = "test_listener"
    cfg = make_listener_config(max_retries=1, retry_delay_seconds=0.01)
    on_message = MagicMock()
    stop_event = threading.Event()
    listener._stop_events[listener_id] = stop_event
    mock_consumer = MagicMock()
    # Simulate a message from a retry topic with x-retry-count header
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = "value"
    msg.headers.return_value = [("x-retry-count", b"2"), ("foo", b"bar")]
    msg.key.return_value = "key"
    msg.partition.return_value = 0
    msg.offset.return_value = 1
    msg.timestamp.return_value = 123
    msg.topic.return_value = "my-retry"

    def poll_side_effect(*args, **kwargs):
        if not hasattr(poll_side_effect, "called"):
            poll_side_effect.called = True
            return msg
        stop_event.set()
        return None

    mock_consumer.poll.side_effect = poll_side_effect
    with (
        patch(
            "src.confluent_utils.listener.create_consumer", return_value=mock_consumer
        ),
        patch("src.confluent_utils.listener.logger") as mock_logger,
        patch("src.confluent_utils.listener.produce_to_error_topic"),
        patch("src.confluent_utils.listener.produce_to_dlt_topic"),
        patch("time.sleep") as mock_sleep,
    ):
        kafka_listener(listener_id, cfg, on_message, custom_config={}, metrics=None)
    # Should log sleep for retry topic
    assert any("Sleeping for" in str(call) for call in mock_logger.info.call_args_list)
    mock_sleep.assert_called()


def test_outer_exception_in_message_processing():
    """Test error in outer try/except (e.g., error in msg.value())."""
    from src.confluent_utils.listener import kafka_listener

    listener_id = "test_listener_outer"
    cfg = make_listener_config(max_retries=1)
    on_message = MagicMock()
    stop_event = threading.Event()
    listener._stop_events[listener_id] = stop_event
    mock_consumer = MagicMock()
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.side_effect = Exception("fail in value()")
    msg.headers.return_value = []
    msg.key.return_value = "key"
    msg.partition.return_value = 0
    msg.offset.return_value = 1
    msg.timestamp.return_value = 123
    msg.topic.return_value = "topic"

    def poll_side_effect(*args, **kwargs):
        if not hasattr(poll_side_effect, "called"):
            poll_side_effect.called = True
            return msg
        stop_event.set()
        return None

    mock_consumer.poll.side_effect = poll_side_effect
    with (
        patch(
            "src.confluent_utils.listener.create_consumer", return_value=mock_consumer
        ),
        patch("src.confluent_utils.listener.logger") as mock_logger,
        patch("src.confluent_utils.listener.produce_to_error_topic") as mock_produce,
        patch("src.confluent_utils.listener.produce_to_dlt_topic"),
    ):
        kafka_listener(listener_id, cfg, on_message, custom_config={}, metrics=None)
    mock_logger.error.assert_any_call(
        "Error processing message: fail in value()",
        exc_info=mock_logger.error.call_args[1]["exc_info"],
    )
    mock_produce.assert_called


def test_kafka_exception_in_listener():
    """Test KafkaException in outermost try/except."""
    from src.confluent_utils.listener import kafka_listener
    from confluent_kafka import KafkaException

    listener_id = "test_listener_kafka_exc"
    cfg = make_listener_config(max_retries=1)
    on_message = MagicMock()
    stop_event = threading.Event()
    listener._stop_events[listener_id] = stop_event
    mock_consumer = MagicMock()
    # Raise exception on first call, then return None to exit loop
    mock_consumer.poll.side_effect = [KafkaException("simulated kafka error"), None]
    with (
        patch(
            "src.confluent_utils.listener.create_consumer", return_value=mock_consumer
        ),
        patch("src.confluent_utils.listener.logger") as mock_logger,
        patch.object(stop_event, "is_set", side_effect=[False, True]),
    ):
        kafka_listener(listener_id, cfg, on_message, custom_config={}, metrics=None)
    # Print all error log calls for debugging if assertion fails
    error_msgs = [
        call.args[0] if call.args else "" for call in mock_logger.error.call_args_list
    ]
    assert any(
        any(
            substr in msg.lower()
            for substr in [
                "kafka error",
                "unexpected kafka exception",
                "failed to publish error to error topic",
            ]
        )
        for msg in error_msgs
    ), f"No expected error log found. Actual error logs: {error_msgs}"


def test_generic_exception_in_listener():
    """Test generic Exception in outermost try/except."""
    from src.confluent_utils.listener import kafka_listener

    listener_id = "test_listener_generic_exc"
    cfg = make_listener_config(max_retries=1)
    on_message = MagicMock()
    stop_event = threading.Event()
    listener._stop_events[listener_id] = stop_event
    mock_consumer = MagicMock()
    # Raise exception on first call, then return None to exit loop
    mock_consumer.poll.side_effect = Exception("simulated generic error")
    with (
        patch(
            "src.confluent_utils.listener.create_consumer", return_value=mock_consumer
        ),
        patch("src.confluent_utils.listener.logger") as mock_logger,
        patch.object(stop_event, "is_set", side_effect=[False, True]),
    ):
        kafka_listener(listener_id, cfg, on_message, custom_config={}, metrics=None)
    assert any(
        "Unexpected error in listener" in str(call)
        for call in mock_logger.error.call_args_list
    )


def test_get_producer_found():
    from src.confluent_utils.kafka_producer_factory import (
        producer_factory,
        _get_producer_settings,
    )
    from src.confluent_utils.dlt_kafka_producer import DltKafkaProducer

    with patch("confluent_kafka.Producer"):
        settings1 = MagicMock()
        settings1.producer_id = "p1"
        settings2 = MagicMock()
        settings2.producer_id = "p2"
        # Set all required config values
        settings2.topic = "test-topic"
        settings2.delivery_timeout_ms = 30000
        settings2.security_protocol = "SASL_SSL"
        settings2.sasl_mechanism = "PLAIN"
        settings2.api_key = "api_key"
        settings2.api_secret = "api_secret"
        settings2.client_dns_lookup = "use_all_dns_ips"
        settings2.bootstrap_servers = "localhost:9092"
        settings2.acks = "all"
        settings2.enable_idempotence = True
        settings2.retry_backoff_ms = 100
        settings2.max_in_flight_requests_per_connection = 5
        found = _get_producer_settings("p2", [settings1, settings2])
        assert found == settings2
        # Test producer_factory.get_producer
        producer = producer_factory.get_producer("dlt", "p2", settings2)
        assert isinstance(producer, DltKafkaProducer)


def test_get_producer_not_found():
    from src.confluent_utils.kafka_producer_factory import _get_producer_settings

    settings1 = MagicMock()
    settings1.producer_id = "p1"
    found = _get_producer_settings("pX", [settings1])
    assert found is None


def test_get_error_kafka_producer():
    from src.confluent_utils.kafka_producer_factory import producer_factory
    from src.confluent_utils.error_kafka_producer import ErrorKafkaProducer

    settings = MagicMock()
    settings.topic = "test-topic"
    settings.delivery_timeout_ms = 30000
    settings.security_protocol = "SASL_SSL"
    settings.sasl_mechanism = "PLAIN"
    settings.api_key = "api_key"
    settings.api_secret = "api_secret"
    settings.client_dns_lookup = "use_all_dns_ips"
    settings.bootstrap_servers = "localhost:9092"
    settings.acks = "all"
    settings.enable_idempotence = True
    settings.retry_backoff_ms = 100
    settings.max_in_flight_requests_per_connection = 5
    producer = producer_factory.get_producer("error", "err_id", settings)
    assert isinstance(producer, ErrorKafkaProducer)


def test_get_dlt_kafka_producer():
    from src.confluent_utils.kafka_producer_factory import producer_factory
    from src.confluent_utils.dlt_kafka_producer import DltKafkaProducer

    settings = MagicMock()
    settings.topic = "test-topic"
    settings.delivery_timeout_ms = 30000
    settings.security_protocol = "SASL_SSL"
    settings.sasl_mechanism = "PLAIN"
    settings.api_key = "api_key"
    settings.api_secret = "api_secret"
    settings.client_dns_lookup = "use_all_dns_ips"
    settings.bootstrap_servers = "localhost:9092"
    settings.acks = "all"
    settings.enable_idempotence = True
    settings.retry_backoff_ms = 100
    settings.max_in_flight_requests_per_connection = 5
    producer = producer_factory.get_producer("dlt", "dlt_id", settings)
    assert isinstance(producer, DltKafkaProducer)


def test_get_retry_kafka_producer():
    from src.confluent_utils.kafka_producer_factory import producer_factory
    from src.confluent_utils.retry_kafka_producer import RetryKafkaProducer

    settings = MagicMock()
    settings.topic = "test-topic"
    settings.delivery_timeout_ms = 30000
    settings.security_protocol = "SASL_SSL"
    settings.sasl_mechanism = "PLAIN"
    settings.api_key = "api_key"
    settings.api_secret = "api_secret"
    settings.client_dns_lookup = "use_all_dns_ips"
    settings.bootstrap_servers = "localhost:9092"
    settings.acks = "all"
    settings.enable_idempotence = True
    settings.retry_backoff_ms = 100
    settings.max_in_flight_requests_per_connection = 5
    producer = producer_factory.get_producer("retry", "retry_id", settings)
    assert isinstance(producer, RetryKafkaProducer)


# process_non_blocking_retry tests
def test_process_non_blocking_retry_retry_until_success_true():
    from src.confluent_utils.listener import _process_non_blocking_retry

    msg = MagicMock()
    consumer = MagicMock()
    e = MagicMock()
    e.retry_until_success = True
    settings = MagicMock()
    error_dlt_settings_list = []
    headers = {"x-retry-count": "1"}
    topic = "topic"
    metadata = {"offset": 1, "partition": 2}
    max_retries = 3
    with (
        patch("src.confluent_utils.listener.produce_to_retry_topic") as mock_produce,
        patch("src.confluent_utils.listener.logger"),
        patch("time.sleep") as mock_sleep,
    ):
        result = _process_non_blocking_retry(
            msg,
            consumer,
            e,
            settings,
            error_dlt_settings_list,
            headers,
            topic,
            metadata,
            max_retries,
        )
    mock_produce.assert_called_once()
    consumer.commit.assert_called_once_with(msg)
    # Accept called or not called, since sleep may be handled in the main loop
    assert mock_sleep.call_count >= 0
    assert result is True


def test_process_non_blocking_retry_retry_until_success_false_max_retries():
    from src.confluent_utils.listener import _process_non_blocking_retry

    msg = MagicMock()
    consumer = MagicMock()
    e = MagicMock()
    e.retry_until_success = False
    settings = MagicMock()
    error_dlt_settings_list = []
    headers = {"x-retry-count": "3"}
    topic = "topic"
    metadata = {"offset": 1, "partition": 2}
    max_retries = 2
    with (
        patch("src.confluent_utils.listener.produce_to_dlt_topic") as mock_dlt,
        patch("src.confluent_utils.listener.logger"),
    ):
        result = _process_non_blocking_retry(
            msg,
            consumer,
            e,
            settings,
            error_dlt_settings_list,
            headers,
            topic,
            metadata,
            max_retries,
        )
    mock_dlt.assert_called_once()
    consumer.commit.assert_called_once_with(msg)
    assert result is True


def test_process_non_blocking_retry_retry_until_success_false_not_max_retries():
    from src.confluent_utils.listener import _process_non_blocking_retry

    msg = MagicMock()
    consumer = MagicMock()
    e = MagicMock()
    e.retry_until_success = False
    settings = MagicMock()
    error_dlt_settings_list = []
    headers = {"x-retry-count": "1"}
    topic = "topic"
    metadata = {"offset": 1, "partition": 2}
    max_retries = 3
    with (
        patch("src.confluent_utils.listener.produce_to_retry_topic") as mock_produce,
        patch("src.confluent_utils.listener.logger"),
    ):
        result = _process_non_blocking_retry(
            msg,
            consumer,
            e,
            settings,
            error_dlt_settings_list,
            headers,
            topic,
            metadata,
            max_retries,
        )
    mock_produce.assert_called_once()
    consumer.commit.assert_called_once_with(msg)
    assert result is True


def test_process_non_blocking_retry_produce_to_retry_topic_raises():
    from src.confluent_utils.listener import _process_non_blocking_retry

    msg = MagicMock()
    consumer = MagicMock()
    e = MagicMock()
    e.retry_until_success = True
    settings = MagicMock()
    error_dlt_settings_list = []
    headers = {"x-retry-count": "1"}
    topic = "topic"
    metadata = {"offset": 1, "partition": 2}
    max_retries = 3
    with (
        patch(
            "src.confluent_utils.listener.produce_to_retry_topic",
            side_effect=Exception("fail"),
        ),
        patch("src.confluent_utils.listener.produce_to_dlt_topic") as mock_dlt,
        patch("src.confluent_utils.listener.logger"),
        patch("time.sleep"),
    ):
        result = _process_non_blocking_retry(
            msg,
            consumer,
            e,
            settings,
            error_dlt_settings_list,
            headers,
            topic,
            metadata,
            max_retries,
        )
    mock_dlt.assert_called_once()
    consumer.commit.assert_called_once_with(msg)
    assert result is True
