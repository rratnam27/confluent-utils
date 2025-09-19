import pytest
from unittest.mock import patch, MagicMock
from src.confluent_utils.kafka_consumer import KafkaConsumer, resolve_handler
from x35_observability.kafka_consumer_metrics import KafkaConsumerMetrics


@pytest.fixture
def fake_settings_list():
    # Create two listeners: one with count=1, one with count=2
    listener1 = MagicMock()
    listener1.listener_id = "listener1"
    listener1.count = 1
    listener1.topic = "topic1"
    listener1.group_id = "group1"
    listener1.handler = "some.module.handler"

    listener2 = MagicMock()
    listener2.listener_id = "listener2"
    listener2.count = 2
    listener2.topic = "topic2"
    listener2.group_id = "group2"
    listener2.handler = "other.module.handler"

    return [listener1, listener2]


@patch("src.confluent_utils.kafka_consumer.start_listener")
@patch("src.confluent_utils.kafka_consumer.resolve_handler")
def test_start_all_calls_start_listener(mock_resolve, mock_start, fake_settings_list):
    handler1_mock = MagicMock()
    handler2_mock = MagicMock()
    mock_resolve.side_effect = [handler1_mock, handler2_mock]
    consumer = KafkaConsumer(fake_settings_list)
    consumer.start_all()
    # listener1: count=1, listener2: count=2
    assert mock_start.call_count == 3
    # Check calls for listener1 and listener2 instances, including extra None arguments

    def is_metrics(arg):
        return (arg is None) or isinstance(arg, KafkaConsumerMetrics)

    found_listener1 = False
    found_listener2_1 = False
    found_listener2_2 = False
    for call_args in mock_start.call_args_list:
        args = call_args[0]
        if (
            args[0] == "listener1"
            and args[1] == fake_settings_list[0]
            and args[2] == handler1_mock
            and is_metrics(args[3])
            and args[4] is None
            and args[5] is None
        ):
            found_listener1 = True
        if (
            args[0] == "listener2_1"
            and args[1] == fake_settings_list[1]
            and args[2] == handler2_mock
            and is_metrics(args[3])
            and args[4] is None
            and args[5] is None
        ):
            found_listener2_1 = True
        if (
            args[0] == "listener2_2"
            and args[1] == fake_settings_list[1]
            and args[2] == handler2_mock
            and is_metrics(args[3])
            and args[4] is None
            and args[5] is None
        ):
            found_listener2_2 = True
    assert found_listener1
    assert found_listener2_1
    assert found_listener2_2


@patch("src.confluent_utils.kafka_consumer.stop_listener")
def test_stop_all_calls_stop_listener(mock_stop, fake_settings_list):
    consumer = KafkaConsumer(fake_settings_list)
    consumer.stop_all()
    # listener1: count=1, listener2: count=2
    assert mock_stop.call_count == 3
    mock_stop.assert_any_call("listener1")
    mock_stop.assert_any_call("listener2_1")
    mock_stop.assert_any_call("listener2_2")


def test_resolve_handler_import_error():
    with pytest.raises(ImportError):
        resolve_handler("not.a.real.module.func")
