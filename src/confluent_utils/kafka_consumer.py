import logging
import importlib
from types import ModuleType
from typing import Callable, Optional, Any

from .listener import start_listener, stop_listener
from x35_settings import KafkaBaseSettings
from x35_observability import KafkaConsumerMetrics, observability_settings
from opentelemetry import metrics

logger = logging.getLogger(f"x35.{__name__}")


class KafkaConsumer:
    """
    KafkaConsumer is responsible for managing the lifecycle of Kafka listeners
    as defined in the application configuration. It supports starting and stopping
    all listeners or individual ones, and resolving the appropriate handler functions.
    """

    def __init__(
        self,
        settings_list: list[KafkaBaseSettings],
        custom_config: Optional[dict[str, Any]] = None,
        other_settings_list: Optional[list[KafkaBaseSettings]] = None,
    ):
        """
        Initializes the KafkaConsumer by loading configuration and preparing
        listener settings from the config.
        """
        self.settings_list = settings_list
        self.custom_config = custom_config
        self.other_settings_list = other_settings_list
        self._metrics_map = {}  # mapping listener_id -> metrics

    def start_all(self):
        """
        Starts all Kafka listeners defined in the configuration. If a listener is configured
        to run with multiple instances, it starts each instance with a unique ID.
        """
        meter = metrics.get_meter("app.kafka.consumer")
        for settings in self.settings_list:
            listener_id = getattr(settings, "listener_id")
            listener_count = getattr(settings, "count")
            topic = getattr(settings, "topic")
            group_id = getattr(settings, "group_id")
            handler = resolve_handler(getattr(settings, "handler"))

            # Create metrics instance per listener_id only if observability is enabled
            if (
                observability_settings.otlp_metrics_exporter
                and observability_settings.otlp_traces_exporter
            ):
                consumer_metrics = KafkaConsumerMetrics(meter)
            else:
                consumer_metrics = None
            self._metrics_map[listener_id] = consumer_metrics

            for i in range(listener_count):
                instance_id = (
                    f"{listener_id}_{i + 1}" if listener_count > 1 else listener_id
                )
                logger.info(
                    f"Starting Kafka listener [{instance_id}] on topic '{topic}' with group '{group_id}'..."
                )
                start_listener(
                    instance_id,
                    settings,
                    handler,
                    consumer_metrics,
                    self.custom_config,
                    self.other_settings_list,
                )

    def stop_all(self):
        """
        Stops all Kafka listeners that are currently running, based on the configuration.
        """
        for settings in self.settings_list:
            listener_id = getattr(settings, "listener_id")
            listener_count = getattr(settings, "count")

            for i in range(listener_count):
                instance_id = (
                    f"{listener_id}_{i + 1}" if listener_count > 1 else listener_id
                )
                logger.info(f"Stopping Kafka listener [{instance_id}]...")
                stop_listener(instance_id)

        self._metrics_map.clear()


def resolve_handler(dotted_path: str) -> Callable:
    """
    Resolves a dotted path string to an actual Python callable.

    Args:
        dotted_path (str): The full dotted path to the handler, e.g. 'my_module.my_handler'.

    Returns:
        Callable: The resolved Python callable.

    Raises:
        ImportError: If the module or handler cannot be found or imported.
    """
    try:
        module_path, func_name = dotted_path.rsplit(".", 1)
        module: ModuleType = importlib.import_module(module_path)
        handler: Callable = getattr(module, func_name)
        return handler
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Could not resolve handler '{dotted_path}': {e}")
