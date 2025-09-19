"""
This module provides a Kafka batch consumer service for reading messages from a Kafka topic partition.

Classes:
    KafkaBatchConsumer: Handles subscribing, consuming, committing, and closing a Kafka consumer for batch processing.

Methods:
    get_offset_range: Returns the low and high offsets for the topic partition and whether it is empty.
    assign: Assigns the consumer to a specific offset in the topic partition.
    consume_batch: Consumes a batch of messages from the topic partition.
    commit: Commits the current offset for the consumer.
    close: Closes the consumer and cleans up resources.
"""

from confluent_kafka import TopicPartition, Consumer
from .enums import KafkaSettingsEnum

from x35_settings import KafkaConsumerSettings

import logging


logger = logging.getLogger(f"x35.{__name__}")


class KafkaBatchConsumer:
    def __init__(self, consumer_settings: KafkaConsumerSettings):
        self.settings = consumer_settings

        # Define security configuration in a separate dictionary
        security_config = {
            "security.protocol": getattr(
                self.settings, KafkaSettingsEnum.SECURITY_PROTOCOL
            ),
            "sasl.mechanisms": getattr(self.settings, KafkaSettingsEnum.SASL_MECHANISM),
            "sasl.username": getattr(self.settings, KafkaSettingsEnum.API_KEY),
            "sasl.password": getattr(self.settings, KafkaSettingsEnum.API_SECRET),
            "client.dns.lookup": getattr(
                self.settings, KafkaSettingsEnum.CLIENT_DNS_LOOKUP
            ),
        }
        consumer_conf = {
            "bootstrap.servers": getattr(
                self.settings, KafkaSettingsEnum.BOOTSTRAP_SERVERS
            ),
            "group.id": getattr(self.settings, KafkaSettingsEnum.GROUP_ID),
            "auto.offset.reset": getattr(
                self.settings, KafkaSettingsEnum.AUTO_OFFSET_RESET
            ),
            "enable.auto.commit": getattr(
                self.settings, KafkaSettingsEnum.ENABLE_AUTO_COMMIT
            ),
            **security_config,  # Unpack the security config dictionary here
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([getattr(self.settings, KafkaSettingsEnum.TOPIC)])

    def get_offset_range(self):
        """
        Get all offset info in one call (low, high, is_empty).

        Returns:
            dict: Dictionary with 'low' (int), 'high' (int), and 'is_empty' (bool) keys.
        """
        tp = TopicPartition(
            getattr(self.settings, KafkaSettingsEnum.TOPIC),
            getattr(self.settings, KafkaSettingsEnum.PARTITION),
        )
        low, high = self.consumer.get_watermark_offsets(tp)
        return {"low": low, "high": high, "is_empty": low >= high}

    def assign(self, offset):
        """
        Assign consumer to a specific topic partition and offset.

        Args:
            offset (int): The offset to assign the consumer to.
        """
        tp = TopicPartition(
            getattr(self.settings, KafkaSettingsEnum.TOPIC),
            getattr(self.settings, KafkaSettingsEnum.PARTITION),
            offset,
        )
        self.consumer.assign([tp])

    def consume_batch(self, batch_size):
        """
        Consume a batch of messages from the topic partition.

        Args:
            batch_size (int): The number of messages to consume in the batch.
        Returns:
            list: List of consumed messages.
        """
        return self.consumer.consume(
            num_messages=batch_size,
            timeout=getattr(self.settings, KafkaSettingsEnum.BATCH_TIMEOUT),
        )

    def commit(self):
        """
        Commit the current offset for the consumer.
        """
        self.consumer.commit()

    def close(self):
        """
        Cleanup consumer and close the connection.
        """
        self.consumer.close()

    def get_committed_offset(self):
        """Get the current committed offset for the consumer group."""
        tp = TopicPartition(
            getattr(self.settings, KafkaSettingsEnum.TOPIC),
            getattr(self.settings, KafkaSettingsEnum.PARTITION),
        )
        committed_tp = self.consumer.committed([tp])[0]
        return committed_tp.offset

    def commit_specific_offset(self, offset: int):
        tp = TopicPartition(
            getattr(self.settings, KafkaSettingsEnum.TOPIC),
            getattr(self.settings, KafkaSettingsEnum.PARTITION),
            offset,
        )
        committed = self.consumer.commit(offsets=[tp], asynchronous=False)
        return committed
