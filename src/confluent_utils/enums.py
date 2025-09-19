"""
This module defines enums used throughout the confluent_utils package.

Enums:
    DeliveryResultEnum: Enum for delivery result keys and values.
    KafkaSettingsEnum: Enum for Kafka configuration setting keys.
"""

from enum import Enum


class DeliveryResultEnum(str, Enum):
    KEY_STATUS = "status"
    KEY_TOPIC = "topic"
    KEY_PARTITION = "partition"
    KEY_OFFSET = "offset"

    VALUE_SUCCESS = "success"


class KafkaSettingsEnum(str, Enum):
    SECURITY_PROTOCOL = "security_protocol"
    SASL_MECHANISM = "sasl_mechanism"
    API_KEY = "api_key"
    API_SECRET = "api_secret"
    CLIENT_DNS_LOOKUP = "client_dns_lookup"
    BOOTSTRAP_SERVERS = "bootstrap_servers"
    GROUP_ID = "group_id"
    AUTO_OFFSET_RESET = "auto_offset_reset"
    ENABLE_AUTO_COMMIT = "enable_auto_commit"
    PARTITION = "partition"
    TOPIC = "topic"
    BATCH_TIMEOUT = "batch_timeout"
    SESSION_TIMEOUT_MS = "session_timeout_ms"
    MAX_POLL_INTERVAL_MS = "max_poll_interval_ms"
    POLL_TIMEOUT = "poll_timeout"
