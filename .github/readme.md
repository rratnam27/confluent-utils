# Confluent Utils

The `confluent-utils` package provides ready-to-use Confluent Kafka Producer and Consumer services driven by a flexible configuration model. It simplifies Kafka integration by abstracting complex setup and runtime behavior.

Key features include:

- Support for **multiple Kafka clusters**
- Support for **multiple producer and consumer** with independent configurations
- **Blocking retry mechanism** for transient (retryable) failures with blocking the consumer thread
- **Non-blocking retry mechanism** for transient (retryable) failures without blocking the consumer thread
- Automatic **routing of failed messages**:
  - To a **Dead Letter Topic (DLT)** for retryable errors after exhausting retry attempts
  - To a dedicated **Error Topic** for non-retryable errors

This package aims to reduce boilerplate and streamline resilient message-driven application development.

## Usage

### 1. Install Dependencies

```bash
pip install confluent-utils
```

### 2. Start the Consumer (FastAPI example)

```python
from confluent_utils import KafkaConsumer
from src.settings import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka_consumer = KafkaConsumer(
        [settings.my_consumer, settings.my_retry_consumer, ...],
        other_settings_list=[settings.my_retry_producer, settings.my_error_producer, ...],
    )
    kafka_consumer.start_all()
    yield
    kafka_consumer.stop_all()
```
Consumer on_message function definition in handler file

```python
def on_message(metadata: dict, msg: dict) -> None:
```
### 3. Define Producer

```python
from confluent_utils import BaseKafkaProducer, KafkaProducerException
    try:
        producer = BaseKafkaProducer(producer_name)
        result = producer.send(key, message)
    except KafkaProducerException as e:
        logger.error(f"Failed to send message: {e}")
```

---

## Configuration via Pydantic Settings

All Kafka producer and consumer configuration is managed via Pydantic settings classes. Each producer or consumer should define its own settings class, inheriting from the provided base settings (e.g., `KafkaProducerSettings`, `KafkaConsumerSettings`).

Settings are typically loaded from environment variables using Pydantic’s `Field` for type safety and documentation.

**Example for a generic producer:**

```python
from pydantic import Field
from src.settings import KafkaProducerSettings

class MyProducerSettings(KafkaProducerSettings):
    """Generic Kafka producer settings."""
    model_config = KafkaProducerSettings.model_config
    model_config["env_prefix"] = "KAFKA_MY_PRODUCER_"

    producer_id: str = Field(default="my_producer", description="Unique Producer Id")
    topic: str = Field(default="my_topic", description="Topic to produce messages to")
    enable_idempotence: bool = Field(default=True, description="Enable Idempotence")
    delivery_timeout_ms: int = Field(default=120000, description="Delivery timeout ms", ge=0)
    max_in_flight_requests_per_connection: int = Field(default=5, description="Max in flight request per connection", ge=0)
    auto_register_schemas: bool = Field(default=False, description="Auto Register Schemas")
    use_latest_version: bool = Field(default=True, description="Use Latest Version")
    client_dns_lookup: str = Field(default="use_all_dns_ips", description="client_dns_lookup")
    value_serializer_type: str = Field(default="str", description="Value Serializer allowed str, avro")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol for Kafka connections.")
    schema_id: str = Field(default="", description="Schema Id")
    schema_registry_url: str = Field(default="", description="Schema Registry URL")
    schema_registry_username: str = Field(default="", description="Schema Registry Username")
    schema_registry_password: str = Field(default="", description="Schema Registry Password")
```

**Example for a generic consumer:**

```python
from pydantic import Field
from src.settings import KafkaConsumerSettings

class MyConsumerSettings(KafkaConsumerSettings):
    """Generic Kafka consumer settings."""
    model_config = KafkaConsumerSettings.model_config
    model_config["env_prefix"] = "KAFKA_MY_CONSUMER_"

    topic: str = Field(default="my_topic", description="Kafka topic to consume messages from.")
    client_dns_lookup: str = Field(default="use_all_dns_ips", description="client_dns_lookup")
    count: int = Field(default=1, description="Number of consumers in a group", gt=0)
    handler: str = Field(default="src.services.my_service.on_message", description="Consumer Handler")
    listener_id: str = Field(default="my_listener", description="Consumer Listener id")
    error_producer: str = Field(default="", description="Error Producer Name")
    dlt_producer: str = Field(default="", description="DLT Producer Name")
    retry_producer: str = Field(default="my_retry_producer", description="Retry Producer Name")
    max_retries: int = Field(default=3, description="Max Retries", gt=0)
    retry_delay_seconds: int = Field(default=60, description="Retry delay seconds", gt=0)
    value_serializer_type: str = Field(default="str", description="Value Serializer allowed str, avro")
    group_id: str = Field(default="my-group", description="Consumer Group Id")
    auto_offset_reset: str = Field(default="latest", description="Offset reset policy (earliest/latest).")
    enable_auto_commit: bool = Field(default=False, description="Enable automatic offset committing.")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol for Kafka connections.")
    sasl_mechanism: str = Field(default="", description="SASL mechanism for Kafka authentication.")
    sasl_username: str = Field(default="", description="SASL username for Kafka authentication.")
    sasl_password: str = Field(default="", description="SASL password for Kafka authentication.")
```
You can define similar settings classes for each producer or consumer, customizing the `env_prefix` and fields as needed. All values are loaded from environment variables.

---

## Environment Variables

Set the following environment variables to configure the service:

**Producer Environment Variables**

| Variable Name                                         | Description                                                  | Default Value                       |
|-------------------------------------------------------|--------------------------------------------------------------|-------------------------------------|
| `KAFKA_PRODUCER_BOOTSTRAP_SERVERS`                    | Kafka bootstrap servers                                      |                                     |
| `KAFKA_PRODUCER_API_KEY`                              | Kafka SASL API key (username)                                |                                     |
| `KAFKA_PRODUCER_API_SECRET`                           | Kafka SASL API secret (password)                             |                                     |
| `KAFKA_PRODUCER_PRODUCER_ID`                          | Unique identifier for this Kafka producer                    | `generic_producer`                  |
| `KAFKA_PRODUCER_TOPIC`                                | Kafka topic to produce messages to                           | `GENERIC.TOPIC`                     |
| `KAFKA_PRODUCER_ENABLE_IDEMPOTENCE`                   | Enable idempotent Kafka producer (True/False)                | `True`                              |
| `KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS`                  | Kafka delivery timeout in milliseconds                       | `120000`                            |
| `KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION`| Max in-flight Kafka requests per connection                  | `5`                                 |
| `KAFKA_PRODUCER_AUTO_REGISTER_SCHEMAS`                | Auto-register Avro schemas with schema registry (True/False) | `False`                             |
| `KAFKA_PRODUCER_USE_LATEST_VERSION`                   | Use latest schema version for Avro (True/False)              | `True`                              |
| `KAFKA_PRODUCER_CLIENT_DNS_LOOKUP`                    | Kafka client DNS lookup mode                                 | `use_all_dns_ips`                   |
| `KAFKA_PRODUCER_VALUE_SERIALIZER_TYPE`                | Kafka value serializer type (str/avro)                       | `str`                               |
| `KAFKA_PRODUCER_SECURITY_PROTOCOL`                    | Security protocol for Kafka connections                      | `PLAINTEXT`                         |
| `KAFKA_PRODUCER_SCHEMA_ID`                            | Schema Id for Kafka messages                                 |                                     |
| `KAFKA_PRODUCER_SCHEMA_REGISTRY_URL`                  | Schema Registry URL for Kafka                                |                                     |
| `KAFKA_PRODUCER_SCHEMA_REGISTRY_USERNAME`             | Schema Registry Username for Kafka                           |                                     |
| `KAFKA_PRODUCER_SCHEMA_REGISTRY_PASSWORD`             | Schema Registry Password for Kafka                           |                                     |
| `KAFKA_PRODUCER_ACKS`                                | Number of acknowledgments producer requires                  | `all`                               |
| `KAFKA_PRODUCER_RETRIES`                             | Number of retries before giving up                           | `5`                                 |
| `KAFKA_PRODUCER_RETRY_BACKOFF_MS`                    | Time to wait before retrying (ms)                            | `100`                               |
| `KAFKA_PRODUCER_COMPRESSION_TYPE`                    | Compression type for messages                                | `lz4`                               |
| `KAFKA_PRODUCER_BATCH_SIZE`                          | Maximum size of message batches in bytes                     | `16384`                             |
| `KAFKA_PRODUCER_LINGER_MS`                           | Time to wait for additional messages before sending batch (ms)| `5`                                |

**Consumer Environment Variables**

| Variable Name                          | Description                                                    | Default Value            |
|----------------------------------------|----------------------------------------------------------------|--------------------------|
| `KAFKA_CONSUMER_BOOTSTRAP_SERVERS`     | Kafka broker addresses                                         |                          |
| `KAFKA_CONSUMER_API_KEY`               | Kafka broker API key                                           |                          |
| `KAFKA_CONSUMER_API_SECRET`            | Kafka broker API secret                                        |                          |
| `KAFKA_CONSUMER_TOPIC`                 | Kafka topic to consume messages from                           | `GENERIC.TOPIC`          |
| `KAFKA_CONSUMER_GROUP_ID`              | Kafka consumer group ID                                        | `generic-consumer-group` |
| `KAFKA_CONSUMER_HANDLER`               | Kafka message handler class/function name                      | `default_handler`        |
| `KAFKA_CONSUMER_LISTENER_ID`           | Kafka consumer listener ID                                     | `generic_listener`       |
| `KAFKA_CONSUMER_COUNT`                 | Number of consumer instances to run                            | `6`                      |
| `KAFKA_CONSUMER_VALUE_SERIALIZER_TYPE` | Kafka value serializer type (str/avro)                         | `str`                    |
| `KAFKA_CONSUMER_AUTO_OFFSET_RESET`     | Kafka auto offset reset                                        | `latest`                 |
| `KAFKA_CONSUMER_ENABLE_AUTO_COMMIT`    | Kafka enable auto commit (True/False)                          | `False`                  |
| `KAFKA_CONSUMER_MAX_RETRIES`           | Maximum number of retry attempts for message processing        | `3`                      |
| `KAFKA_CONSUMER_RETRY_DELAY_SECONDS`   | Delay in seconds between retry attempts                        | `30`                     |
| `KAFKA_CONSUMER_ERROR_PRODUCER`        | Kafka producer topic for error messages                        |                          |
| `KAFKA_CONSUMER_DLT_PRODUCER`          | Kafka producer topic for dead-lettered messages                |                          |
| `KAFKA_CONSUMER_RETRY_PRODUCER`        | Kafka producer topic for retry messages                        |                          |
| `KAFKA_CONSUMER_CLIENT_DNS_LOOKUP`     | Kafka client DNS lookup                                        | `use_all_dns_ips`        |
| `KAFKA_CONSUMER_SESSION_TIMEOUT_MS`    | Kafka consumer session timeout (ms)                            | `60000`                  |
| `KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS`  | Maximum delay between polls before consumer is considered dead | `300000`                 |
| `KAFKA_CONSUMER_SECURITY_PROTOCOL`     | Security protocol for Kafka connections                        | `SASL_SSL`               |
| `KAFKA_CONSUMER_SASL_MECHANISM`        | SASL mechanism for authentication                              | `PLAIN`                  |
| `KAFKA_CONSUMER_SASL_USERNAME`         | SASL username for Kafka authentication                         |                          |
| `KAFKA_CONSUMER_SASL_PASSWORD`         | SASL password for Kafka authentication                         |                          |

---

## Listener Message Handling Logic

The listener supports advanced error handling and retry strategies:

- **Blocking Retry (`BlockingRetryException`)**:
  - If `retry_until_success=True`, the message is not committed and will be retried after a delay (retry_delay_seconds) until success.
  - If `retry_until_success=False`, the message is retried up to max_retries. If retries are exhausted, it is sent to the DLT (Dead Letter Topic) if DLT is configured and the message is committed.
- **Non-Blocking Retry (`NonBlockingRetryException`)**:
  - If `retry_until_success=True`, the message is committed and message will be sent to retry topic, will be retried after a delay until success.
  - If `retry_until_success=False`, the message is committed and message will be sent to retry topic, will be retried after a delay until message reaches the max_retries.
- **Non-Retryable Exception (`NonRetryableException`)**:
  - The message is sent directly to the error topic.
- **Generic Exception**:
  - The message is sent directly to the error topic.

The configuration for DLT (Dead Letter Topic), Error, and Retry topic producers must be defined within the respective services, depending on their usage. The corresponding producer IDs should be added to the consumer configuration under the properties dlt_producer, error_producer, and retry_producer. If these topics are used, their configurations must be provided during server startup.

When a Retry topic is defined, its name must end with "retry", and a separate consumer configuration must be specified for it. The Retry consumer's property handler should use the same value as the actual (primary) consumer configuration handler. This Retry consumer configuration must also be included during startup alongside the main consumer configuration.

### Handler Signature

Handlers must follow the signature:

```python
def on_message(metadata: dict, msg: dict) -> None
```

- Raise the appropriate exception (`BlockingRetryException`, `NonBlockingRetryException`, `NonRetryableException`) to control retry and error handling behavior.

---

## Notes

- The configuration supports multiple Kafka clusters and maps producers and listeners accordingly.
- Handlers must follow the signature: `def on_message(metadata: dict, msg: dict) -> None`
- Errors and DLT messages will be routed based on the config-defined producers.
