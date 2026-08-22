# omnixys-kafka

Omnixys shared async Kafka package — envelope producer, consumer with retry/DLQ/idempotency/circuit-breaker, decorator-driven handler dispatch, and header/topic utilities.

## Installation

```bash
pip install omnixys-kafka
```

## Features

- **Envelope producer**: `AIOKafkaEventProducer` builds versioned `KafkaEnvelope`s and injects OpenTelemetry trace headers
- **Consumer**: reliable `KafkaConsumer` with manual offset commit, partition pausing for delayed retries, per-topic circuit breakers and optional idempotency
- **Retry/DLQ**: exponential backoff onto `<topic>.retry`, dead-lettering onto `<topic>.dlq` after `max_retries`
- **Handler decorators**: `@kafka_handler(topic)`, `KafkaHandlerRegistry`, `KafkaEventDispatcher` for in-process dispatch
- **Headers**: `create_kafka_headers()`/`KafkaHeaderCarrier` builder and `parse_headers()` for the aiokafka wire format
- **Topics**: `retry_topic_name`, `dlq_topic_name`, `original_topic_name`, `expand_topic_names` — one source of truth for derived topic names
- **DI**: `KafkaProvider` container integration (dishka)
- **Serialization**: compact JSON with UUID/`datetime` support

## Usage

### Handler registration (decorator + dispatcher)

```python
from kafka import KafkaEventDispatcher, KafkaEnvelope, kafka_handler

@kafka_handler("event.created")
async def on_event_created(envelope: KafkaEnvelope, headers: dict[str, str]) -> None:
    print(envelope.event_id, envelope.payload, headers.get("x-request-id"))

dispatcher = KafkaEventDispatcher()
await dispatcher.dispatch("event.created", envelope, {"x-request-id": "r1"})
# or from raw bytes:
await dispatcher.dispatch_raw("event.created", value, [("x-retry-count", b"0")])
```

### Producing

```python
from aiokafka import AIOKafkaProducer
from kafka import AIOKafkaEventProducer

producer = AIOKafkaEventProducer(
    AIOKafkaProducer(bootstrap_servers="localhost:9092", enable_idempotence=True)
)
await producer.publish(event_name="event.created", payload={...}, topic="event.created", service="ticketing")
await producer.publish_raw("event.created.dlq", b"...", key="user-42")
```

### Consuming with retry and DLQ

```python
from kafka import KafkaConsumer, RetryConfig

consumer = KafkaConsumer(
    consumer=raw_consumer,                 # aiokafka.AIOKafkaConsumer
    bootstrap_servers="localhost:9092",
    retry_config=RetryConfig(max_retries=5),
)
consumer.register_handler("event.created", on_event_created)
await consumer.start()
```

Failures are retried with exponential backoff into `event.created.retry`, and dead-lettered into `event.created.dlq` once `max_retries` is exhausted. Header metadata (`x-original-topic`, `x-retry-count`, `x-retry-at`) routes retries back to the right handler.

### Headers and topics

```python
from kafka import create_kafka_headers, expand_topic_names, retry_topic_name

headers = create_kafka_headers()
headers.set("x-meta-service", "user")

retry_topic_name("event.created")                 # "event.created.retry"
expand_topic_names(["event.created"])             # original + retry + dlq variants
```

## Testing

The package ships behavior-level tests (pytest, no broker needed):

```bash
uv run pytest -q          # 29 tests
uv run ruff check .       # lint
uv run mypy src/          # strict typing
```

## License

GPL-3.0-or-later
