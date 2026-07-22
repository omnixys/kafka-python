# omnixys-kafka

Omnixys shared Kafka package with producer, consumer, retry, DLQ, idempotency, and circuit-breaker.

## Installation

```bash
pip install omnixys-kafka
```

## Features

- Async Kafka producer and consumer
- Built-in retry mechanisms
- Dead Letter Queue (DLQ) support
- Idempotency service
- Circuit breaker pattern
- OpenTelemetry instrumentation

## Usage

```python
from omnixys_kafka import AIOKafkaEventProducer, KafkaConsumer, JsonEventSerializer
```

## License

GPL-3.0-or-later
