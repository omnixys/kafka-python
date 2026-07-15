from __future__ import annotations

from aiokafka import AIOKafkaConsumer as _AIOKafkaConsumer
from aiokafka import AIOKafkaProducer as _AIOKafkaProducer
from dishka import Provider, Scope, provide

from omnixys_kafka.consumer import CircuitBreakerConfig, IdempotencyService, KafkaConsumer, RetryConfig
from omnixys_kafka.producer import AIOKafkaEventProducer
from omnixys_kafka.serializer import JsonEventSerializer


class KafkaProvider(Provider):
    scope = Scope.APP

    @provide
    def json_serializer(self) -> JsonEventSerializer:
        return JsonEventSerializer()

    @provide
    def retry_config(self) -> RetryConfig:
        return RetryConfig()

    @provide
    def circuit_breaker_config(self) -> CircuitBreakerConfig:
        return CircuitBreakerConfig()

    @provide
    def idempotency_service(self) -> IdempotencyService:
        return IdempotencyService()

    @provide
    def producer(
        self,
        bootstrap_servers: str,
        serializer: JsonEventSerializer,
    ) -> AIOKafkaEventProducer:
        raw_producer = _AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            enable_idempotence=True,
            acks="all",
            max_in_flight_requests_per_connection=1,
        )
        return AIOKafkaEventProducer(producer=raw_producer, serializer=serializer)

    @provide
    def consumer(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        serializer: JsonEventSerializer,
        retry_config: RetryConfig,
        circuit_breaker_config: CircuitBreakerConfig,
        idempotency: IdempotencyService | None = None,
    ) -> KafkaConsumer:
        retry_topics = [f"{t}{retry_config.retry_topic_suffix}" for t in topics]
        all_topics = topics + retry_topics
        raw_consumer = _AIOKafkaConsumer(
            *all_topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        return KafkaConsumer(
            consumer=raw_consumer,
            bootstrap_servers=bootstrap_servers,
            serializer=serializer,
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config,
            idempotency=idempotency,
        )
