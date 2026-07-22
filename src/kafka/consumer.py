from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from time import time
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer as _AIOKafkaConsumer
from aiokafka import AIOKafkaProducer as _AIOKafkaProducer

if TYPE_CHECKING:
    from aiokafka.structs import TopicPartition

from kafka.model import KafkaEnvelope
from kafka.serializer import JsonEventSerializer

logger = logging.getLogger(__name__)

MessageHandler = Callable[[KafkaEnvelope[Any], dict[str, str]], Awaitable[None]]

NO_RETRY_HEADERS = frozenset({"x-retry-count", "x-original-topic", "x-retry-at", "x-error"})
NO_DLQ_HEADERS = frozenset({"x-original-topic", "x-error", "x-failed-at", "x-retry-count", "x-retry-at"})


class CircuitBreakerState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class RetryConfig:
    max_retries: int = 3
    initial_delay_ms: int = 500
    multiplier: float = 2.0
    max_delay_ms: int = 30000
    retry_topic_suffix: str = ".retry"
    dlq_topic_suffix: str = ".dlq"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    reset_timeout_seconds: float = 10.0


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        self._config = config or CircuitBreakerConfig()
        self._state: CircuitBreakerState = CircuitBreakerState.CLOSED
        self._failure_count: int = 0
        self._last_failure_time: float = 0.0

    @property
    def state(self) -> CircuitBreakerState:
        if (
            self._state == CircuitBreakerState.OPEN
            and time() - self._last_failure_time >= self._config.reset_timeout_seconds
        ):
            self._state = CircuitBreakerState.HALF_OPEN
        return self._state

    def record_success(self) -> None:
        self._failure_count = 0
        if self._state in (CircuitBreakerState.HALF_OPEN, CircuitBreakerState.OPEN):
            self._state = CircuitBreakerState.CLOSED

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time()
        if self._failure_count >= self._config.failure_threshold:
            self._state = CircuitBreakerState.OPEN


class IdempotencyService:
    def __init__(self, redis_client: Any | None = None, ttl_seconds: int = 86400) -> None:
        self._redis = redis_client
        self._ttl = ttl_seconds
        self._memory: dict[str, float] = {}

    async def is_processed(self, event_id: str) -> bool:
        if self._redis is not None:
            result = await self._redis.get(self._key(event_id))
            return result is not None
        return event_id in self._memory

    async def mark_processed(self, event_id: str) -> None:
        if self._redis is not None:
            await self._redis.setex(self._key(event_id), self._ttl, "1")
        else:
            self._memory[event_id] = time()

    @staticmethod
    def _key(event_id: str) -> str:
        return f"kafka:idempotency:{event_id}"


class KafkaConsumer:
    def __init__(
        self,
        consumer: _AIOKafkaConsumer,
        bootstrap_servers: str,
        serializer: JsonEventSerializer | None = None,
        retry_config: RetryConfig | None = None,
        circuit_breaker_config: CircuitBreakerConfig | None = None,
        idempotency: IdempotencyService | None = None,
    ) -> None:
        self._consumer = consumer
        self._bootstrap_servers = bootstrap_servers
        self._serializer = serializer or JsonEventSerializer()
        self._retry_config = retry_config or RetryConfig()
        self._cb_config = circuit_breaker_config
        self._idempotency = idempotency
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._handlers: dict[str, MessageHandler] = {}
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        self._paused_partitions: dict[TopicPartition, asyncio.Task[None]] = {}

    def register_handler(self, topic: str, handler: MessageHandler) -> None:
        self._handlers[topic] = handler

    async def start(self) -> None:
        self._running = True
        await self._consumer.start()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._consumer.stop()

    async def _run(self) -> None:
        while self._running:
            try:
                batch = await self._consumer.getmany(timeout_ms=1000)
            except Exception as exc:
                logger.warning("Kafka poll failed", exc_info=exc)
                continue

            for tp, messages in batch.items():
                last_ok_offset: int | None = None
                for msg in messages:
                    ok = await self._process_message(tp, msg)
                    if ok:
                        last_ok_offset = msg.offset
                if last_ok_offset is not None:
                    await self._consumer.commit({tp: last_ok_offset + 1})

    async def _process_message(self, tp: TopicPartition, msg: Any) -> bool:
        headers = self._parse_headers(msg.headers)
        raw_envelope = self._serializer.deserialize(msg.value)
        envelope = KafkaEnvelope.from_dict(raw_envelope)

        topic = msg.topic
        original_topic = headers.get("x-original-topic", topic)

        retry_count = int(headers.get("x-retry-count", "0"))
        retry_at_str = headers.get("x-retry-at")

        if retry_at_str:
            retry_at = self._parse_retry_at(retry_at_str, tp)
            if retry_at is False:
                return False

        if self._idempotency is not None and await self._idempotency.is_processed(envelope.event_id):
            return True

        cb = self._get_circuit_breaker(original_topic)
        if cb.state == CircuitBreakerState.OPEN:
            await self._publish_dlq(original_topic, msg.value, headers, "Circuit breaker open")
            return True

        handler = self._handlers.get(original_topic)
        if handler is None:
            await self._publish_dlq(
                original_topic,
                msg.value,
                headers,
                f"No handler for topic: {original_topic}",
            )
            return True

        try:
            await handler(envelope, headers)
        except Exception as exc:
            cb.record_failure()
            if retry_count < self._retry_config.max_retries:
                await self._publish_retry(
                    original_topic,
                    msg.value,
                    headers,
                    retry_count + 1,
                    str(exc),
                )
            else:
                await self._publish_dlq(original_topic, msg.value, headers, str(exc))
            return False

        cb.record_success()
        if self._idempotency is not None:
            await self._idempotency.mark_processed(envelope.event_id)
        return True

    def _parse_retry_at(self, retry_at_str: str, tp: TopicPartition) -> bool | None:
        try:
            retry_at = datetime.fromisoformat(retry_at_str)
        except ValueError:
            return None
        if retry_at <= datetime.now(UTC):
            return None
        delay = (retry_at - datetime.now(UTC)).total_seconds()
        if tp not in self._paused_partitions:
            self._consumer.pause(tp)
            self._paused_partitions[tp] = asyncio.create_task(
                self._resume_after(tp, delay),
            )
        return False

    def _get_circuit_breaker(self, topic: str) -> CircuitBreaker:
        if topic not in self._circuit_breakers:
            self._circuit_breakers[topic] = CircuitBreaker(self._cb_config)
        return self._circuit_breakers[topic]

    async def _publish_retry(
        self,
        original_topic: str,
        value: bytes,
        headers: dict[str, str],
        retry_count: int,
        error: str,
    ) -> None:
        retry_topic = f"{original_topic}{self._retry_config.retry_topic_suffix}"
        delay_ms = min(
            self._retry_config.initial_delay_ms * (self._retry_config.multiplier ** (retry_count - 1)),
            self._retry_config.max_delay_ms,
        )
        retry_ts = datetime.now(UTC).timestamp() + delay_ms / 1000
        retry_at = datetime.fromtimestamp(retry_ts, tz=UTC)
        retry_headers = [
            ("x-retry-count", str(retry_count).encode("utf-8")),
            ("x-original-topic", original_topic.encode("utf-8")),
            ("x-retry-at", retry_at.isoformat().encode("utf-8")),
            ("x-error", error.encode("utf-8")),
        ]
        for k, v in headers.items():
            if k not in NO_RETRY_HEADERS:
                retry_headers.append((k, v.encode("utf-8")))

        producer = _AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        await producer.start()
        try:
            await producer.send_and_wait(topic=retry_topic, value=value, headers=retry_headers)
        finally:
            await producer.stop()

    async def _publish_dlq(
        self,
        original_topic: str,
        value: bytes,
        headers: dict[str, str],
        error: str,
    ) -> None:
        dlq_topic = f"{original_topic}{self._retry_config.dlq_topic_suffix}"
        dlq_headers = [
            ("x-original-topic", original_topic.encode("utf-8")),
            ("x-error", error.encode("utf-8")),
            ("x-failed-at", datetime.now(UTC).isoformat().encode("utf-8")),
        ]
        for k, v in headers.items():
            if k not in NO_DLQ_HEADERS:
                dlq_headers.append((k, v.encode("utf-8")))

        producer = _AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        await producer.start()
        try:
            await producer.send_and_wait(topic=dlq_topic, value=value, headers=dlq_headers)
        finally:
            await producer.stop()

    async def _resume_after(self, tp: TopicPartition, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            self._consumer.resume(tp)
        except asyncio.CancelledError:
            pass
        finally:
            self._paused_partitions.pop(tp, None)

    @staticmethod
    def _parse_headers(raw: list[tuple[str, bytes]] | None) -> dict[str, str]:
        if not raw:
            return {}
        result: dict[str, str] = {}
        for k, v in raw:
            if k not in result:
                result[k] = v.decode("utf-8") if v else ""
        return result
