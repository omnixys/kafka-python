"""Decorator-driven Kafka handler registration and dispatch."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

from kafka.model import KafkaEnvelope
from kafka.serializer import JsonEventSerializer

EventHandler = Callable[[KafkaEnvelope[Any], dict[str, str]], Awaitable[None]]


@dataclass(slots=True)
class KafkaEventContext:
    """Transport metadata delivered alongside a message to a handler.

    Mirrors the TypeScript `IKafkaEventContext`.
    """

    event_id: str
    event_version: str
    service: str
    topic: str
    partition: int = 0
    offset: str = "0"
    timestamp: str = ""
    event_type: str | None = None
    request_id: str | None = None
    correlation_id: str | None = None
    actor_id: str | None = None
    tenant_id: str | None = None
    trace_id: str | None = None
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_envelope(
        cls,
        topic: str,
        envelope: KafkaEnvelope[Any],
        *,
        partition: int = 0,
        offset: str = "0",
        headers: dict[str, str] | None = None,
    ) -> KafkaEventContext:
        return cls(
            event_id=envelope.event_id,
            event_version=envelope.event_version,
            event_type=envelope.event_type,
            service=envelope.service,
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=envelope.timestamp,
            request_id=_header(headers, "x-request-id"),
            correlation_id=_header(headers, "x-correlation-id"),
            actor_id=_header(headers, "x-meta-actorId"),
            tenant_id=_header(headers, "x-meta-tenantId"),
            trace_id=_header(headers, "x-meta-traceId"),
            headers=headers or {},
        )


def _header(headers: dict[str, str] | None, key: str) -> str | None:
    if not headers:
        return None
    value = headers.get(key)
    return value or None


class KafkaHandlerNotFoundError(KeyError):
    def __init__(self, topic: str) -> None:
        self.topic = topic
        super().__init__(f"No handler registered for Kafka topic '{topic}'")


class KafkaHandlerRegistry:
    """Maps topics to their async handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, EventHandler] = {}

    def register(self, topic: str, handler: EventHandler) -> None:
        if topic in self._handlers:
            raise ValueError(f"Duplicate Kafka handler for topic '{topic}'")
        self._handlers[topic] = handler

    def has(self, topic: str) -> bool:
        return topic in self._handlers

    def topics(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))

    def handlers(self) -> dict[str, EventHandler]:
        return dict(self._handlers)

    def __len__(self) -> int:
        return len(self._handlers)

    def __iter__(self) -> Iterator[tuple[str, EventHandler]]:
        return iter(self._handlers.items())


_default_registry = KafkaHandlerRegistry()


def get_default_registry() -> KafkaHandlerRegistry:
    return _default_registry


def kafka_handler(topic: str) -> Callable[[EventHandler], EventHandler]:
    """Decorator that registers an async handler for a Kafka topic.

    Example:
        @kafka_handler("event.created")
        async def handle_event_created(envelope, headers):
            ...
    """

    def decorator(handler: EventHandler) -> EventHandler:
        _default_registry.register(topic, handler)
        return handler

    return decorator


class KafkaEventDispatcher:
    """Dispatches a deserialized envelope to the handler registered for its topic.

    Useful for tests, in-process reprocessing and non-consumer entry points.
    """

    def __init__(
        self,
        registry: KafkaHandlerRegistry | None = None,
        serializer: JsonEventSerializer | None = None,
    ) -> None:
        self._registry = registry or _default_registry
        self._serializer = serializer or JsonEventSerializer()

    def has_handler(self, topic: str) -> bool:
        return self._registry.has(topic)

    def registered_topics(self) -> tuple[str, ...]:
        return self._registry.topics()

    def diagnostics(self) -> dict[str, Any]:
        return {"handlerCount": len(self._registry), "topics": self.registered_topics()}

    async def dispatch(
        self,
        topic: str,
        envelope: KafkaEnvelope[Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        handler = self._registry.handlers().get(topic)
        if handler is None:
            raise KafkaHandlerNotFoundError(topic)
        await handler(envelope, headers or {})

    async def dispatch_raw(
        self,
        topic: str,
        value: bytes,
        headers: list[tuple[str, bytes]] | None = None,
        *,
        partition: int = 0,
        offset: str = "0",
    ) -> None:
        raw_envelope = self._serializer.deserialize(value)
        envelope = KafkaEnvelope.from_dict(raw_envelope)
        string_headers = _parse_headers(headers)
        context = KafkaEventContext.from_envelope(
            topic,
            envelope,
            partition=partition,
            offset=offset,
            headers=string_headers,
        )
        await self.dispatch(topic, envelope, context.headers)


def _parse_headers(raw: list[tuple[str, bytes]] | None) -> dict[str, str]:
    if not raw:
        return {}
    result: dict[str, str] = {}
    for key, value in raw:
        if key not in result:
            result[key] = value.decode("utf-8") if value else ""
    return result
