"""Behavioral tests for the Kafka handler registry, decorator and dispatcher."""

from __future__ import annotations

import pytest

from kafka import (
    JsonEventSerializer,
    KafkaEnvelope,
    KafkaEventContext,
    KafkaEventDispatcher,
    KafkaHandlerNotFoundError,
    KafkaHandlerRegistry,
    kafka_handler,
)
from kafka.handlers import get_default_registry


def _envelope(topic: str = "greeting.sent") -> KafkaEnvelope[dict]:
    return KafkaEnvelope.create(event_name=topic, service="tests", payload={"name": "ada"})


async def test_registry_register_has_len() -> None:
    registry = KafkaHandlerRegistry()
    assert len(registry) == 0
    assert registry.has("a") is False

    async def handle(envelope, headers) -> None: ...

    registry.register("a", handle)
    assert len(registry) == 1
    assert registry.has("a") is True
    assert registry.topics() == ("a",)


async def test_registry_rejects_duplicates() -> None:
    registry = KafkaHandlerRegistry()

    async def handle(envelope, headers) -> None: ...

    registry.register("a", handle)
    with pytest.raises(ValueError):
        registry.register("a", handle)


async def test_dispatch_calls_handler_with_envelope_and_headers() -> None:
    calls: list[tuple[KafkaEnvelope[dict], dict[str, str]]] = []

    async def handle(envelope, headers) -> None:
        calls.append((envelope, headers))

    registry = KafkaHandlerRegistry()
    registry.register("greeting.sent", handle)
    dispatcher = KafkaEventDispatcher(registry)

    envelope = _envelope()
    await dispatcher.dispatch("greeting.sent", envelope, {"x-request-id": "r1"})

    assert len(calls) == 1
    assert calls[0][0].event_id == envelope.event_id
    assert calls[0][1] == {"x-request-id": "r1"}


async def test_dispatch_unknown_topic_raises() -> None:
    dispatcher = KafkaEventDispatcher(KafkaHandlerRegistry())
    with pytest.raises(KafkaHandlerNotFoundError) as exc_info:
        await dispatcher.dispatch("missing", _envelope())
    assert exc_info.value.topic == "missing"


async def test_dispatch_raw_decodes_and_routes() -> None:
    seen: dict[str, str] = {}

    async def handle(envelope, headers) -> None:
        seen["name"] = envelope.payload["name"]
        seen["requestId"] = headers["x-request-id"]

    registry = KafkaHandlerRegistry()
    registry.register("greeting.sent", handle)
    dispatcher = KafkaEventDispatcher(registry)

    serializer = JsonEventSerializer()
    value = serializer.serialize(_envelope().to_dict())
    await dispatcher.dispatch_raw(
        "greeting.sent",
        value,
        [("x-request-id", b"r9")],
        partition=2,
        offset="41",
    )

    assert seen == {"name": "ada", "requestId": "r9"}


def test_context_from_envelope_maps_headers_and_metadata() -> None:
    envelope = _envelope()
    context = KafkaEventContext.from_envelope(
        "greeting.sent",
        envelope,
        partition=3,
        offset="12",
        headers={"x-request-id": "req", "x-meta-actorId": "actor-1"},
    )
    assert context.topic == "greeting.sent"
    assert context.event_id == envelope.event_id
    assert context.event_version == envelope.event_version
    assert context.partition == 3
    assert context.offset == "12"
    assert context.request_id == "req"
    assert context.actor_id == "actor-1"
    assert context.trace_id is None


def test_context_omits_blank_headers() -> None:
    context = KafkaEventContext.from_envelope("t", _envelope(), headers={"x-request-id": ""})
    assert context.request_id is None


async def test_dispatcher_diagnostics() -> None:
    registry = KafkaHandlerRegistry()

    async def handle(envelope, headers) -> None: ...

    registry.register("a", handle)
    diagnostics = KafkaEventDispatcher(registry).diagnostics()
    assert diagnostics["handlerCount"] == 1
    assert diagnostics["topics"] == ("a",)


async def test_kafka_handler_decorator_registers_into_default_registry() -> None:
    called: list[str] = []

    @kafka_handler("decorated.topic")
    async def handle(envelope, headers) -> None:
        called.append(envelope.event_name)

    registry = get_default_registry()
    assert registry.has("decorated.topic") is True

    dispatcher = KafkaEventDispatcher(registry)
    await dispatcher.dispatch("decorated.topic", _envelope("decorated.topic"))
    assert called == ["decorated.topic"]
