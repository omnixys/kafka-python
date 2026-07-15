from __future__ import annotations

from typing import Any, Protocol

from aiokafka import AIOKafkaProducer as _AIOKafkaProducer
from opentelemetry import trace
from opentelemetry.propagate import inject

from omnixys_kafka.model import KafkaEnvelope
from omnixys_kafka.serializer import JsonEventSerializer


class KafkaProducer(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def publish(
        self,
        event_name: str,
        payload: Any,
        topic: str,
        service: str,
        key: str | None = None,
        event_version: str = "1",
        event_type: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None: ...
    async def publish_raw(
        self,
        topic: str,
        value: bytes,
        key: str | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None: ...


class AIOKafkaEventProducer:
    def __init__(
        self,
        producer: _AIOKafkaProducer,
        serializer: JsonEventSerializer | None = None,
    ) -> None:
        self._producer = producer
        self._serializer = serializer or JsonEventSerializer()

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(
        self,
        event_name: str,
        payload: Any,
        topic: str,
        service: str,
        key: str | None = None,
        event_version: str = "1",
        event_type: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        envelope = KafkaEnvelope.create(
            event_name=event_name,
            service=service,
            payload=payload,
            event_version=event_version,
            event_type=event_type,
        )
        value = self._serializer.serialize(envelope.to_dict())
        kafka_headers = self._build_headers(headers or {})
        await self._producer.send_and_wait(
            topic=topic,
            value=value,
            key=key.encode("utf-8") if key else None,
            headers=kafka_headers,
        )

    async def publish_raw(
        self,
        topic: str,
        value: bytes,
        key: str | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        await self._producer.send_and_wait(
            topic=topic,
            value=value,
            key=key.encode("utf-8") if key else None,
            headers=headers or [],
        )

    @staticmethod
    def _build_headers(custom: dict[str, str]) -> list[tuple[str, bytes]]:
        headers: list[tuple[str, bytes]] = [(k, v.encode("utf-8")) for k, v in custom.items()]
        trace_headers: dict[str, str] = {}
        inject(trace_headers)
        for k, v in trace_headers.items():
            headers.append((k, v.encode("utf-8")))
        current_span = trace.get_current_span()
        span_context = current_span.get_span_context()
        if span_context.is_valid:
            headers.append(("x-meta-traceId", format(span_context.trace_id, "032x").encode("utf-8")))
            headers.append(("x-meta-spanId", format(span_context.span_id, "016x").encode("utf-8")))
            headers.append(("x-meta-sampled", str(span_context.trace_flags).encode("utf-8")))
        return headers
