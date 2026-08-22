"""Kafka header constants, carrier and builder."""

from __future__ import annotations

from typing import Protocol

KAFKA_HEADERS = {
    "SERVICE": "x-meta-service",
    "VERSION": "x-meta-version",
    "CLASS": "x-meta-class",
    "OPERATION": "x-meta-operation",
    "TYPE": "x-meta-type",
    "ACTOR_ID": "x-meta-actorId",
    "TENANT_ID": "x-meta-tenantId",
    "USER_ID": "x-meta-userId",
    "REQUEST_ID": "x-request-id",
    "CORRELATION_ID": "x-correlation-id",
    "TRACE_ID": "x-meta-traceId",
    "SPAN_ID": "x-meta-spanId",
    "PARENT_SPAN_ID": "x-meta-parentSpanId",
    "SAMPLED": "x-meta-sampled",
}

KAFKA_RETRY_HEADERS = {
    "COUNT": "x-retry-count",
    "ORIGINAL_TOPIC": "x-original-topic",
    "RETRY_AT": "x-retry-at",
    "ERROR": "x-error",
}

W3C_TRACE_HEADERS = {
    "TRACE_PARENT": "traceparent",
    "TRACE_STATE": "tracestate",
}

RETRY_HEADERS = frozenset(KAFKA_RETRY_HEADERS.values())
DLQ_HEADERS = frozenset(
    {
        *KAFKA_RETRY_HEADERS.values(),
        "x-failed-at",
    },
)


class HeaderCarrier(Protocol):
    """String-based key/value header access, compatible with OpenTelemetry."""

    def get(self, key: str) -> str | None: ...
    def set(self, key: str, value: str) -> None: ...
    def keys(self) -> list[str]: ...


class KafkaHeaderCarrier:
    """Builds and carries Kafka message headers.

    Mirrors the TypeScript `KafkaCarrier`: values are kept as bytes but
    exposed as strings, and can be materialized to the wire format that
    `aiokafka` expects (`list[tuple[str, bytes]]`).
    """

    def __init__(self, headers: dict[str, str | bytes] | None = None) -> None:
        self._headers: dict[str, bytes] = {
            key: (value.encode("utf-8") if isinstance(value, str) else value)
            for key, value in (headers or {}).items()
        }

    def get(self, key: str) -> str | None:
        value = self._headers.get(key)
        return value.decode("utf-8") if value is not None else None

    def set(self, key: str, value: str) -> None:
        self._headers[key] = value.encode("utf-8")

    def keys(self) -> list[str]:
        return list(self._headers)

    def to_kafka_headers(self) -> list[tuple[str, bytes]]:
        return list(self._headers.items())

    def to_dict(self) -> dict[str, str]:
        return {key: value.decode("utf-8") for key, value in self._headers.items()}

    def __len__(self) -> int:
        return len(self._headers)


def create_kafka_headers() -> KafkaHeaderCarrier:
    """Build an empty header carrier for a new Kafka message."""
    return KafkaHeaderCarrier()


def parse_headers(raw: list[tuple[str, bytes]] | None) -> dict[str, str]:
    """Convert aiokafka wire headers (list of (key, bytes)) to a string dict."""
    if not raw:
        return {}
    result: dict[str, str] = {}
    for key, value in raw:
        if key not in result:
            result[key] = value.decode("utf-8") if value else ""
    return result
