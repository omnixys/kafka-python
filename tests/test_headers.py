"""Behavioral tests for the Kafka header carrier and builder."""

from __future__ import annotations

from kafka import create_kafka_headers, parse_headers


def test_create_kafka_headers_starts_empty() -> None:
    carrier = create_kafka_headers()
    assert len(carrier) == 0
    assert carrier.to_kafka_headers() == []


def test_carrier_set_get_roundtrip() -> None:
    carrier = create_kafka_headers()
    carrier.set("x-meta-service", "user")
    carrier.set("x-request-id", "req-1")
    assert carrier.get("x-meta-service") == "user"
    assert carrier.get("x-request-id") == "req-1"
    assert carrier.get("missing") is None


def test_carrier_keys_and_dict() -> None:
    carrier = create_kafka_headers()
    carrier.set("a", "1")
    carrier.set("b", "2")
    assert sorted(carrier.keys()) == ["a", "b"]
    assert carrier.to_dict() == {"a": "1", "b": "2"}


def test_carrier_to_kafka_headers_wire_format() -> None:
    carrier = create_kafka_headers()
    carrier.set("traceparent", "00-abc-def-01")
    assert carrier.to_kafka_headers() == [("traceparent", b"00-abc-def-01")]


def test_carrier_accepts_initial_bytes() -> None:
    from kafka import KafkaHeaderCarrier

    carrier = KafkaHeaderCarrier({"x-retry-count": b"2"})
    assert carrier.get("x-retry-count") == "2"


def test_parse_headers_handles_none_and_bytes() -> None:
    assert parse_headers(None) == {}
    assert parse_headers([]) == {}
    raw = [("x-error", b"boom"), ("x-retry-count", b"2")]
    assert parse_headers(raw) == {"x-error": "boom", "x-retry-count": "2"}


def test_parse_headers_keeps_first_occurrence() -> None:
    raw = [("k", b"first"), ("k", b"second")]
    assert parse_headers(raw) == {"k": "first"}
