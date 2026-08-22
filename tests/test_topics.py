"""Behavioral tests for Kafka topic name derivation."""

from __future__ import annotations

from kafka import (
    DEFAULT_DLQ_TOPIC_SUFFIX,
    DEFAULT_RETRY_TOPIC_SUFFIX,
    dlq_topic_name,
    expand_topic,
    expand_topic_names,
    is_dlq_topic,
    is_retry_topic,
    original_topic_name,
    retry_topic_name,
)


def test_default_suffixes() -> None:
    assert DEFAULT_RETRY_TOPIC_SUFFIX == ".retry"
    assert DEFAULT_DLQ_TOPIC_SUFFIX == ".dlq"


def test_retry_topic_name_derivation() -> None:
    assert retry_topic_name("event.created") == "event.created.retry"
    assert retry_topic_name("event.created.retry") == "event.created.retry"


def test_dlq_topic_name_derivation() -> None:
    assert dlq_topic_name("event.created") == "event.created.dlq"
    assert dlq_topic_name("event.created.retry") == "event.created.dlq"
    assert dlq_topic_name("event.created.dlq") == "event.created.dlq"


def test_dlq_from_embedded_retry_segment() -> None:
    assert dlq_topic_name("admin.retry.event") == "admin.dlq.event"


def test_predicates() -> None:
    assert is_retry_topic("a.retry") is True
    assert is_retry_topic("a.retry.b") is True
    assert is_retry_topic("a.dlq") is False
    assert is_dlq_topic("a.dlq") is True
    assert is_dlq_topic("a.dlq.b") is True
    assert is_dlq_topic("a.retry") is False


def test_original_topic_name_strips_retry_suffix() -> None:
    assert original_topic_name("event.created.retry") == "event.created"
    assert original_topic_name("event.created") == "event.created"


def test_expand_topic_names_dedupes_and_passes_through_derived() -> None:
    assert expand_topic_names(["event.created", "user.updated"]) == [
        "event.created",
        "event.created.retry",
        "event.created.dlq",
        "user.updated",
        "user.updated.retry",
        "user.updated.dlq",
    ]


def test_expand_topic_names_can_disable_variants() -> None:
    assert expand_topic_names(["a"], include_retry=False, include_dlq=False) == ["a"]
    assert expand_topic_names(["a"], include_dlq=False) == ["a", "a.retry"]


def test_expand_topic_names_keeps_existing_retry_dlq_unchanged() -> None:
    assert expand_topic_names(["a.retry", "a.dlq"]) == ["a.retry", "a.dlq"]


def test_expand_topic_names_deduplicates() -> None:
    assert expand_topic_names(["a", "a.retry"]) == ["a", "a.retry", "a.dlq"]


def test_expand_topic_triple() -> None:
    expansion = expand_topic("ticket.created")
    assert expansion.original == "ticket.created"
    assert expansion.retry == "ticket.created.retry"
    assert expansion.dlq == "ticket.created.dlq"
