"""Kafka topic name derivation helpers.

Centralizes retry/DLQ topic naming so consumers, producers and tooling
always derive the same topic names.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_RETRY_TOPIC_SUFFIX = ".retry"
DEFAULT_DLQ_TOPIC_SUFFIX = ".dlq"


def is_retry_topic(topic: str, retry_suffix: str = DEFAULT_RETRY_TOPIC_SUFFIX) -> bool:
    return topic.endswith(retry_suffix) or ".retry." in topic


def is_dlq_topic(topic: str, dlq_suffix: str = DEFAULT_DLQ_TOPIC_SUFFIX) -> bool:
    return topic.endswith(dlq_suffix) or ".dlq." in topic


def retry_topic_name(topic: str, retry_suffix: str = DEFAULT_RETRY_TOPIC_SUFFIX) -> str:
    return topic if is_retry_topic(topic, retry_suffix) else f"{topic}{retry_suffix}"


def dlq_topic_name(
    topic: str,
    retry_suffix: str = DEFAULT_RETRY_TOPIC_SUFFIX,
    dlq_suffix: str = DEFAULT_DLQ_TOPIC_SUFFIX,
) -> str:
    if is_dlq_topic(topic, dlq_suffix):
        return topic
    if topic.endswith(retry_suffix):
        return f"{topic[: -len(retry_suffix)]}{dlq_suffix}"
    if ".retry." in topic:
        return topic.replace(".retry.", ".dlq.")
    return f"{topic}{dlq_suffix}"


def original_topic_name(topic: str, retry_suffix: str = DEFAULT_RETRY_TOPIC_SUFFIX) -> str:
    return topic.removesuffix(retry_suffix)


@dataclass(frozen=True, slots=True)
class TopicExpansion:
    original: str
    retry: str
    dlq: str


def expand_topic_names(
    topics: list[str],
    retry_suffix: str = DEFAULT_RETRY_TOPIC_SUFFIX,
    dlq_suffix: str = DEFAULT_DLQ_TOPIC_SUFFIX,
    *,
    include_retry: bool = True,
    include_dlq: bool = True,
) -> list[str]:
    """Expand base topics into their retry and DLQ variants (deduplicated).

    Existing retry/DLQ topics are passed through unchanged.
    """
    result: list[str] = []
    seen: set[str] = set()
    for topic in topics:
        candidates = [topic]
        if not is_retry_topic(topic, retry_suffix) and not is_dlq_topic(topic, dlq_suffix):
            if include_retry:
                candidates.append(retry_topic_name(topic, retry_suffix))
            if include_dlq:
                candidates.append(dlq_topic_name(topic, retry_suffix, dlq_suffix))
        for candidate in candidates:
            if candidate not in seen:
                seen.add(candidate)
                result.append(candidate)
    return result


def expand_topic(topic: str) -> TopicExpansion:
    """Derive the retry/DLQ variant triple for a single base topic."""
    return TopicExpansion(
        original=topic,
        retry=retry_topic_name(topic),
        dlq=dlq_topic_name(topic),
    )
