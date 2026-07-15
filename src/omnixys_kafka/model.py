from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4


class EventType(StrEnum):
    LOG = "LOG"
    EVENT = "EVENT"
    METRIC = "METRIC"
    ALERT = "ALERT"
    COMMAND = "COMMAND"


@dataclass
class KafkaEnvelope[T]:
    event_id: str
    event_name: str
    event_version: str
    service: str
    payload: T
    event_type: str = field(default_factory=lambda: EventType.EVENT)
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    @classmethod
    def create(
        cls,
        event_name: str,
        service: str,
        payload: T,
        event_version: str = "1",
        event_type: str | None = None,
    ) -> KafkaEnvelope[T]:
        return cls(
            event_id=str(uuid4()),
            event_name=event_name,
            event_version=event_version,
            service=service,
            payload=payload,
            event_type=event_type or EventType.EVENT,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "eventId": self.event_id,
            "eventName": self.event_name,
            "eventType": self.event_type,
            "eventVersion": self.event_version,
            "service": self.service,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> KafkaEnvelope[Any]:
        return cls(
            event_id=data["eventId"],
            event_name=data["eventName"],
            event_type=data.get("eventType", EventType.EVENT),
            event_version=data["eventVersion"],
            service=data["service"],
            timestamp=data["timestamp"],
            payload=data["payload"],
        )
