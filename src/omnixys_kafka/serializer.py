from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID


class JsonEventSerializer:
    def serialize(self, payload: Any) -> bytes:
        return json.dumps(
            payload,
            default=self._default,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))

    @staticmethod
    def _default(obj: Any) -> str:
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        msg = f"Object of type {type(obj).__name__} is not JSON serializable"
        raise TypeError(msg)
