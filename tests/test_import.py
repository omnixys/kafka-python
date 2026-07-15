"""Smoke test - verifies omnixys-kafka can be imported."""

from __future__ import annotations

import importlib



def test_package_importable() -> None:
    mod = importlib.import_module("omnixys_kafka")
    assert hasattr(mod, "__version__")
    assert mod.__version__ == "1.0.0"


def test_public_api() -> None:
    from omnixys_kafka import consumer, producer, serializer

    assert consumer is not None
    assert producer is not None
    assert serializer is not None
