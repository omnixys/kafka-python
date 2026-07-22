"""Smoke test - verifies omnixys-kafka can be imported."""

from __future__ import annotations

import importlib
from importlib.metadata import version as pkg_version


def test_package_importable() -> None:
    mod = importlib.import_module("kafka")
    assert hasattr(mod, "__version__")
    assert mod.__version__ == pkg_version("omnixys-kafka")


def test_public_api() -> None:
    from kafka import consumer, producer, serializer

    assert consumer is not None
    assert producer is not None
    assert serializer is not None
