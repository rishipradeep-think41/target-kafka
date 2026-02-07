"""Pytest configuration and fixtures for target-kafka tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def mock_kafka_producer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock KafkaProducer so tests run without a real Kafka broker."""
    producer_mock = MagicMock()
    producer_mock.flush.return_value = None
    producer_mock.close.return_value = None

    import target_kafka.sinks as sinks_module

    monkeypatch.setattr(sinks_module, "KafkaProducer", lambda *args, **kwargs: producer_mock)
