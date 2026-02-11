"""Tests for JSON serialization via Pydantic (record → Kafka message)."""

from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import UUID

import pytest

from target_kafka.sinks import _to_json_bytes


class _ExampleEnum(Enum):
    FOO = "foo"
    BAR = 2


def test_datetime() -> None:
    record = {"t": datetime(2024, 1, 15, 10, 30, 0)}
    raw = _to_json_bytes(record)
    assert json.loads(raw.decode()) == {"t": "2024-01-15T10:30:00"}


def test_date() -> None:
    record = {"d": date(2024, 1, 15)}
    raw = _to_json_bytes(record)
    assert json.loads(raw.decode()) == {"d": "2024-01-15"}


def test_time() -> None:
    record = {"t": time(10, 30, 0)}
    raw = _to_json_bytes(record)
    parsed = json.loads(raw.decode())
    assert "10:30:00" in parsed["t"]


def test_decimal() -> None:
    record = {"p": Decimal("99.99999999999999")}
    raw = _to_json_bytes(record)
    assert json.loads(raw.decode()) == {"p": "99.99999999999999"}


def test_uuid() -> None:
    record = {"id": UUID("550e8400-e29b-41d4-a716-446655440000")}
    raw = _to_json_bytes(record)
    assert "550e8400-e29b-41d4-a716-446655440000" in raw.decode()


def test_set() -> None:
    record = {"s": {1, 2, 3}}
    raw = _to_json_bytes(record)
    assert set(json.loads(raw.decode())["s"]) == {1, 2, 3}


def test_enum() -> None:
    record = {"e": _ExampleEnum.FOO}
    raw = _to_json_bytes(record)
    assert json.loads(raw.decode()) == {"e": "foo"}


def test_path() -> None:
    record = {"p": Path("/tmp/foo.json")}
    raw = _to_json_bytes(record)
    assert "/tmp/foo.json" in raw.decode()


def test_float_nan() -> None:
    record = {"x": float("nan")}
    raw = _to_json_bytes(record)
    assert json.loads(raw.decode()) == {"x": None}


def test_float_inf() -> None:
    record = {"x": float("inf")}
    raw = _to_json_bytes(record)
    assert json.loads(raw.decode()) == {"x": None}


def test_full_record_like_rest_api() -> None:
    record = {
        "id": 1,
        "name": "test",
        "created_at": datetime(2024, 1, 15, 12, 0, 0),
        "price": Decimal("19.99"),
        "tags": {"a", "b"},
        "ref": UUID("550e8400-e29b-41d4-a716-446655440000"),
    }
    raw = _to_json_bytes(record)
    parsed = json.loads(raw.decode())
    assert parsed["id"] == 1
    assert parsed["name"] == "test"
    assert parsed["created_at"] == "2024-01-15T12:00:00"
    assert parsed["price"] == "19.99"
    assert set(parsed["tags"]) == {"a", "b"}
    assert parsed["ref"] == "550e8400-e29b-41d4-a716-446655440000"


def test_unsupported_type_raises() -> None:
    """Custom types not supported by Pydantic raise (no fallback)."""
    class C:
        pass
    record = {"c": C()}
    with pytest.raises(Exception):  # PydanticSerializationError or similar
        _to_json_bytes(record)
