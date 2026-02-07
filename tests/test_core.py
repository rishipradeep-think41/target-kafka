"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

import pytest
from singer_sdk.testing import get_target_test_class

from target_kafka.target import TargetKafka

# Minimal config for standard SDK tests (KafkaProducer is mocked in conftest.py)
SAMPLE_CONFIG: dict[str, t.Any] = {
    "bootstrap_servers": "localhost:9092",
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetKafka,
    config=SAMPLE_CONFIG,
)


class TestTargetKafka(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/packages
        """
        return "resource"


