"""Kafka target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_kafka.sinks import (
    KafkaSink,
)


class TargetKafka(Target):
    """Meltano Target for Apache Kafka."""

    name = "target-kafka"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "bootstrap_servers",
            th.StringType(nullable=False),
            required=True,
            description="Kafka bootstrap servers (comma separated: localhost:9092,localhost:9093)",
        ),
        th.Property(
            "topic_prefix",
            th.StringType(),
            default="",
            description="Prefix to add to all topic names",
        ),
        th.Property(
            "key_properties",
            th.ArrayType(th.StringType()),
            default=[],
            description="Properties to use as Kafka message key (empty = round-robin partition)",
        ),
        th.Property(
            "batch_size",
            th.IntegerType(),
            default=100,
            description="Number of records to batch before sending to kafka",
        ),
        th.Property(
            "compression_type",
            th.StringType(),
            default="snappy",
            allowed_values=["none", "gzip", "snappy", "lz4", "zstd"],
            description="Kafka message compression",
        ),
        th.Property(
            "include_sdc_properties",
            th.BooleanType(),
            default=True,
            description="Include Meltano-specific metadata (_sdc_extracted_at, _sdc_received_at, etc.)",
        )
    ).to_dict()

    default_sink_class = KafkaSink


if __name__ == "__main__":
    TargetKafka.cli()
