# src/target_kafka/sinks.py

from __future__ import annotations

import json

from singer_sdk.sinks import BatchSink
from kafka import KafkaProducer
from kafka.errors import KafkaError


class KafkaSink(BatchSink):
    """Kafka target sink class."""

    max_size = 10000  # Max records to write in one batch

    def __init__(
        self,
        target,
        stream_name: str,
        schema: dict,
        key_properties: list[str],
    ) -> None:
        """Initialize Kafka sink.

        Args:
            target: The target instance.
            stream_name: The stream name.
            schema: The stream schema.
            key_properties: List of key properties.
        """
        super().__init__(target, stream_name, schema, key_properties)

        self.producer: KafkaProducer | None = None
        self._batch: list[dict] = []

        # Get batch size from config (default 100 from target.py)
        self._batch_size = self.config.get("batch_size", 100)

        # Derive topic name from stream name with optional prefix
        topic_prefix = self.config.get("topic_prefix", "")
        self.topic_name = (
            f"{topic_prefix}{stream_name}"
            if topic_prefix
            else stream_name
        )

        self.logger.info(
            f"Initialized KafkaSink for stream '{stream_name}' → "
            f"topic '{self.topic_name}' (batch_size={self._batch_size})"
        )

    def _init_producer(self) -> None:
        """Initialize Kafka producer with configuration.

        Raises:
            ValueError: If bootstrap_servers not configured.
            KafkaError: If connection to Kafka fails.
        """
        if self.producer is not None:
            return  # Already initialized

        bootstrap_servers = self.config.get("bootstrap_servers")
        if not bootstrap_servers:
            raise ValueError(
                "bootstrap_servers is required in configuration. "
                "Format: localhost:9092,localhost:9093"
            )

        compression_type = self.config.get("compression_type", "snappy")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                compression_type=compression_type,
                acks="all",  # Wait for all in-sync replicas to acknowledge
                retries=3,
                linger_ms=10,  # Wait up to 10ms to batch messages
                value_serializer=lambda v: (
                    v if isinstance(v, bytes)
                    else json.dumps(v).encode("utf-8")
                ),
                key_serializer=lambda k: (
                    k if isinstance(k, bytes)
                    else json.dumps(k).encode("utf-8") if k
                    else None
                ),
            )
            self.logger.info(
                f"KafkaProducer initialized for bootstrap_servers: "
                f"{bootstrap_servers} (compression: {compression_type})"
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize KafkaProducer: {e}")
            raise

    def _get_message_key(self, record: dict) -> str | None:
        """Extract message key from record based on key_properties.

        Args:
            record: The data record.

        Returns:
            JSON string of key properties (or None if no keys configured).
        """
        key_properties = self.config.get("key_properties", [])

        if not key_properties:
            # No key specified: Kafka will use round-robin partition assignment
            return None

        # Build composite key from specified properties
        key_values = {}
        for prop in key_properties:
            if prop in record:
                key_values[prop] = record[prop]
            else:
                self.logger.warning(
                    f"Key property '{prop}' not found in record for stream "
                    f"'{self.stream_name}'. Record: {record}"
                )

        # Return JSON-serialized key (serializer handles bytes conversion)
        return json.dumps(key_values, sort_keys=True, default=str)

    def start_batch(self, context: dict) -> None:
        """Start a new batch.

        Args:
            context: Stream partition or context dictionary.
        """
        self._batch = []
        self.logger.debug(
            f"Starting batch for stream '{self.stream_name}' "
            f"(partition context: {context})"
        )

    def process_record(self, record: dict, context: dict) -> None:
        """Buffer a record and auto-flush if batch size reached.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        if self.producer is None:
            self._init_producer()

        # Add Meltano metadata if configured
        if self.config.get("include_sdc_properties", True):
            # These are already added by Meltano, but ensuring they are present
            if "_sdc_extracted_at" not in record and hasattr(context, "get"):
                record["_sdc_extracted_at"] = context.get("_sdc_extracted_at")

        self._batch.append(record)

        # Auto-flush when batch size reached
        if len(self._batch) >= self._batch_size:
            self.logger.debug(
                f"Batch size ({self._batch_size}) reached for stream "
                f"'{self.stream_name}'. Flushing..."
            )
            self._flush_batch()

    def process_batch(self, context: dict) -> None:
        """Flush remaining records to Kafka when batch completes.

        Called by Meltano when stream batch is complete. Ensures all
        buffered records are written before acknowledging the batch.

        Args:
            context: Stream partition or context dictionary.
        """
        self.logger.debug(
            f"process_batch called for stream '{self.stream_name}'. "
            f"Flushing {len(self._batch)} buffered records..."
        )
        self._flush_batch()

    def _flush_batch(self) -> None:
        """Write buffered records to Kafka.

        Raises:
            KafkaError: If Kafka write fails.
        """
        if not self._batch:
            self.logger.debug(f"No records to flush for stream '{self.stream_name}'")
            return

        if self.producer is None:
            self._init_producer()

        batch_size = len(self._batch)

        try:
            for record in self._batch:
                message_key = self._get_message_key(record)
                # Note: value_serializer handles JSON encoding

                self.producer.send(
                    self.topic_name,
                    value=record,  # Serializer will handle JSON encoding
                    key=message_key,  # Serializer will handle if bytes needed
                )

            # Wait for all pending sends to complete
            self.producer.flush(timeout=30)

            self.logger.info(
                f"Flushed {batch_size} records to Kafka topic "
                f"'{self.topic_name}' from stream '{self.stream_name}'"
            )
            self._batch = []

        except KafkaError as e:
            self.logger.error(
                f"Kafka error while writing {batch_size} records to topic "
                f"'{self.topic_name}': {e}"
            )
            raise
        except Exception as e:
            self.logger.error(
                f"Unexpected error while flushing batch to topic "
                f"'{self.topic_name}': {e}"
            )
            raise

    def close_connection(self) -> None:
        """Close Kafka producer and flush any pending messages.

        Called by Meltano during target shutdown. Ensures graceful
        termination and all pending records are written.

        Raises:
            KafkaError: If producer close fails.
        """
        if self.producer is None:
            self.logger.debug("Producer not initialized, nothing to close")
            return

        try:
            # Final flush of any pending records
            self._flush_batch()

            # Close producer
            self.producer.flush(timeout=30)
            self.producer.close(timeout=30)

            self.logger.info(
                f"KafkaProducer closed successfully for stream "
                f"'{self.stream_name}'"
            )
        except Exception as e:
            self.logger.error(
                f"Error closing KafkaProducer for stream "
                f"'{self.stream_name}': {e}"
            )
            raise
