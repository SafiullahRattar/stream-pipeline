"""
Dead Letter Queue (DLQ) for events that fail processing.

Malformed events, deserialization failures, and validation errors are routed
to a dedicated Kafka DLQ topic with metadata about the failure. This allows
data engineers to inspect, debug, and optionally replay failed events without
blocking the main processing pipeline.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from confluent_kafka import Producer

from src.config import settings
from src.metrics.prometheus import get_metrics

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """
    Routes failed events to a Kafka dead letter queue topic.

    Each DLQ message includes:
    - The original event payload (base64-encoded if binary)
    - The error reason
    - Source topic/partition/offset for traceability
    - Timestamp of when the failure occurred
    """

    def __init__(self) -> None:
        self._config = settings.kafka
        self._metrics = get_metrics()

        self._producer = Producer({
            "bootstrap.servers": self._config.bootstrap_servers,
            "client.id": "dlq-producer",
            "acks": "all",
            "retries": 3,
        })

    def send(
        self,
        original_value: bytes | dict[str, Any],
        error_reason: str,
        source_topic: str,
        source_partition: int,
        source_offset: int,
    ) -> None:
        """
        Send a failed event to the DLQ topic.

        Args:
            original_value: The raw event payload (bytes or dict).
            error_reason: Human-readable description of why processing failed.
            source_topic: The Kafka topic the event was consumed from.
            source_partition: The partition the event was consumed from.
            source_offset: The offset of the failed event.
        """
        # Build DLQ envelope with metadata
        if isinstance(original_value, bytes):
            try:
                payload = original_value.decode("utf-8")
            except UnicodeDecodeError:
                import base64
                payload = base64.b64encode(original_value).decode("ascii")
        elif isinstance(original_value, dict):
            payload = json.dumps(original_value, default=str)
        else:
            payload = str(original_value)

        dlq_message: dict[str, Any] = {
            "original_payload": payload,
            "error_reason": error_reason,
            "source_topic": source_topic,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "failed_at": int(time.time() * 1000),
            "failed_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        try:
            self._producer.produce(
                topic=self._config.dlq_topic,
                key=f"{source_topic}:{source_partition}:{source_offset}".encode("utf-8"),
                value=json.dumps(dlq_message).encode("utf-8"),
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
            self._metrics.events_dlq.inc()

            logger.warning(
                "Event sent to DLQ: topic=%s partition=%d offset=%d reason='%s'",
                source_topic, source_partition, source_offset, error_reason,
            )
        except Exception as exc:
            # DLQ failure should never crash the pipeline
            logger.error(
                "Failed to send event to DLQ: %s (original error: %s)",
                exc, error_reason,
            )
            self._metrics.events_failed.labels(stage="dlq_write").inc()

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("DLQ delivery failed: %s", err)
            self._metrics.events_failed.labels(stage="dlq_delivery").inc()

    def flush(self, timeout: float = 5.0) -> int:
        """Flush pending DLQ messages."""
        return self._producer.flush(timeout=timeout)

    def close(self) -> None:
        """Flush and clean up."""
        remaining = self.flush()
        if remaining > 0:
            logger.warning("%d DLQ messages not delivered on close", remaining)
