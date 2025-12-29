"""
Kafka producer for clickstream events.

Serializes events with Avro using fastavro, sends them to the clickstream
topic with the user_id as the partition key (ensures per-user ordering),
and exposes Prometheus metrics for monitoring.
"""

from __future__ import annotations

import io
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any

import fastavro
from confluent_kafka import KafkaError, Producer

from src.config import settings
from src.metrics.prometheus import get_metrics, start_metrics_server
from src.producer.event_generator import EventGenerator

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "clickstream.avsc"


def load_avro_schema(path: Path) -> dict[str, Any]:
    """Load and parse an Avro schema file."""
    with open(path) as f:
        schema = json.load(f)
    # fastavro expects a parsed schema
    return fastavro.parse_schema(schema)


def serialize_avro(record: dict[str, Any], parsed_schema: Any) -> bytes:
    """Serialize a single record to Avro binary format."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


class ClickstreamProducer:
    """
    High-throughput Kafka producer for clickstream events.

    Features:
    - Avro serialization with schema validation
    - Partition key on user_id for per-user ordering
    - Delivery confirmation callbacks
    - Prometheus metrics for produced/failed counts
    - Graceful shutdown on SIGINT/SIGTERM
    """

    def __init__(self) -> None:
        self._config = settings.kafka
        self._producer_config = settings.producer
        self._schema = load_avro_schema(SCHEMA_PATH)
        self._generator = EventGenerator()
        self._metrics = get_metrics()
        self._running = False

        self._producer = Producer({
            "bootstrap.servers": self._config.bootstrap_servers,
            "client.id": "clickstream-producer",
            "acks": "all",                    # durability: wait for all ISR
            "retries": 5,
            "retry.backoff.ms": 500,
            "linger.ms": 10,                  # micro-batch for throughput
            "batch.num.messages": 1000,
            "compression.type": "snappy",
            "enable.idempotence": True,        # exactly-once per partition
        })

    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        """Called once per message to confirm delivery or log failure."""
        if err is not None:
            logger.error("Delivery failed for %s: %s", msg.key(), err)
            self._metrics.events_failed.labels(stage="produce").inc()
        else:
            self._metrics.events_produced.inc()

    def produce_event(self, event: dict[str, Any]) -> None:
        """Serialize and send a single event to Kafka."""
        try:
            payload = serialize_avro(event, self._schema)
            self._producer.produce(
                topic=self._config.clickstream_topic,
                key=event.get("user_id", "unknown").encode("utf-8"),
                value=payload,
                callback=self._delivery_callback,
            )
        except Exception as exc:
            logger.warning("Failed to produce event %s: %s", event.get("event_id"), exc)
            self._metrics.events_failed.labels(stage="serialize").inc()

    def run(self) -> None:
        """Main producer loop: generate events at the configured rate."""
        self._running = True
        target_interval = 1.0 / self._producer_config.events_per_second

        logger.info(
            "Starting producer: %.0f events/sec to topic '%s'",
            self._producer_config.events_per_second,
            self._config.clickstream_topic,
        )

        events_since_cleanup = 0

        while self._running:
            start = time.monotonic()

            event = self._generator.generate_event()
            self.produce_event(event)

            # Serve delivery callbacks
            self._producer.poll(0)

            # Periodic session cleanup
            events_since_cleanup += 1
            if events_since_cleanup >= 10000:
                expired = self._generator.cleanup_expired_sessions()
                if expired > 0:
                    logger.debug("Cleaned up %d expired sessions", expired)
                events_since_cleanup = 0

            # Rate limiting
            elapsed = time.monotonic() - start
            sleep_time = target_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Flush remaining messages
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d messages were not delivered on shutdown", remaining)
        logger.info("Producer stopped")

    def stop(self) -> None:
        """Signal the producer loop to stop."""
        self._running = False


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start_metrics_server()
    producer = ClickstreamProducer()

    def shutdown(signum: int, frame: Any) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        producer.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        producer.run()
    except KeyboardInterrupt:
        producer.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
