"""
Kafka consumer with stream processing pipeline.

Consumes Avro-encoded clickstream events, runs them through the processing
chain (validate, filter, normalize, enrich), feeds them into windowed
aggregators, writes processed events to the PostgreSQL sink, and publishes
aggregation results to the aggregated topic.

Implements at-least-once delivery with manual offset commits after successful
processing and sinking.
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
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from src.config import settings
from src.consumer.processors import ValidationError, process_event
from src.consumer.windowing import SessionDetector, TumblingWindowAggregator
from src.metrics.prometheus import get_metrics, start_metrics_server
from src.sink.dead_letter import DeadLetterQueue
from src.sink.postgres_sink import PostgresSink

logger = logging.getLogger(__name__)

CLICKSTREAM_SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "clickstream.avsc"
AGGREGATED_SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "aggregated.avsc"


def load_schema(path: Path) -> Any:
    with open(path) as f:
        return fastavro.parse_schema(json.load(f))


def deserialize_avro(data: bytes, schema: Any) -> dict[str, Any]:
    """Deserialize Avro binary data using a schemaless reader."""
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, schema)


class StreamConsumer:
    """
    Main consumer that orchestrates the entire processing pipeline.

    Architecture:
        Kafka topic -> deserialize -> process -> window aggregate
                                        |              |
                                        v              v
                                   Postgres sink   Aggregated topic
                                        |
                                   DLQ (on failure)
    """

    def __init__(self) -> None:
        self._config = settings.kafka
        self._metrics = get_metrics()
        self._running = False

        # Load Avro schemas
        self._clickstream_schema = load_schema(CLICKSTREAM_SCHEMA_PATH)
        self._aggregated_schema = load_schema(AGGREGATED_SCHEMA_PATH)

        # Kafka consumer with manual offset management
        self._consumer = Consumer({
            "bootstrap.servers": self._config.bootstrap_servers,
            "group.id": self._config.consumer_group,
            "auto.offset.reset": self._config.auto_offset_reset,
            "enable.auto.commit": self._config.enable_auto_commit,
            "session.timeout.ms": self._config.session_timeout_ms,
            "max.poll.interval.ms": self._config.max_poll_interval_ms,
            "fetch.min.bytes": 1024,
            "fetch.wait.max.ms": 100,
        })

        # Producer for aggregated results
        self._agg_producer = Producer({
            "bootstrap.servers": self._config.bootstrap_servers,
            "client.id": "aggregation-producer",
            "acks": "all",
            "compression.type": "snappy",
        })

        # Processing components
        self._events_per_min = TumblingWindowAggregator(
            window_size_seconds=settings.window.tumbling_window_seconds,
            window_type="events_per_minute",
        )
        self._revenue_per_5min = TumblingWindowAggregator(
            window_size_seconds=settings.window.revenue_window_seconds,
            window_type="revenue_per_5min",
        )
        self._session_detector = SessionDetector()

        # Sinks
        self._pg_sink = PostgresSink()
        self._dlq = DeadLetterQueue()

        # Offset tracking for manual commits
        self._pending_offsets: dict[tuple[str, int], TopicPartition] = {}

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Callback when partitions are assigned to this consumer."""
        partition_list = [(p.topic, p.partition) for p in partitions]
        logger.info("Partitions assigned: %s", partition_list)
        self._metrics.consumer_partitions.set(len(partitions))

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Callback when partitions are revoked (rebalance). Commit pending offsets."""
        logger.info("Partitions revoked, committing pending offsets...")
        self._commit_offsets()
        self._pg_sink.flush()

    def _commit_offsets(self) -> None:
        """Commit accumulated offsets to Kafka."""
        if self._pending_offsets:
            offsets = list(self._pending_offsets.values())
            try:
                self._consumer.commit(offsets=offsets, asynchronous=False)
                self._pending_offsets.clear()
                logger.debug("Committed %d partition offsets", len(offsets))
            except KafkaException as exc:
                logger.error("Failed to commit offsets: %s", exc)

    def _track_offset(self, topic: str, partition: int, offset: int) -> None:
        """Track the latest offset per partition for batch commits."""
        key = (topic, partition)
        self._pending_offsets[key] = TopicPartition(topic, partition, offset + 1)

    def _publish_aggregation(self, result: dict[str, Any]) -> None:
        """Publish a windowed aggregation result to the aggregated topic."""
        try:
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, self._aggregated_schema, result)
            self._agg_producer.produce(
                topic=self._config.aggregated_topic,
                key=result["window_type"].encode("utf-8"),
                value=buf.getvalue(),
            )
            self._agg_producer.poll(0)
            self._metrics.aggregations_emitted.labels(
                window_type=result["window_type"]
            ).inc()
        except Exception as exc:
            logger.error("Failed to publish aggregation: %s", exc)

    def _process_message(self, key: bytes | None, value: bytes, topic: str,
                         partition: int, offset: int) -> None:
        """Process a single consumed message through the full pipeline."""
        # Deserialize
        try:
            event = deserialize_avro(value, self._clickstream_schema)
        except Exception as exc:
            logger.warning(
                "Deserialization failed (topic=%s, partition=%d, offset=%d): %s",
                topic, partition, offset, exc,
            )
            self._dlq.send(
                original_value=value,
                error_reason=f"Deserialization error: {exc}",
                source_topic=topic,
                source_partition=partition,
                source_offset=offset,
            )
            self._metrics.events_failed.labels(stage="deserialize").inc()
            return

        # Process (validate, filter, normalize, enrich)
        try:
            processed = process_event(event)
        except ValidationError as exc:
            self._dlq.send(
                original_value=value,
                error_reason=exc.reason,
                source_topic=topic,
                source_partition=partition,
                source_offset=offset,
            )
            self._metrics.events_dlq.inc()
            return

        # Update windowed aggregations
        self._events_per_min.add_event(processed)
        self._revenue_per_5min.add_event(processed)
        self._session_detector.track_event(processed)

        # Send to PostgreSQL sink (batched)
        self._pg_sink.add_event(processed)

        self._metrics.events_consumed.labels(
            event_type=processed["event_type"]
        ).inc()

    def _emit_aggregations(self) -> None:
        """Check all window aggregators and emit any closed windows."""
        for result in self._events_per_min.emit_closed_windows():
            self._publish_aggregation(result)

        for result in self._revenue_per_5min.emit_closed_windows():
            self._publish_aggregation(result)

        for result in self._session_detector.emit_expired_sessions():
            self._publish_aggregation(result)

    def run(self) -> None:
        """
        Main consumer loop.

        Polls Kafka, processes events, manages offsets, and periodically
        flushes sinks and emits window aggregations.
        """
        self._running = True
        self._consumer.subscribe(
            [self._config.clickstream_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        logger.info(
            "Consumer started: group='%s', topic='%s'",
            self._config.consumer_group,
            self._config.clickstream_topic,
        )

        last_commit_time = time.monotonic()
        last_flush_time = time.monotonic()
        commit_interval = 5.0  # seconds

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    # No message available, check timers
                    pass
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            "Reached end of partition %s [%d] at offset %d",
                            msg.topic(), msg.partition(), msg.offset(),
                        )
                    else:
                        logger.error("Consumer error: %s", msg.error())
                        self._metrics.events_failed.labels(stage="consume").inc()
                else:
                    self._process_message(
                        key=msg.key(),
                        value=msg.value(),
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                    self._track_offset(msg.topic(), msg.partition(), msg.offset())

                now = time.monotonic()

                # Periodic offset commit (at-least-once guarantee)
                if now - last_commit_time >= commit_interval:
                    self._commit_offsets()
                    last_commit_time = now

                # Periodic sink flush and window emission
                if now - last_flush_time >= settings.sink.flush_interval_seconds:
                    self._pg_sink.flush()
                    self._emit_aggregations()
                    last_flush_time = now

                    # Update gauge metrics
                    self._metrics.active_windows.set(
                        self._events_per_min.active_window_count
                        + self._revenue_per_5min.active_window_count
                    )
                    self._metrics.active_sessions.set(
                        self._session_detector.active_session_count
                    )

        except KeyboardInterrupt:
            pass
        finally:
            logger.info("Shutting down consumer...")
            self._pg_sink.flush()
            self._commit_offsets()
            self._consumer.close()
            self._pg_sink.close()
            self._dlq.close()
            logger.info("Consumer stopped")

    def stop(self) -> None:
        self._running = False


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start_metrics_server()
    consumer = StreamConsumer()

    def shutdown(signum: int, frame: Any) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        consumer.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        consumer.run()
    except Exception:
        logger.exception("Consumer crashed")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
