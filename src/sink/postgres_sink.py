"""
Batched PostgreSQL sink for processed clickstream events.

Accumulates events in memory and flushes them to PostgreSQL using batch
INSERT with execute_values for high throughput. Implements a dual trigger:
flush when the batch reaches N events OR when T seconds have elapsed,
whichever comes first.

Failed batches are retried with exponential backoff. Events that still fail
after max retries are logged and counted in Prometheus metrics.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import psycopg2
import psycopg2.extras

from src.config import settings
from src.metrics.prometheus import get_metrics

logger = logging.getLogger(__name__)


class PostgresSink:
    """
    Batched, fault-tolerant PostgreSQL sink.

    Thread Safety: This class is NOT thread-safe. It is designed to be used
    by a single consumer thread. For multi-threaded use, instantiate one
    sink per thread.
    """

    def __init__(self) -> None:
        self._config = settings.sink
        self._pg_config = settings.postgres
        self._metrics = get_metrics()
        self._buffer: list[dict[str, Any]] = []
        self._last_flush_time = time.monotonic()
        self._conn: psycopg2.extensions.connection | None = None

    def _get_connection(self) -> psycopg2.extensions.connection:
        """Get or create a PostgreSQL connection with auto-reconnect."""
        if self._conn is None or self._conn.closed:
            logger.info("Connecting to PostgreSQL at %s:%d", self._pg_config.host, self._pg_config.port)
            self._conn = psycopg2.connect(self._pg_config.dsn)
            self._conn.autocommit = False
        return self._conn

    def add_event(self, event: dict[str, Any]) -> None:
        """
        Add a processed event to the write buffer.

        Automatically triggers a flush if the buffer reaches the configured
        batch size.
        """
        self._buffer.append(event)

        if len(self._buffer) >= self._config.batch_size:
            self.flush()

    def flush(self) -> None:
        """
        Write all buffered events to PostgreSQL in a single batch INSERT.

        Uses psycopg2.extras.execute_values for efficient multi-row inserts.
        Retries with exponential backoff on failure.
        """
        if not self._buffer:
            return

        batch = self._buffer[:]
        self._buffer.clear()
        self._last_flush_time = time.monotonic()

        for attempt in range(self._config.max_retries):
            try:
                self._write_batch(batch)
                self._metrics.sink_batch_size.observe(len(batch))
                self._metrics.events_written.inc(len(batch))
                logger.debug("Flushed %d events to PostgreSQL", len(batch))
                return
            except Exception as exc:
                backoff = self._config.retry_backoff_seconds * (2 ** attempt)
                logger.warning(
                    "Postgres write failed (attempt %d/%d): %s. Retrying in %.1fs",
                    attempt + 1, self._config.max_retries, exc, backoff,
                )
                # Reconnect on failure
                self._conn = None
                time.sleep(backoff)

        # All retries exhausted
        logger.error(
            "Failed to write %d events after %d retries, events dropped",
            len(batch), self._config.max_retries,
        )
        self._metrics.events_failed.labels(stage="postgres_write").inc(len(batch))

    def _write_batch(self, batch: list[dict[str, Any]]) -> None:
        """Execute the batch insert within a transaction."""
        conn = self._get_connection()

        # Extract values in column order
        values = []
        for e in batch:
            values.append((
                e.get("event_id"),
                e.get("event_type"),
                e.get("timestamp"),
                e.get("user_id"),
                e.get("session_id"),
                e.get("product_id"),
                e.get("product_category"),
                e.get("price_cents"),
                e.get("quantity"),
                e.get("revenue_cents", 0),
                e.get("search_query"),
                e.get("page_url"),
                e.get("referrer"),
                e.get("user_agent"),
                e.get("ip_address"),
                e.get("geo_country"),
                e.get("geo_city"),
                e.get("event_hour"),
                e.get("is_mobile"),
            ))

        insert_sql = """
            INSERT INTO clickstream_events (
                event_id, event_type, event_timestamp_ms, user_id, session_id,
                product_id, product_category, price_cents, quantity, revenue_cents,
                search_query, page_url, referrer, user_agent, ip_address,
                geo_country, geo_city, event_hour, is_mobile
            ) VALUES %s
            ON CONFLICT (event_id) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, insert_sql, values,
                    template="""(
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )""",
                    page_size=500,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def should_flush(self) -> bool:
        """Check if a time-based flush is due."""
        elapsed = time.monotonic() - self._last_flush_time
        return len(self._buffer) > 0 and elapsed >= self._config.flush_interval_seconds

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)

    def close(self) -> None:
        """Flush remaining events and close the connection."""
        self.flush()
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("PostgreSQL connection closed")
