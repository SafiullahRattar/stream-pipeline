"""
Prometheus metrics for the streaming pipeline.

Exposes counters, gauges, and histograms that track event throughput,
processing latency, error rates, and system health. All metrics are
prefixed with the configured namespace (default: stream_pipeline).
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from src.config import settings

logger = logging.getLogger(__name__)

_metrics_instance: PipelineMetrics | None = None
_lock = threading.Lock()


@dataclass
class PipelineMetrics:
    """Container for all pipeline Prometheus metrics."""

    # Producer metrics
    events_produced: Counter
    events_produced_bytes: Counter

    # Consumer metrics
    events_consumed: Counter
    events_failed: Counter
    events_dlq: Counter

    # Processing metrics
    processing_latency: Histogram
    aggregations_emitted: Counter

    # Sink metrics
    events_written: Counter
    sink_batch_size: Histogram
    sink_flush_latency: Histogram

    # System gauges
    active_windows: Gauge
    active_sessions: Gauge
    consumer_partitions: Gauge
    consumer_lag: Gauge


def create_metrics() -> PipelineMetrics:
    """Create and register all Prometheus metrics."""
    ns = settings.metrics.namespace

    return PipelineMetrics(
        events_produced=Counter(
            f"{ns}_events_produced_total",
            "Total number of events successfully produced to Kafka",
        ),
        events_produced_bytes=Counter(
            f"{ns}_events_produced_bytes_total",
            "Total bytes produced to Kafka",
        ),
        events_consumed=Counter(
            f"{ns}_events_consumed_total",
            "Total number of events consumed and processed",
            ["event_type"],
        ),
        events_failed=Counter(
            f"{ns}_events_failed_total",
            "Total number of events that failed processing",
            ["stage"],
        ),
        events_dlq=Counter(
            f"{ns}_events_dlq_total",
            "Total number of events sent to the dead letter queue",
        ),
        processing_latency=Histogram(
            f"{ns}_processing_latency_seconds",
            "Event processing latency in seconds",
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
        ),
        aggregations_emitted=Counter(
            f"{ns}_aggregations_emitted_total",
            "Total number of window aggregations emitted",
            ["window_type"],
        ),
        events_written=Counter(
            f"{ns}_events_written_total",
            "Total events successfully written to PostgreSQL",
        ),
        sink_batch_size=Histogram(
            f"{ns}_sink_batch_size",
            "Number of events per Postgres batch write",
            buckets=[10, 50, 100, 250, 500, 1000],
        ),
        sink_flush_latency=Histogram(
            f"{ns}_sink_flush_latency_seconds",
            "Time spent flushing a batch to PostgreSQL",
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        ),
        active_windows=Gauge(
            f"{ns}_active_windows",
            "Number of currently open aggregation windows",
        ),
        active_sessions=Gauge(
            f"{ns}_active_sessions",
            "Number of currently active user sessions",
        ),
        consumer_partitions=Gauge(
            f"{ns}_consumer_partitions",
            "Number of partitions assigned to this consumer",
        ),
        consumer_lag=Gauge(
            f"{ns}_consumer_lag",
            "Consumer group lag in number of messages",
        ),
    )


def get_metrics() -> PipelineMetrics:
    """Get the singleton metrics instance (lazy initialization)."""
    global _metrics_instance
    if _metrics_instance is None:
        with _lock:
            if _metrics_instance is None:
                _metrics_instance = create_metrics()
    return _metrics_instance


def start_metrics_server() -> None:
    """Start the Prometheus HTTP metrics server in a daemon thread."""
    port = settings.metrics.port
    try:
        start_http_server(port)
        logger.info("Prometheus metrics server started on port %d", port)
    except OSError as exc:
        logger.warning("Could not start metrics server on port %d: %s", port, exc)
