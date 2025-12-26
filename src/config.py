"""
Centralized configuration management using Pydantic Settings.

All configuration is loaded from environment variables with sensible defaults
for local Docker Compose development. No secrets are hardcoded.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    """Kafka broker and topic configuration."""

    model_config = {"env_prefix": "KAFKA_"}

    bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    clickstream_topic: str = Field(
        default="clickstream.events",
        description="Topic for raw clickstream events",
    )
    aggregated_topic: str = Field(
        default="clickstream.aggregated",
        description="Topic for windowed aggregation results",
    )
    dlq_topic: str = Field(
        default="clickstream.dlq",
        description="Dead letter queue topic for failed events",
    )
    consumer_group: str = Field(
        default="stream-pipeline-consumers",
        description="Consumer group ID for offset management",
    )
    auto_offset_reset: str = Field(
        default="earliest",
        description="Where to start consuming when no committed offset exists",
    )
    enable_auto_commit: bool = Field(
        default=False,
        description="Disable auto-commit for manual offset management (at-least-once)",
    )
    session_timeout_ms: int = Field(
        default=30000,
        description="Consumer session timeout in milliseconds",
    )
    max_poll_interval_ms: int = Field(
        default=300000,
        description="Maximum time between poll() calls before consumer is removed from group",
    )
    schema_registry_url: str = Field(
        default="http://schema-registry:8081",
        description="Confluent Schema Registry URL",
    )


class PostgresConfig(BaseSettings):
    """PostgreSQL connection configuration."""

    model_config = {"env_prefix": "POSTGRES_"}

    host: str = Field(default="postgres", description="PostgreSQL hostname")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(default="pipeline", description="Database name")
    user: str = Field(default="pipeline", description="Database user")
    password: str = Field(default="pipeline", description="Database password")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    @property
    def url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class SinkConfig(BaseSettings):
    """Batch sink configuration for PostgreSQL writes."""

    model_config = {"env_prefix": "SINK_"}

    batch_size: int = Field(
        default=500,
        description="Flush to Postgres after accumulating this many events",
    )
    flush_interval_seconds: float = Field(
        default=5.0,
        description="Flush to Postgres after this many seconds, even if batch is not full",
    )
    max_retries: int = Field(
        default=3,
        description="Number of retries for failed Postgres inserts",
    )
    retry_backoff_seconds: float = Field(
        default=1.0,
        description="Initial backoff between retries (doubles each attempt)",
    )


class ProducerConfig(BaseSettings):
    """Event generator configuration."""

    model_config = {"env_prefix": "PRODUCER_"}

    events_per_second: float = Field(
        default=100.0,
        description="Target event generation rate",
    )
    num_users: int = Field(
        default=1000,
        description="Size of the simulated user pool",
    )
    num_products: int = Field(
        default=500,
        description="Size of the simulated product catalog",
    )
    error_rate: float = Field(
        default=0.02,
        description="Fraction of events intentionally malformed (for DLQ testing)",
    )


class WindowConfig(BaseSettings):
    """Windowed aggregation configuration."""

    model_config = {"env_prefix": "WINDOW_"}

    tumbling_window_seconds: int = Field(
        default=60,
        description="Tumbling window size for per-minute event counts",
    )
    revenue_window_seconds: int = Field(
        default=300,
        description="Tumbling window size for revenue aggregation (5 min)",
    )
    session_timeout_seconds: int = Field(
        default=1800,
        description="User session timeout (30 min of inactivity)",
    )
    late_arrival_tolerance_seconds: int = Field(
        default=30,
        description="How long to keep expired windows for late-arriving events",
    )


class DashboardConfig(BaseSettings):
    """Dashboard server configuration."""

    model_config = {"env_prefix": "DASHBOARD_"}

    host: str = Field(default="0.0.0.0", description="Dashboard bind address")
    port: int = Field(default=8000, description="Dashboard port")
    ws_broadcast_interval: float = Field(
        default=1.0,
        description="Seconds between WebSocket broadcast updates",
    )


class MetricsConfig(BaseSettings):
    """Prometheus metrics configuration."""

    model_config = {"env_prefix": "METRICS_"}

    port: int = Field(default=9090, description="Prometheus metrics server port")
    namespace: str = Field(
        default="stream_pipeline",
        description="Prometheus metric name prefix",
    )


class AppConfig:
    """Top-level configuration aggregator."""

    def __init__(self) -> None:
        self.kafka = KafkaConfig()
        self.postgres = PostgresConfig()
        self.sink = SinkConfig()
        self.producer = ProducerConfig()
        self.window = WindowConfig()
        self.dashboard = DashboardConfig()
        self.metrics = MetricsConfig()


# Module-level singleton
settings = AppConfig()
