# Stream Pipeline

A production-grade, real-time data streaming pipeline built in Python that ingests, processes, and analyzes e-commerce clickstream events. The system demonstrates core data engineering patterns: schema-enforced event streaming, windowed aggregations, dead letter queues, batched database writes, and live monitoring -- all orchestrated with Docker Compose.

## The Problem

E-commerce platforms generate millions of user interaction events per day -- page views, searches, cart actions, and purchases. Engineering teams need to:

1. **Ingest events at scale** with schema validation and backward compatibility
2. **Process events in real-time** -- enrich, filter, validate, and aggregate
3. **Detect sessions and compute windowed metrics** (events/minute, revenue/5-min)
4. **Route malformed data** to a dead letter queue without blocking the pipeline
5. **Store processed events** efficiently in a relational database for analytics
6. **Monitor throughput, lag, and errors** in real-time

This project implements all of the above as a fully containerized system.

## Architecture

```
                                                  ┌─────────────────────┐
                                                  │   Prometheus        │
                                                  │   (metrics scrape)  │
                                                  └───────┬─────────────┘
                                                          │ :9090
    ┌──────────────┐     ┌──────────────────┐     ┌───────┴─────────────┐
    │              │     │                  │     │                     │
    │   Producer   │────>│  Kafka Broker    │────>│   Consumer          │
    │              │     │                  │     │                     │
    │  Generates   │     │  clickstream.    │     │  ┌───────────────┐  │
    │  clickstream │     │    events (6P)   │     │  │ Validate      │  │
    │  events at   │     │                  │     │  │ Filter bots   │  │
    │  100 evt/s   │     │  clickstream.    │     │  │ Normalize     │  │
    │              │     │    aggregated    │<────│  │ Enrich        │  │
    │  Avro schema │     │                  │     │  └───────┬───────┘  │
    │  validation  │     │  clickstream.    │     │          │          │
    └──────────────┘     │    dlq           │     │  ┌───────┴───────┐  │
                         └──────────────────┘     │  │  Windowing    │  │
                                │                 │  │  - 1min count │  │
                                │                 │  │  - 5min rev   │  │
           ┌────────────────────┘                 │  │  - Sessions   │  │
           │                                      │  └───────┬───────┘  │
           v                                      └──────────┼──────────┘
    ┌──────────────┐                                         │
    │  Schema      │                              ┌──────────┴──────────┐
    │  Registry    │                              │                     │
    │  (Avro)      │                  ┌───────────┤     Sinks           │
    └──────────────┘                  │           │                     │
                                      │           └─────────────────────┘
                                      │                     │
                               ┌──────┴──────┐      ┌──────┴──────┐
                               │             │      │             │
                               │ PostgreSQL  │      │  Dead Letter │
                               │ (batch      │      │  Queue       │
                               │  inserts)   │      │  (Kafka DLQ) │
                               │             │      │             │
                               └─────────────┘      └─────────────┘

    ┌──────────────────────────────────────────────────────────────────┐
    │  Dashboard (FastAPI + WebSocket)                                 │
    │  Consumes aggregated topic, broadcasts live metrics to browser   │
    │  http://localhost:8000                                           │
    └──────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **Producer** generates realistic e-commerce clickstream events (page views, searches, cart actions, purchases) with weighted distributions and session continuity. Events are serialized with Avro and sent to `clickstream.events` partitioned by `user_id`.

2. **Consumer** reads from the topic with manual offset management (at-least-once delivery):
   - **Validate**: checks required fields, event types, price constraints
   - **Filter**: removes bot traffic based on user-agent signatures
   - **Normalize**: standardizes country codes, lowercases search queries
   - **Enrich**: computes `revenue_cents`, `event_hour`, `is_mobile`

3. **Windowed Aggregations** run in-memory:
   - **Events per minute** (60s tumbling window): counts by event type, unique users
   - **Revenue per 5 minutes** (300s tumbling window): sums purchase revenue
   - **Session detection**: groups events by user with 30-min inactivity timeout

4. **PostgreSQL Sink** batches processed events (500 events or 5 seconds, whichever comes first) and writes them using `execute_values` for throughput. Failed batches retry with exponential backoff.

5. **Dead Letter Queue** catches malformed events (missing fields, invalid types, negative prices) and routes them to `clickstream.dlq` with error metadata for debugging.

6. **Dashboard** consumes from `clickstream.aggregated`, maintains state, and broadcasts snapshots to all connected WebSocket clients every second.

## Event Schema

Events use Avro serialization for schema enforcement and evolution support.

### ClickstreamEvent

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | string | UUID v4 unique identifier |
| `event_type` | enum | `page_view`, `add_to_cart`, `remove_from_cart`, `purchase`, `search`, `wishlist` |
| `timestamp` | long (ms) | Event timestamp in milliseconds since epoch |
| `user_id` | string | Anonymized user identifier |
| `session_id` | string | Browser session identifier |
| `product_id` | string? | Product identifier |
| `product_category` | string? | Product category |
| `price_cents` | long? | Price in cents (avoids floating-point) |
| `quantity` | int? | Quantity for cart/purchase events |
| `search_query` | string? | Search text for search events |
| `page_url` | string | URL of the page |
| `referrer` | string? | HTTP referrer |
| `user_agent` | string | Browser user-agent |
| `ip_address` | string | Client IP (anonymized) |
| `geo_country` | string | ISO 3166-1 alpha-2 country code |
| `geo_city` | string? | City from IP geolocation |

### AggregatedMetrics

| Field | Type | Description |
|-------|------|-------------|
| `window_id` | string | Unique window instance ID |
| `window_type` | enum | `events_per_minute`, `revenue_per_5min`, `session_summary` |
| `window_start` / `window_end` | long (ms) | Window boundaries |
| `event_count` | long | Total events in window |
| `unique_users` | long | Distinct user count |
| `page_views` / `add_to_carts` / `purchases` | long | Counts by type |
| `total_revenue_cents` | long | Sum of purchase revenue |
| `top_products` | string[] | Top 10 products by frequency |
| `top_categories` | string[] | Top 5 categories |

## Quick Start

### Prerequisites

- Docker and Docker Compose v2
- 4 GB+ free RAM (Kafka + ZooKeeper + Schema Registry + Postgres)

### Run

```bash
# Clone and start
git clone https://github.com/SafiullahRattar/stream-pipeline.git
cd stream-pipeline
docker compose up -d

# Wait ~30s for all services to initialize, then open the dashboard
open http://localhost:8000
```

### Verify

```bash
# Check all services are running
docker compose ps

# Watch producer logs
docker compose logs -f producer

# Watch consumer logs (see window closures, batch flushes)
docker compose logs -f consumer

# Query events in PostgreSQL
docker compose exec postgres psql -U pipeline -d pipeline \
  -c "SELECT event_type, COUNT(*) FROM clickstream_events GROUP BY event_type;"

# Check DLQ for failed events
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic clickstream.dlq \
  --from-beginning --max-messages 5

# View Prometheus metrics
curl http://localhost:9091/metrics | grep stream_pipeline
```

### Stop

```bash
docker compose down          # stop containers
docker compose down -v       # stop and remove volumes (data reset)
```

## Configuration

All configuration is via environment variables with sensible defaults.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker addresses |
| `KAFKA_CLICKSTREAM_TOPIC` | `clickstream.events` | Raw events topic |
| `KAFKA_AGGREGATED_TOPIC` | `clickstream.aggregated` | Aggregation results topic |
| `KAFKA_DLQ_TOPIC` | `clickstream.dlq` | Dead letter queue topic |
| `KAFKA_CONSUMER_GROUP` | `stream-pipeline-consumers` | Consumer group ID |
| `PRODUCER_EVENTS_PER_SECOND` | `100` | Event generation rate |
| `PRODUCER_NUM_USERS` | `1000` | Simulated user pool size |
| `PRODUCER_ERROR_RATE` | `0.02` | Fraction of intentionally malformed events |
| `SINK_BATCH_SIZE` | `500` | Postgres batch insert size |
| `SINK_FLUSH_INTERVAL_SECONDS` | `5` | Max seconds between flushes |
| `WINDOW_TUMBLING_WINDOW_SECONDS` | `60` | Event count window size |
| `WINDOW_REVENUE_WINDOW_SECONDS` | `300` | Revenue window size |
| `WINDOW_SESSION_TIMEOUT_SECONDS` | `1800` | Session inactivity timeout |
| `POSTGRES_HOST` | `postgres` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DATABASE` | `pipeline` | Database name |
| `POSTGRES_USER` | `pipeline` | Database user |
| `POSTGRES_PASSWORD` | `pipeline` | Database password |
| `DASHBOARD_PORT` | `8000` | Dashboard HTTP port |
| `METRICS_PORT` | `9090` | Prometheus metrics port |

## Monitoring

### Prometheus Metrics

All metrics are prefixed with `stream_pipeline_`.

| Metric | Type | Description |
|--------|------|-------------|
| `events_produced_total` | Counter | Events sent to Kafka |
| `events_consumed_total` | Counter | Events processed (by type) |
| `events_failed_total` | Counter | Failed events (by stage) |
| `events_dlq_total` | Counter | Events routed to DLQ |
| `events_written_total` | Counter | Events written to Postgres |
| `aggregations_emitted_total` | Counter | Window aggregations emitted |
| `sink_batch_size` | Histogram | Batch sizes for Postgres writes |
| `active_windows` | Gauge | Currently open aggregation windows |
| `active_sessions` | Gauge | Currently tracked user sessions |

### Endpoints

| URL | Description |
|-----|-------------|
| `http://localhost:8000` | Live dashboard |
| `http://localhost:8000/api/stats` | REST stats endpoint |
| `http://localhost:8000/health` | Health check |
| `http://localhost:9091/metrics` | Producer Prometheus metrics |
| `http://localhost:9092/metrics` | Consumer Prometheus metrics |
| `http://localhost:9094` | Prometheus UI |

## Performance

At the default configuration (100 events/sec producer rate, single consumer):

| Metric | Value |
|--------|-------|
| Producer throughput | 100 events/sec (configurable up to 10K+) |
| Consumer processing | ~5,000 events/sec capacity |
| Postgres batch write | ~2,000 rows/sec (500-row batches) |
| End-to-end latency | < 100ms (producer to dashboard) |
| Memory usage | ~200MB (consumer with active windows) |

The bottleneck is typically the PostgreSQL batch insert. For higher throughput, increase `SINK_BATCH_SIZE` or add consumer instances to the consumer group.

## Testing

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=src --cov-report=term-missing
```

## Project Structure

```
stream-pipeline/
├── README.md                       # This file
├── docker-compose.yml              # Full infrastructure stack
├── Dockerfile                      # Multi-stage Python image
├── pyproject.toml                  # Project metadata and dependencies
├── requirements.txt                # Pinned dependencies
├── src/
│   ├── config.py                   # Pydantic settings (env-driven)
│   ├── producer/
│   │   ├── event_generator.py      # Realistic clickstream data generator
│   │   └── producer.py             # Kafka producer with Avro serialization
│   ├── consumer/
│   │   ├── consumer.py             # Main consumer loop with offset management
│   │   ├── processors.py           # Validate, filter, normalize, enrich
│   │   └── windowing.py            # Tumbling windows and session detection
│   ├── sink/
│   │   ├── postgres_sink.py        # Batched PostgreSQL writer
│   │   └── dead_letter.py          # DLQ for failed events
│   ├── schemas/
│   │   ├── clickstream.avsc        # Avro schema: raw events
│   │   └── aggregated.avsc         # Avro schema: window results
│   ├── dashboard/
│   │   ├── app.py                  # FastAPI + WebSocket server
│   │   ├── templates/index.html    # Dashboard UI
│   │   └── static/dashboard.js     # WebSocket client
│   └── metrics/
│       └── prometheus.py           # Prometheus metric definitions
├── tests/
│   ├── test_processors.py          # Processing chain unit tests
│   ├── test_windowing.py           # Window aggregation tests
│   └── test_dead_letter.py         # DLQ tests with mocked Kafka
├── scripts/
│   ├── setup_topics.sh             # Kafka topic creation
│   └── seed_data.py                # Bulk event seeder for demos
├── sql/
│   ├── init.sql                    # PostgreSQL schema (auto-run)
│   └── queries.sql                 # Example analytics queries
└── monitoring/
    └── prometheus.yml              # Prometheus scrape config
```

## Design Decisions

- **Manual offset commits** (at-least-once): Offsets are committed after processing and writing, not before. This means events may be reprocessed on crash, but never lost. The `ON CONFLICT DO NOTHING` clause in Postgres handles deduplication.
- **Avro + fastavro**: Schema enforcement at the serialization layer catches data issues early. `fastavro` is a fast pure-Python implementation that avoids the heavy JVM-based Confluent serializer.
- **Dict-based windowing**: Simple and transparent for a single-consumer setup. Production systems would use Flink or Kafka Streams with RocksDB state backends.
- **Batch inserts**: Collecting events and writing in bulk (500 rows via `execute_values`) gives 10-50x better throughput than individual inserts.
- **DLQ as a Kafka topic**: Failed events stay in the same ecosystem, can be replayed, and don't require separate infrastructure.

## License

MIT
