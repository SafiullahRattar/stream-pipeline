-- ============================================================================
-- Stream Pipeline: PostgreSQL Schema Initialization
--
-- Creates the tables and indexes needed by the clickstream pipeline.
-- This script is run automatically when the Postgres container starts.
-- ============================================================================

-- Main events table: stores all processed clickstream events
CREATE TABLE IF NOT EXISTS clickstream_events (
    event_id            VARCHAR(36)     PRIMARY KEY,
    event_type          VARCHAR(20)     NOT NULL,
    event_timestamp_ms  BIGINT          NOT NULL,
    user_id             VARCHAR(50)     NOT NULL,
    session_id          VARCHAR(36)     NOT NULL,
    product_id          VARCHAR(50),
    product_category    VARCHAR(50),
    price_cents         BIGINT,
    quantity            INTEGER,
    revenue_cents       BIGINT          DEFAULT 0,
    search_query        TEXT,
    page_url            TEXT            NOT NULL,
    referrer            TEXT,
    user_agent          TEXT,
    ip_address          VARCHAR(45),
    geo_country         VARCHAR(5),
    geo_city            VARCHAR(100),
    event_hour          SMALLINT,
    is_mobile           BOOLEAN,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for time-range queries (most common analytics pattern)
CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON clickstream_events (event_timestamp_ms DESC);

-- Index for user-level queries
CREATE INDEX IF NOT EXISTS idx_events_user_id
    ON clickstream_events (user_id, event_timestamp_ms DESC);

-- Index for event type filtering
CREATE INDEX IF NOT EXISTS idx_events_type
    ON clickstream_events (event_type);

-- Index for product analytics
CREATE INDEX IF NOT EXISTS idx_events_product
    ON clickstream_events (product_id)
    WHERE product_id IS NOT NULL;

-- Index for geographic analysis
CREATE INDEX IF NOT EXISTS idx_events_geo
    ON clickstream_events (geo_country, geo_city);

-- Composite index for session analysis
CREATE INDEX IF NOT EXISTS idx_events_session
    ON clickstream_events (session_id, event_timestamp_ms);

-- ============================================================================
-- Aggregated metrics table: stores window aggregation results
-- ============================================================================

CREATE TABLE IF NOT EXISTS aggregated_metrics (
    window_id           VARCHAR(36)     PRIMARY KEY,
    window_type         VARCHAR(30)     NOT NULL,
    window_start        BIGINT          NOT NULL,
    window_end          BIGINT          NOT NULL,
    event_count         BIGINT          NOT NULL,
    unique_users        BIGINT          NOT NULL,
    page_views          BIGINT          DEFAULT 0,
    add_to_carts        BIGINT          DEFAULT 0,
    purchases           BIGINT          DEFAULT 0,
    total_revenue_cents BIGINT          DEFAULT 0,
    top_products        TEXT[],
    top_categories      TEXT[],
    emitted_at          BIGINT          NOT NULL,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agg_window_type_start
    ON aggregated_metrics (window_type, window_start DESC);

-- ============================================================================
-- Dead letter queue audit table: optional Postgres mirror of DLQ
-- ============================================================================

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id                  SERIAL          PRIMARY KEY,
    original_payload    TEXT,
    error_reason        TEXT            NOT NULL,
    source_topic        VARCHAR(100),
    source_partition    INTEGER,
    source_offset       BIGINT,
    failed_at           BIGINT,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
