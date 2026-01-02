-- ============================================================================
-- Stream Pipeline: Example Analytics Queries
--
-- Useful queries for analyzing the clickstream data stored in PostgreSQL.
-- Run these against the pipeline database to explore the data.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Events per minute over the last hour
-- ---------------------------------------------------------------------------
SELECT
    TO_TIMESTAMP(event_timestamp_ms / 1000)::timestamp AT TIME ZONE 'UTC' AS event_minute,
    COUNT(*)                    AS event_count,
    COUNT(DISTINCT user_id)     AS unique_users
FROM clickstream_events
WHERE event_timestamp_ms > (EXTRACT(EPOCH FROM NOW()) * 1000 - 3600000)::BIGINT
GROUP BY 1
ORDER BY 1 DESC;

-- ---------------------------------------------------------------------------
-- 2. Revenue by product category (last 24 hours)
-- ---------------------------------------------------------------------------
SELECT
    product_category,
    COUNT(*)                                AS purchase_count,
    SUM(revenue_cents) / 100.0              AS total_revenue_dollars,
    AVG(revenue_cents) / 100.0              AS avg_order_value_dollars
FROM clickstream_events
WHERE event_type = 'purchase'
  AND event_timestamp_ms > (EXTRACT(EPOCH FROM NOW()) * 1000 - 86400000)::BIGINT
GROUP BY product_category
ORDER BY total_revenue_dollars DESC;

-- ---------------------------------------------------------------------------
-- 3. Conversion funnel: page_view -> add_to_cart -> purchase
-- ---------------------------------------------------------------------------
SELECT
    'page_view'    AS stage, COUNT(*) FILTER (WHERE event_type = 'page_view')    AS count,
    ROUND(100.0, 1) AS pct
FROM clickstream_events
UNION ALL
SELECT
    'add_to_cart', COUNT(*) FILTER (WHERE event_type = 'add_to_cart'),
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE event_type = 'add_to_cart') /
        NULLIF(COUNT(*) FILTER (WHERE event_type = 'page_view'), 0),
        1
    )
FROM clickstream_events
UNION ALL
SELECT
    'purchase', COUNT(*) FILTER (WHERE event_type = 'purchase'),
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE event_type = 'purchase') /
        NULLIF(COUNT(*) FILTER (WHERE event_type = 'page_view'), 0),
        1
    )
FROM clickstream_events;

-- ---------------------------------------------------------------------------
-- 4. Top 10 products by interaction count
-- ---------------------------------------------------------------------------
SELECT
    product_id,
    product_category,
    COUNT(*)                                        AS interactions,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    SUM(revenue_cents) / 100.0                      AS revenue_dollars
FROM clickstream_events
WHERE product_id IS NOT NULL
GROUP BY product_id, product_category
ORDER BY interactions DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- 5. Session analysis: average session length and depth
-- ---------------------------------------------------------------------------
SELECT
    session_id,
    user_id,
    COUNT(*)                                            AS events_in_session,
    (MAX(event_timestamp_ms) - MIN(event_timestamp_ms)) / 1000.0
                                                        AS session_duration_seconds,
    COUNT(DISTINCT page_url)                            AS unique_pages,
    BOOL_OR(event_type = 'purchase')                    AS converted
FROM clickstream_events
GROUP BY session_id, user_id
HAVING COUNT(*) > 1
ORDER BY session_duration_seconds DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 6. Geographic distribution of events
-- ---------------------------------------------------------------------------
SELECT
    geo_country,
    geo_city,
    COUNT(*)                    AS events,
    COUNT(DISTINCT user_id)     AS unique_users,
    SUM(revenue_cents) / 100.0  AS revenue_dollars
FROM clickstream_events
GROUP BY geo_country, geo_city
ORDER BY events DESC
LIMIT 15;

-- ---------------------------------------------------------------------------
-- 7. Mobile vs desktop conversion rate
-- ---------------------------------------------------------------------------
SELECT
    CASE WHEN is_mobile THEN 'Mobile' ELSE 'Desktop' END AS platform,
    COUNT(*)                                               AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'purchase')        AS purchases,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE event_type = 'purchase') /
        NULLIF(COUNT(*), 0), 2
    )                                                      AS purchase_rate_pct
FROM clickstream_events
GROUP BY is_mobile;

-- ---------------------------------------------------------------------------
-- 8. Hourly activity pattern
-- ---------------------------------------------------------------------------
SELECT
    event_hour,
    COUNT(*)                    AS events,
    COUNT(DISTINCT user_id)     AS unique_users,
    SUM(revenue_cents) / 100.0  AS revenue_dollars
FROM clickstream_events
GROUP BY event_hour
ORDER BY event_hour;

-- ---------------------------------------------------------------------------
-- 9. Dead letter queue inspection
-- ---------------------------------------------------------------------------
SELECT
    error_reason,
    COUNT(*)                AS error_count,
    MIN(TO_TIMESTAMP(failed_at / 1000)) AS first_seen,
    MAX(TO_TIMESTAMP(failed_at / 1000)) AS last_seen
FROM dead_letter_events
GROUP BY error_reason
ORDER BY error_count DESC;

-- ---------------------------------------------------------------------------
-- 10. Windowed aggregation history
-- ---------------------------------------------------------------------------
SELECT
    window_type,
    TO_TIMESTAMP(window_start / 1000) AS window_start_ts,
    TO_TIMESTAMP(window_end / 1000)   AS window_end_ts,
    event_count,
    unique_users,
    purchases,
    total_revenue_cents / 100.0       AS revenue_dollars,
    top_categories[1:3]               AS top_3_categories
FROM aggregated_metrics
ORDER BY window_start DESC
LIMIT 20;
