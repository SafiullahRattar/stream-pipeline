"""
Stream processing transformations for clickstream events.

Each processor is a pure function that takes a deserialized event dict and
returns either a transformed event or None (to filter it out). This design
makes processors easy to unit test and compose into a processing chain.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Valid event types per the Avro schema
VALID_EVENT_TYPES = frozenset({
    "page_view", "add_to_cart", "remove_from_cart",
    "purchase", "search", "wishlist",
})

REQUIRED_FIELDS = frozenset({
    "event_id", "event_type", "timestamp", "user_id",
    "session_id", "page_url", "user_agent", "ip_address",
    "geo_country",
})


class ValidationError(Exception):
    """Raised when an event fails validation and should be routed to the DLQ."""

    def __init__(self, event: dict[str, Any], reason: str) -> None:
        self.event = event
        self.reason = reason
        super().__init__(reason)


def validate_event(event: dict[str, Any]) -> dict[str, Any]:
    """
    Validate that an event has all required fields and sensible values.

    Raises ValidationError for events that should go to the DLQ.
    """
    # Check required fields
    missing = REQUIRED_FIELDS - set(event.keys())
    if missing:
        raise ValidationError(event, f"Missing required fields: {sorted(missing)}")

    # Check event type
    if event.get("event_type") not in VALID_EVENT_TYPES:
        raise ValidationError(
            event,
            f"Invalid event_type: {event.get('event_type')}",
        )

    # Check timestamp is positive
    ts = event.get("timestamp")
    if not isinstance(ts, (int, float)) or ts <= 0:
        raise ValidationError(event, f"Invalid timestamp: {ts}")

    # Check price is non-negative for purchase events
    if event["event_type"] == "purchase":
        price = event.get("price_cents")
        qty = event.get("quantity")
        if price is not None and price < 0:
            raise ValidationError(event, f"Negative price_cents: {price}")
        if qty is not None and qty <= 0:
            raise ValidationError(event, f"Invalid quantity: {qty}")

    return event


def enrich_event(event: dict[str, Any]) -> dict[str, Any]:
    """
    Enrich an event with derived fields useful for analytics.

    Adds:
    - revenue_cents: computed total for purchase events
    - event_hour: hour of day (0-23) for time-of-day analysis
    - is_mobile: boolean derived from user_agent
    """
    enriched = dict(event)

    # Compute revenue for purchase events
    if enriched["event_type"] == "purchase":
        price = enriched.get("price_cents") or 0
        qty = enriched.get("quantity") or 0
        enriched["revenue_cents"] = price * qty
    else:
        enriched["revenue_cents"] = 0

    # Extract hour of day from timestamp
    ts_seconds = enriched["timestamp"] / 1000.0
    import datetime
    dt = datetime.datetime.fromtimestamp(ts_seconds, tz=datetime.timezone.utc)
    enriched["event_hour"] = dt.hour

    # Simple mobile detection
    ua = enriched.get("user_agent", "").lower()
    enriched["is_mobile"] = any(
        token in ua for token in ("mobile", "iphone", "android", "ipad")
    )

    return enriched


def filter_bot_traffic(event: dict[str, Any]) -> dict[str, Any] | None:
    """
    Filter out events from known bots and crawlers.

    Returns None for bot traffic, which removes it from the processing pipeline.
    """
    ua = event.get("user_agent", "").lower()
    bot_signatures = ("bot", "crawler", "spider", "scrapy", "curl", "wget", "httpx")
    if any(sig in ua for sig in bot_signatures):
        logger.debug("Filtered bot event: %s (ua=%s)", event.get("event_id"), ua[:50])
        return None
    return event


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize fields for consistency before storage.

    - Lowercases geo_country
    - Strips whitespace from string fields
    - Sets default geo_country if missing
    """
    normalized = dict(event)

    if normalized.get("geo_country"):
        normalized["geo_country"] = normalized["geo_country"].upper().strip()

    if normalized.get("search_query"):
        normalized["search_query"] = normalized["search_query"].strip().lower()

    if not normalized.get("geo_country"):
        normalized["geo_country"] = "UNKNOWN"

    return normalized


def process_event(event: dict[str, Any]) -> dict[str, Any]:
    """
    Run the full processing chain on a single event.

    Pipeline: validate -> filter bots -> normalize -> enrich

    Raises ValidationError if the event is malformed.
    Returns the fully processed event, or raises ValidationError for DLQ routing.
    """
    # Step 1: validate (may raise ValidationError)
    event = validate_event(event)

    # Step 2: filter bots
    result = filter_bot_traffic(event)
    if result is None:
        raise ValidationError(event, "Filtered as bot traffic")

    # Step 3: normalize
    event = normalize_event(result)

    # Step 4: enrich
    event = enrich_event(event)

    return event
