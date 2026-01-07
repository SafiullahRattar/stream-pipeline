"""
Unit tests for the stream processing pipeline.

Tests the validation, enrichment, filtering, and normalization processors
in isolation using synthetic event data.
"""

from __future__ import annotations

import time
import uuid

import pytest

from src.consumer.processors import (
    ValidationError,
    enrich_event,
    filter_bot_traffic,
    normalize_event,
    process_event,
    validate_event,
)


def _make_event(**overrides: object) -> dict:
    """Factory for a valid clickstream event with optional overrides."""
    base = {
        "event_id": str(uuid.uuid4()),
        "event_type": "page_view",
        "timestamp": int(time.time() * 1000),
        "user_id": "user_000001",
        "session_id": str(uuid.uuid4()),
        "product_id": "prod_00001",
        "product_category": "Electronics",
        "price_cents": 1999,
        "quantity": 1,
        "search_query": None,
        "page_url": "https://shop.example.com/products/prod_00001",
        "referrer": "https://www.google.com/search",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0",
        "ip_address": "192.168.1.100",
        "geo_country": "US",
        "geo_city": "New York",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# validate_event tests
# ---------------------------------------------------------------------------

class TestValidateEvent:
    def test_valid_event_passes(self) -> None:
        event = _make_event()
        result = validate_event(event)
        assert result["event_id"] == event["event_id"]

    def test_missing_required_field_raises(self) -> None:
        event = _make_event()
        del event["user_id"]
        with pytest.raises(ValidationError, match="Missing required fields"):
            validate_event(event)

    def test_invalid_event_type_raises(self) -> None:
        event = _make_event(event_type="INVALID")
        with pytest.raises(ValidationError, match="Invalid event_type"):
            validate_event(event)

    def test_negative_timestamp_raises(self) -> None:
        event = _make_event(timestamp=-100)
        with pytest.raises(ValidationError, match="Invalid timestamp"):
            validate_event(event)

    def test_zero_timestamp_raises(self) -> None:
        event = _make_event(timestamp=0)
        with pytest.raises(ValidationError, match="Invalid timestamp"):
            validate_event(event)

    def test_purchase_negative_price_raises(self) -> None:
        event = _make_event(event_type="purchase", price_cents=-500, quantity=1)
        with pytest.raises(ValidationError, match="Negative price_cents"):
            validate_event(event)

    def test_purchase_zero_quantity_raises(self) -> None:
        event = _make_event(event_type="purchase", price_cents=1000, quantity=0)
        with pytest.raises(ValidationError, match="Invalid quantity"):
            validate_event(event)

    def test_purchase_valid_passes(self) -> None:
        event = _make_event(event_type="purchase", price_cents=5000, quantity=2)
        result = validate_event(event)
        assert result["event_type"] == "purchase"

    def test_all_valid_event_types_pass(self) -> None:
        for etype in ("page_view", "add_to_cart", "remove_from_cart",
                       "purchase", "search", "wishlist"):
            event = _make_event(event_type=etype)
            if etype == "purchase":
                event["price_cents"] = 1000
                event["quantity"] = 1
            result = validate_event(event)
            assert result["event_type"] == etype


# ---------------------------------------------------------------------------
# enrich_event tests
# ---------------------------------------------------------------------------

class TestEnrichEvent:
    def test_purchase_revenue_calculation(self) -> None:
        event = _make_event(event_type="purchase", price_cents=2500, quantity=3)
        enriched = enrich_event(event)
        assert enriched["revenue_cents"] == 7500

    def test_non_purchase_has_zero_revenue(self) -> None:
        event = _make_event(event_type="page_view")
        enriched = enrich_event(event)
        assert enriched["revenue_cents"] == 0

    def test_event_hour_extracted(self) -> None:
        event = _make_event()
        enriched = enrich_event(event)
        assert 0 <= enriched["event_hour"] <= 23

    def test_mobile_detection_iphone(self) -> None:
        event = _make_event(
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X)"
        )
        enriched = enrich_event(event)
        assert enriched["is_mobile"] is True

    def test_mobile_detection_android(self) -> None:
        event = _make_event(
            user_agent="Mozilla/5.0 (Linux; Android 14) Chrome/121.0.0.0 Mobile"
        )
        enriched = enrich_event(event)
        assert enriched["is_mobile"] is True

    def test_desktop_detection(self) -> None:
        event = _make_event(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0"
        )
        enriched = enrich_event(event)
        assert enriched["is_mobile"] is False

    def test_enrichment_preserves_original_fields(self) -> None:
        event = _make_event()
        enriched = enrich_event(event)
        assert enriched["event_id"] == event["event_id"]
        assert enriched["user_id"] == event["user_id"]
        assert enriched["page_url"] == event["page_url"]


# ---------------------------------------------------------------------------
# filter_bot_traffic tests
# ---------------------------------------------------------------------------

class TestFilterBotTraffic:
    def test_normal_user_passes(self) -> None:
        event = _make_event()
        result = filter_bot_traffic(event)
        assert result is not None

    def test_bot_user_agent_filtered(self) -> None:
        event = _make_event(user_agent="Googlebot/2.1 (+http://www.google.com/bot.html)")
        result = filter_bot_traffic(event)
        assert result is None

    def test_crawler_filtered(self) -> None:
        event = _make_event(user_agent="Mozilla/5.0 (compatible; YandexBot/3.0)")
        result = filter_bot_traffic(event)
        assert result is None

    def test_curl_filtered(self) -> None:
        event = _make_event(user_agent="curl/7.88.1")
        result = filter_bot_traffic(event)
        assert result is None

    def test_empty_user_agent_passes(self) -> None:
        event = _make_event(user_agent="")
        result = filter_bot_traffic(event)
        assert result is not None


# ---------------------------------------------------------------------------
# normalize_event tests
# ---------------------------------------------------------------------------

class TestNormalizeEvent:
    def test_country_uppercased(self) -> None:
        event = _make_event(geo_country="us")
        result = normalize_event(event)
        assert result["geo_country"] == "US"

    def test_search_query_lowercased_and_stripped(self) -> None:
        event = _make_event(search_query="  Running SHOES  ")
        result = normalize_event(event)
        assert result["search_query"] == "running shoes"

    def test_missing_country_defaults_to_unknown(self) -> None:
        event = _make_event(geo_country="")
        result = normalize_event(event)
        assert result["geo_country"] == "UNKNOWN"

    def test_none_country_defaults_to_unknown(self) -> None:
        event = _make_event(geo_country=None)
        result = normalize_event(event)
        assert result["geo_country"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# Full pipeline tests
# ---------------------------------------------------------------------------

class TestProcessEvent:
    def test_full_pipeline_valid_event(self) -> None:
        event = _make_event()
        result = process_event(event)
        assert "revenue_cents" in result
        assert "event_hour" in result
        assert "is_mobile" in result

    def test_full_pipeline_malformed_raises(self) -> None:
        event = {"event_id": str(uuid.uuid4()), "event_type": "page_view"}
        with pytest.raises(ValidationError):
            process_event(event)

    def test_full_pipeline_bot_raises_validation_error(self) -> None:
        event = _make_event(user_agent="Googlebot/2.1")
        with pytest.raises(ValidationError, match="bot traffic"):
            process_event(event)

    def test_full_pipeline_purchase_enriched(self) -> None:
        event = _make_event(event_type="purchase", price_cents=9999, quantity=2)
        result = process_event(event)
        assert result["revenue_cents"] == 19998
        assert result["geo_country"] == "US"
