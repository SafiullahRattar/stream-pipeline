"""
Unit tests for the tumbling window aggregator and session detector.

Tests window assignment, aggregation correctness, window closure/eviction,
and late arrival handling using controlled timestamps.
"""

from __future__ import annotations

import time
import uuid
from unittest.mock import patch

import pytest

from src.consumer.windowing import SessionDetector, TumblingWindowAggregator


def _make_processed_event(
    event_type: str = "page_view",
    timestamp_ms: int | None = None,
    user_id: str = "user_000001",
    product_id: str = "prod_00001",
    product_category: str = "Electronics",
    revenue_cents: int = 0,
) -> dict:
    """Factory for a processed event (post-enrichment)."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": timestamp_ms or int(time.time() * 1000),
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "product_id": product_id,
        "product_category": product_category,
        "price_cents": 2000,
        "quantity": 1,
        "revenue_cents": revenue_cents,
        "page_url": "https://shop.example.com/products/prod_00001",
        "user_agent": "Mozilla/5.0",
        "ip_address": "192.168.1.1",
        "geo_country": "US",
        "event_hour": 14,
        "is_mobile": False,
    }


# ---------------------------------------------------------------------------
# TumblingWindowAggregator tests
# ---------------------------------------------------------------------------

class TestTumblingWindowAggregator:
    def test_single_event_creates_window(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60)
        event = _make_processed_event()
        agg.add_event(event)
        assert agg.active_window_count == 1

    def test_events_in_same_window_aggregated(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60)
        base_ts = 1700000000000  # A fixed timestamp

        for i in range(5):
            event = _make_processed_event(
                timestamp_ms=base_ts + i * 1000,  # 1 second apart
                user_id=f"user_{i:06d}",
            )
            agg.add_event(event)

        assert agg.active_window_count == 1

    def test_events_in_different_windows(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60)
        base_ts = 1700000000000

        # Event in window 1
        agg.add_event(_make_processed_event(timestamp_ms=base_ts))

        # Event in window 2 (61 seconds later)
        agg.add_event(_make_processed_event(timestamp_ms=base_ts + 61_000))

        assert agg.active_window_count == 2

    def test_window_emits_on_closure(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60, window_type="events_per_minute")

        # Use a timestamp far in the past so the window is already expired
        old_ts = int((time.time() - 300) * 1000)  # 5 minutes ago
        agg.add_event(_make_processed_event(timestamp_ms=old_ts))

        results = agg.emit_closed_windows()
        assert len(results) == 1
        assert results[0]["window_type"] == "events_per_minute"
        assert results[0]["event_count"] == 1
        assert results[0]["unique_users"] == 1

    def test_emitted_window_evicted(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60)
        old_ts = int((time.time() - 300) * 1000)
        agg.add_event(_make_processed_event(timestamp_ms=old_ts))

        agg.emit_closed_windows()
        assert agg.active_window_count == 0

    def test_revenue_aggregation(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=300)
        old_ts = int((time.time() - 600) * 1000)

        # 3 purchases with different revenues
        for rev in [1000, 2500, 5000]:
            agg.add_event(_make_processed_event(
                event_type="purchase",
                timestamp_ms=old_ts,
                revenue_cents=rev,
            ))

        results = agg.emit_closed_windows()
        assert len(results) == 1
        assert results[0]["total_revenue_cents"] == 8500
        assert results[0]["purchases"] == 3

    def test_event_type_counting(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60)
        old_ts = int((time.time() - 300) * 1000)

        agg.add_event(_make_processed_event(event_type="page_view", timestamp_ms=old_ts))
        agg.add_event(_make_processed_event(event_type="page_view", timestamp_ms=old_ts))
        agg.add_event(_make_processed_event(event_type="add_to_cart", timestamp_ms=old_ts))
        agg.add_event(_make_processed_event(event_type="purchase", timestamp_ms=old_ts, revenue_cents=1000))

        results = agg.emit_closed_windows()
        assert results[0]["page_views"] == 2
        assert results[0]["add_to_carts"] == 1
        assert results[0]["purchases"] == 1
        assert results[0]["event_count"] == 4

    def test_unique_users_counted(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60)
        old_ts = int((time.time() - 300) * 1000)

        # Same user appears twice, different user once
        agg.add_event(_make_processed_event(user_id="user_A", timestamp_ms=old_ts))
        agg.add_event(_make_processed_event(user_id="user_A", timestamp_ms=old_ts))
        agg.add_event(_make_processed_event(user_id="user_B", timestamp_ms=old_ts))

        results = agg.emit_closed_windows()
        assert results[0]["unique_users"] == 2

    def test_top_products_tracked(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60)
        old_ts = int((time.time() - 300) * 1000)

        for _ in range(5):
            agg.add_event(_make_processed_event(product_id="popular", timestamp_ms=old_ts))
        agg.add_event(_make_processed_event(product_id="rare", timestamp_ms=old_ts))

        results = agg.emit_closed_windows()
        assert results[0]["top_products"][0] == "popular"

    def test_current_window_not_emitted(self) -> None:
        agg = TumblingWindowAggregator(window_size_seconds=60)
        # Event right now should stay open
        agg.add_event(_make_processed_event())
        results = agg.emit_closed_windows()
        assert len(results) == 0
        assert agg.active_window_count == 1


# ---------------------------------------------------------------------------
# SessionDetector tests
# ---------------------------------------------------------------------------

class TestSessionDetector:
    def test_new_event_creates_session(self) -> None:
        detector = SessionDetector()
        event = _make_processed_event()
        detector.track_event(event)
        assert detector.active_session_count == 1

    def test_same_user_same_session(self) -> None:
        detector = SessionDetector()
        sid = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)

        for i in range(5):
            event = _make_processed_event(
                user_id="user_A",
                timestamp_ms=now_ms + i * 1000,
            )
            event["session_id"] = sid
            detector.track_event(event)

        assert detector.active_session_count == 1

    def test_different_users_different_sessions(self) -> None:
        detector = SessionDetector()
        for uid in ["user_A", "user_B", "user_C"]:
            event = _make_processed_event(user_id=uid)
            detector.track_event(event)

        assert detector.active_session_count == 3

    @patch("src.consumer.windowing.settings")
    def test_expired_session_emitted(self, mock_settings: object) -> None:
        # Patch session timeout to 1 second for testing
        mock_settings.window.session_timeout_seconds = 1

        detector = SessionDetector()
        detector._timeout_ms = 1000  # Override timeout

        old_ts = int((time.time() - 10) * 1000)  # 10 seconds ago
        event = _make_processed_event(timestamp_ms=old_ts)
        detector.track_event(event)

        results = detector.emit_expired_sessions()
        assert len(results) == 1
        assert results[0]["window_type"] == "session_summary"
        assert results[0]["event_count"] == 1

    def test_active_session_not_emitted(self) -> None:
        detector = SessionDetector()
        event = _make_processed_event()
        detector.track_event(event)

        results = detector.emit_expired_sessions()
        assert len(results) == 0

    @patch("src.consumer.windowing.settings")
    def test_session_revenue_tracking(self, mock_settings: object) -> None:
        mock_settings.window.session_timeout_seconds = 1
        detector = SessionDetector()
        detector._timeout_ms = 1000

        old_ts = int((time.time() - 10) * 1000)
        sid = str(uuid.uuid4())

        for rev in [1000, 2000, 3000]:
            event = _make_processed_event(
                event_type="purchase",
                timestamp_ms=old_ts,
                revenue_cents=rev,
            )
            event["session_id"] = sid
            detector.track_event(event)

        results = detector.emit_expired_sessions()
        assert results[0]["total_revenue_cents"] == 6000
