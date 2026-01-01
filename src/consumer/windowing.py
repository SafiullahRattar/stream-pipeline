"""
Tumbling window aggregations for real-time clickstream analytics.

Implements two window types using dict-based in-memory state:
1. Events-per-minute: 60-second tumbling window counting events by type
2. Revenue-per-5min: 300-second tumbling window summing purchase revenue

Windows are keyed by their start timestamp (floored to the window boundary).
Expired windows are emitted and evicted after a configurable late-arrival
tolerance period.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from src.config import settings

logger = logging.getLogger(__name__)


@dataclass
class WindowState:
    """Mutable state for a single tumbling window instance."""

    window_start_ms: int
    window_end_ms: int
    event_count: int = 0
    user_ids: set[str] = field(default_factory=set)
    event_type_counts: Counter[str] = field(default_factory=Counter)
    total_revenue_cents: int = 0
    product_counts: Counter[str] = field(default_factory=Counter)
    category_counts: Counter[str] = field(default_factory=Counter)
    emitted: bool = False


class TumblingWindowAggregator:
    """
    Manages tumbling windows for clickstream event aggregation.

    Each event is assigned to a window based on its timestamp. When the
    current time passes the window boundary plus the late-arrival tolerance,
    the window is closed, its aggregation result is emitted, and the state
    is evicted.

    This is a simplified in-memory implementation suitable for a single
    consumer instance. For production multi-partition setups, you would
    use a distributed state store (e.g., RocksDB-backed Kafka Streams
    or Flink state backends).
    """

    def __init__(self, window_size_seconds: int, window_type: str = "events_per_minute") -> None:
        self._window_size_ms = window_size_seconds * 1000
        self._window_type = window_type
        self._tolerance_ms = settings.window.late_arrival_tolerance_seconds * 1000
        self._windows: dict[int, WindowState] = {}

    def _get_window_start(self, event_timestamp_ms: int) -> int:
        """Floor the timestamp to the window boundary."""
        return (event_timestamp_ms // self._window_size_ms) * self._window_size_ms

    def _get_or_create_window(self, window_start_ms: int) -> WindowState:
        """Return the window state, creating it if it doesn't exist."""
        if window_start_ms not in self._windows:
            self._windows[window_start_ms] = WindowState(
                window_start_ms=window_start_ms,
                window_end_ms=window_start_ms + self._window_size_ms,
            )
        return self._windows[window_start_ms]

    def add_event(self, event: dict[str, Any]) -> None:
        """Add a processed event to its corresponding window."""
        event_ts = event["timestamp"]
        window_start = self._get_window_start(event_ts)
        window = self._get_or_create_window(window_start)

        window.event_count += 1
        window.user_ids.add(event["user_id"])
        window.event_type_counts[event["event_type"]] += 1

        # Track revenue for purchase events
        revenue = event.get("revenue_cents", 0)
        if revenue and event["event_type"] == "purchase":
            window.total_revenue_cents += revenue

        # Track product and category popularity
        product_id = event.get("product_id")
        if product_id:
            window.product_counts[product_id] += 1

        category = event.get("product_category")
        if category:
            window.category_counts[category] += 1

    def emit_closed_windows(self) -> list[dict[str, Any]]:
        """
        Check for windows that have passed the late-arrival tolerance
        and emit their aggregation results.

        Returns a list of aggregation result dicts matching the
        AggregatedMetrics Avro schema.
        """
        now_ms = int(time.time() * 1000)
        results: list[dict[str, Any]] = []
        expired_keys: list[int] = []

        for window_start, window in self._windows.items():
            # Window is closeable if current time > window_end + tolerance
            if now_ms >= window.window_end_ms + self._tolerance_ms and not window.emitted:
                result = {
                    "window_id": str(uuid.uuid4()),
                    "window_type": self._window_type,
                    "window_start": window.window_start_ms,
                    "window_end": window.window_end_ms,
                    "event_count": window.event_count,
                    "unique_users": len(window.user_ids),
                    "page_views": window.event_type_counts.get("page_view", 0),
                    "add_to_carts": window.event_type_counts.get("add_to_cart", 0),
                    "purchases": window.event_type_counts.get("purchase", 0),
                    "total_revenue_cents": window.total_revenue_cents,
                    "top_products": [
                        pid for pid, _ in window.product_counts.most_common(10)
                    ],
                    "top_categories": [
                        cat for cat, _ in window.category_counts.most_common(5)
                    ],
                    "emitted_at": now_ms,
                }
                results.append(result)
                window.emitted = True
                expired_keys.append(window_start)

                logger.info(
                    "Window [%s] closed: %d events, %d users, $%.2f revenue",
                    self._window_type,
                    window.event_count,
                    len(window.user_ids),
                    window.total_revenue_cents / 100.0,
                )

        # Evict emitted windows to prevent memory growth
        for key in expired_keys:
            del self._windows[key]

        return results

    @property
    def active_window_count(self) -> int:
        """Number of currently open windows."""
        return len(self._windows)


class SessionDetector:
    """
    Detects user sessions by grouping events with a configurable inactivity timeout.

    A session starts on the first event from a user_id and ends when no new
    events arrive within the timeout period. Session summaries are emitted
    when a session expires.
    """

    @dataclass
    class Session:
        user_id: str
        session_id: str
        start_ms: int
        last_event_ms: int
        event_count: int = 0
        event_types: Counter[str] = field(default_factory=Counter)
        revenue_cents: int = 0
        pages_visited: set[str] = field(default_factory=set)

    def __init__(self) -> None:
        self._timeout_ms = settings.window.session_timeout_seconds * 1000
        self._sessions: dict[str, SessionDetector.Session] = {}

    def track_event(self, event: dict[str, Any]) -> None:
        """Update session state with a new event."""
        user_id = event["user_id"]
        event_ts = event["timestamp"]
        session_key = f"{user_id}:{event.get('session_id', '')}"

        session = self._sessions.get(session_key)

        if session is None or (event_ts - session.last_event_ms) > self._timeout_ms:
            session = self.Session(
                user_id=user_id,
                session_id=event.get("session_id", "unknown"),
                start_ms=event_ts,
                last_event_ms=event_ts,
            )
            self._sessions[session_key] = session

        session.last_event_ms = event_ts
        session.event_count += 1
        session.event_types[event["event_type"]] += 1
        session.revenue_cents += event.get("revenue_cents", 0)
        session.pages_visited.add(event.get("page_url", ""))

    def emit_expired_sessions(self) -> list[dict[str, Any]]:
        """Emit summaries for sessions that have timed out."""
        now_ms = int(time.time() * 1000)
        results: list[dict[str, Any]] = []
        expired_keys: list[str] = []

        for key, session in self._sessions.items():
            if (now_ms - session.last_event_ms) > self._timeout_ms:
                results.append({
                    "window_id": str(uuid.uuid4()),
                    "window_type": "session_summary",
                    "window_start": session.start_ms,
                    "window_end": session.last_event_ms,
                    "event_count": session.event_count,
                    "unique_users": 1,
                    "page_views": session.event_types.get("page_view", 0),
                    "add_to_carts": session.event_types.get("add_to_cart", 0),
                    "purchases": session.event_types.get("purchase", 0),
                    "total_revenue_cents": session.revenue_cents,
                    "top_products": [],
                    "top_categories": [],
                    "emitted_at": now_ms,
                })
                expired_keys.append(key)

        for key in expired_keys:
            del self._sessions[key]

        return results

    @property
    def active_session_count(self) -> int:
        return len(self._sessions)
