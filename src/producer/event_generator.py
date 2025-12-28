"""
Realistic e-commerce clickstream event generator.

Simulates user behavior with weighted event types, realistic product catalogs,
session continuity, and geographic distribution. Intentionally injects a small
percentage of malformed events to exercise the dead letter queue.
"""

from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from src.config import settings

# ---------------------------------------------------------------------------
# Static catalogs
# ---------------------------------------------------------------------------

CATEGORIES: list[str] = [
    "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
    "Toys", "Beauty", "Automotive", "Grocery", "Health",
]

SEARCH_QUERIES: list[str] = [
    "wireless headphones", "running shoes", "yoga mat", "laptop stand",
    "coffee maker", "winter jacket", "protein powder", "desk lamp",
    "mechanical keyboard", "water bottle", "backpack", "sunglasses",
    "phone case", "bluetooth speaker", "gaming mouse",
]

USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 Safari/17.2",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/121.0.0.0 Mobile",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/121.0.0.0",
    "Mozilla/5.0 (iPad; CPU OS 17_3 like Mac OS X) AppleWebKit/605.1.15",
]

COUNTRIES: list[tuple[str, str, float]] = [
    ("US", "New York", 0.35),
    ("US", "San Francisco", 0.10),
    ("US", "Chicago", 0.08),
    ("US", "Austin", 0.05),
    ("GB", "London", 0.10),
    ("DE", "Berlin", 0.07),
    ("CA", "Toronto", 0.06),
    ("FR", "Paris", 0.05),
    ("AU", "Sydney", 0.04),
    ("JP", "Tokyo", 0.05),
    ("IN", "Mumbai", 0.05),
]

REFERRERS: list[str | None] = [
    None,
    "https://www.google.com/search",
    "https://www.facebook.com",
    "https://twitter.com",
    "https://www.reddit.com",
    "https://news.ycombinator.com",
    None,
    None,
]

# Event type weights simulate a realistic funnel:
# many page views, fewer add-to-carts, even fewer purchases
EVENT_TYPE_WEIGHTS: dict[str, float] = {
    "page_view": 0.50,
    "search": 0.15,
    "add_to_cart": 0.15,
    "remove_from_cart": 0.05,
    "wishlist": 0.05,
    "purchase": 0.10,
}

PAGE_TEMPLATES: list[str] = [
    "/products/{product_id}",
    "/category/{category}",
    "/search?q={query}",
    "/cart",
    "/checkout",
    "/",
    "/deals",
    "/account/orders",
]


@dataclass
class Product:
    """A product in the simulated catalog."""

    product_id: str
    category: str
    price_cents: int


@dataclass
class UserSession:
    """Tracks an active user session for realistic event sequences."""

    user_id: str
    session_id: str
    user_agent: str
    geo_country: str
    geo_city: str | None
    ip_address: str
    last_active: float = field(default_factory=time.time)
    cart: list[str] = field(default_factory=list)


class EventGenerator:
    """
    Generates realistic clickstream events at a configurable rate.

    Maintains a pool of active user sessions and a product catalog.
    Sessions expire after the configured timeout, simulating real user behavior.
    """

    def __init__(self) -> None:
        self._config = settings.producer
        self._products = self._build_catalog()
        self._active_sessions: dict[str, UserSession] = {}
        self._user_ids = [f"user_{i:06d}" for i in range(self._config.num_users)]
        self._event_types = list(EVENT_TYPE_WEIGHTS.keys())
        self._event_weights = list(EVENT_TYPE_WEIGHTS.values())

    def _build_catalog(self) -> list[Product]:
        """Generate a deterministic product catalog."""
        rng = random.Random(42)
        products: list[Product] = []
        for i in range(self._config.num_products):
            products.append(
                Product(
                    product_id=f"prod_{i:05d}",
                    category=rng.choice(CATEGORIES),
                    price_cents=rng.choice([
                        rng.randint(499, 2999),       # low-price items
                        rng.randint(2999, 9999),       # mid-price
                        rng.randint(9999, 49999),      # high-price
                        rng.randint(49999, 199999),    # premium
                    ]),
                )
            )
        return products

    def _get_or_create_session(self, user_id: str) -> UserSession:
        """Return an existing session or create a new one."""
        session = self._active_sessions.get(user_id)
        now = time.time()

        if session is None or (now - session.last_active) > settings.window.session_timeout_seconds:
            geo = random.choices(COUNTRIES, weights=[g[2] for g in COUNTRIES], k=1)[0]
            session = UserSession(
                user_id=user_id,
                session_id=str(uuid.uuid4()),
                user_agent=random.choice(USER_AGENTS),
                geo_country=geo[0],
                geo_city=geo[1],
                ip_address=f"{random.randint(1,255)}.{random.randint(0,255)}."
                           f"{random.randint(0,255)}.{random.randint(1,254)}",
            )
            self._active_sessions[user_id] = session

        session.last_active = now
        return session

    def _pick_product(self) -> Product:
        return random.choice(self._products)

    def generate_event(self) -> dict[str, Any]:
        """
        Generate a single clickstream event.

        Returns a dictionary matching the ClickstreamEvent Avro schema.
        A small fraction of events are intentionally malformed to test the DLQ.
        """
        # Intentionally produce malformed events at the configured error rate
        if random.random() < self._config.error_rate:
            return self._generate_malformed_event()

        user_id = random.choice(self._user_ids)
        session = self._get_or_create_session(user_id)
        event_type: str = random.choices(self._event_types, self._event_weights, k=1)[0]
        product = self._pick_product()
        now_ms = int(time.time() * 1000)

        event: dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": now_ms,
            "user_id": session.user_id,
            "session_id": session.session_id,
            "product_id": None,
            "product_category": None,
            "price_cents": None,
            "quantity": None,
            "search_query": None,
            "page_url": f"https://shop.example.com/",
            "referrer": random.choice(REFERRERS),
            "user_agent": session.user_agent,
            "ip_address": session.ip_address,
            "geo_country": session.geo_country,
            "geo_city": session.geo_city,
        }

        # Enrich based on event type
        if event_type == "page_view":
            event["product_id"] = product.product_id
            event["product_category"] = product.category
            event["price_cents"] = product.price_cents
            event["page_url"] = f"https://shop.example.com/products/{product.product_id}"

        elif event_type == "search":
            query = random.choice(SEARCH_QUERIES)
            event["search_query"] = query
            event["page_url"] = f"https://shop.example.com/search?q={query.replace(' ', '+')}"

        elif event_type in ("add_to_cart", "wishlist"):
            event["product_id"] = product.product_id
            event["product_category"] = product.category
            event["price_cents"] = product.price_cents
            event["quantity"] = random.randint(1, 3)
            event["page_url"] = f"https://shop.example.com/products/{product.product_id}"
            if event_type == "add_to_cart":
                session.cart.append(product.product_id)

        elif event_type == "remove_from_cart":
            if session.cart:
                removed = session.cart.pop()
                event["product_id"] = removed
            else:
                event["product_id"] = product.product_id
            event["page_url"] = "https://shop.example.com/cart"

        elif event_type == "purchase":
            event["product_id"] = product.product_id
            event["product_category"] = product.category
            event["price_cents"] = product.price_cents
            event["quantity"] = random.randint(1, 5)
            event["page_url"] = "https://shop.example.com/checkout"
            session.cart.clear()

        return event

    def _generate_malformed_event(self) -> dict[str, Any]:
        """Generate an intentionally broken event for DLQ testing."""
        malformed_type = random.choice(["missing_fields", "bad_type", "negative_price"])

        if malformed_type == "missing_fields":
            return {
                "event_id": str(uuid.uuid4()),
                "event_type": "page_view",
                # Missing required fields: timestamp, user_id, session_id, etc.
            }
        elif malformed_type == "bad_type":
            return {
                "event_id": str(uuid.uuid4()),
                "event_type": "INVALID_TYPE",
                "timestamp": int(time.time() * 1000),
                "user_id": "user_000001",
                "session_id": str(uuid.uuid4()),
                "page_url": "https://shop.example.com/",
                "user_agent": "test",
                "ip_address": "127.0.0.1",
                "geo_country": "US",
            }
        else:
            return {
                "event_id": str(uuid.uuid4()),
                "event_type": "purchase",
                "timestamp": int(time.time() * 1000),
                "user_id": "user_000001",
                "session_id": str(uuid.uuid4()),
                "product_id": "prod_00001",
                "product_category": "Electronics",
                "price_cents": -9999,  # Negative price
                "quantity": 0,         # Zero quantity
                "page_url": "https://shop.example.com/checkout",
                "user_agent": "test",
                "ip_address": "127.0.0.1",
                "geo_country": "US",
            }

    def cleanup_expired_sessions(self) -> int:
        """Remove sessions that have been inactive beyond the session timeout."""
        now = time.time()
        timeout = settings.window.session_timeout_seconds
        expired = [
            uid for uid, sess in self._active_sessions.items()
            if (now - sess.last_active) > timeout
        ]
        for uid in expired:
            del self._active_sessions[uid]
        return len(expired)
