"""
Real-time dashboard API powered by FastAPI and WebSockets.

Provides:
- HTML dashboard at / with live-updating metrics
- WebSocket endpoint at /ws that broadcasts aggregation updates
- REST endpoint at /api/stats for polling-based clients
- Health check at /health

The dashboard consumes from the aggregated Kafka topic in a background
thread and pushes updates to all connected WebSocket clients.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

import fastavro
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from src.config import settings

logger = logging.getLogger(__name__)

AGGREGATED_SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "aggregated.avsc"

app = FastAPI(title="Stream Pipeline Dashboard", version="0.1.0")

# Static files and templates
STATIC_DIR = Path(__file__).parent / "static"
TEMPLATES_DIR = Path(__file__).parent / "templates"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

class DashboardState:
    """Thread-safe container for the latest aggregation data."""

    def __init__(self, max_history: int = 120) -> None:
        self._lock = threading.Lock()
        self._latest: dict[str, Any] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=max_history)
        self._total_events: int = 0
        self._total_revenue_cents: int = 0
        self._total_purchases: int = 0
        self._total_unique_users: int = 0
        self._connected_clients: int = 0

    def update(self, aggregation: dict[str, Any]) -> None:
        with self._lock:
            self._latest = aggregation
            self._history.append(aggregation)
            self._total_events += aggregation.get("event_count", 0)
            self._total_revenue_cents += aggregation.get("total_revenue_cents", 0)
            self._total_purchases += aggregation.get("purchases", 0)
            self._total_unique_users += aggregation.get("unique_users", 0)

    def get_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "latest": self._latest,
                "history": list(self._history),
                "totals": {
                    "events": self._total_events,
                    "revenue_dollars": self._total_revenue_cents / 100.0,
                    "purchases": self._total_purchases,
                    "unique_users": self._total_unique_users,
                },
                "connected_clients": self._connected_clients,
                "timestamp": int(time.time() * 1000),
            }

    def increment_clients(self) -> None:
        with self._lock:
            self._connected_clients += 1

    def decrement_clients(self) -> None:
        with self._lock:
            self._connected_clients -= 1


state = DashboardState()
ws_clients: list[WebSocket] = []
ws_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Kafka consumer background thread
# ---------------------------------------------------------------------------

def _load_aggregated_schema() -> Any:
    with open(AGGREGATED_SCHEMA_PATH) as f:
        return fastavro.parse_schema(json.load(f))


def _kafka_consumer_thread() -> None:
    """Background thread that consumes aggregated results from Kafka."""
    logger.info("Dashboard Kafka consumer thread starting...")
    schema = _load_aggregated_schema()

    consumer = Consumer({
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "group.id": "dashboard-consumers",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([settings.kafka.aggregated_topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Dashboard consumer error: %s", msg.error())
                continue

            try:
                buf = io.BytesIO(msg.value())
                record = fastavro.schemaless_reader(buf, schema)
                state.update(record)
            except Exception as exc:
                logger.warning("Failed to deserialize aggregation: %s", exc)
    except Exception:
        logger.exception("Dashboard consumer thread crashed")
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# WebSocket broadcaster
# ---------------------------------------------------------------------------

async def _broadcast_loop() -> None:
    """Periodically broadcast the latest state to all WebSocket clients."""
    interval = settings.dashboard.ws_broadcast_interval

    while True:
        await asyncio.sleep(interval)
        if not ws_clients:
            continue

        snapshot = state.get_snapshot()
        message = json.dumps(snapshot, default=str)

        async with ws_lock:
            disconnected: list[WebSocket] = []
            for ws in ws_clients:
                try:
                    await ws.send_text(message)
                except Exception:
                    disconnected.append(ws)

            for ws in disconnected:
                ws_clients.remove(ws)
                state.decrement_clients()


# ---------------------------------------------------------------------------
# Lifecycle events
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup() -> None:
    # Start Kafka consumer in background thread
    thread = threading.Thread(target=_kafka_consumer_thread, daemon=True)
    thread.start()

    # Start WebSocket broadcast loop
    asyncio.create_task(_broadcast_loop())
    logger.info("Dashboard started on %s:%d", settings.dashboard.host, settings.dashboard.port)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request) -> HTMLResponse:
    """Serve the main dashboard HTML page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    """WebSocket endpoint for real-time dashboard updates."""
    await ws.accept()
    async with ws_lock:
        ws_clients.append(ws)
    state.increment_clients()

    logger.info("WebSocket client connected (total: %d)", len(ws_clients))

    try:
        # Send initial snapshot
        snapshot = state.get_snapshot()
        await ws.send_text(json.dumps(snapshot, default=str))

        # Keep connection alive, listen for client messages (e.g., ping)
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        async with ws_lock:
            if ws in ws_clients:
                ws_clients.remove(ws)
        state.decrement_clients()
        logger.info("WebSocket client disconnected (total: %d)", len(ws_clients))


@app.get("/api/stats")
async def get_stats() -> dict[str, Any]:
    """REST endpoint returning the current pipeline statistics."""
    return state.get_snapshot()


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "healthy"}


def main() -> None:
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    uvicorn.run(
        "src.dashboard.app:app",
        host=settings.dashboard.host,
        port=settings.dashboard.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
