"""
Seed script: produce a burst of events for local development and demos.

Usage:
    python -m scripts.seed_data --count 1000 --rate 500

This sends events as fast as possible (or at the specified rate) to quickly
populate the pipeline with data for testing.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from src.producer.event_generator import EventGenerator
from src.producer.producer import ClickstreamProducer

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the pipeline with test events")
    parser.add_argument(
        "--count", type=int, default=1000,
        help="Number of events to produce (default: 1000)",
    )
    parser.add_argument(
        "--rate", type=float, default=0.0,
        help="Events per second (0 = as fast as possible, default: 0)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    producer = ClickstreamProducer()
    generator = EventGenerator()

    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    logger.info("Seeding %d events (rate: %s events/sec)...", args.count,
                 f"{args.rate:.0f}" if args.rate > 0 else "unlimited")

    start = time.monotonic()
    for i in range(args.count):
        event = generator.generate_event()
        producer.produce_event(event)

        if (i + 1) % 100 == 0:
            producer._producer.poll(0)
            logger.info("  Produced %d / %d events", i + 1, args.count)

        if interval > 0:
            time.sleep(interval)

    # Flush all pending messages
    remaining = producer._producer.flush(timeout=30)
    elapsed = time.monotonic() - start

    logger.info(
        "Seed complete: %d events in %.1fs (%.0f events/sec). %d undelivered.",
        args.count, elapsed, args.count / elapsed if elapsed > 0 else 0, remaining,
    )
    sys.exit(0 if remaining == 0 else 1)


if __name__ == "__main__":
    main()
