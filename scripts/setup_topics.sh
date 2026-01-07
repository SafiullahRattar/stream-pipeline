#!/usr/bin/env bash
# ============================================================================
# Create Kafka topics for the streaming pipeline.
#
# Run this after the Kafka broker is up:
#   docker compose exec kafka bash /scripts/setup_topics.sh
#
# Or it runs automatically via the init container in docker-compose.
# ============================================================================

set -euo pipefail

KAFKA_BROKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server "$KAFKA_BROKER" --list > /dev/null 2>&1; do
    echo "  Kafka not ready yet, retrying in 3s..."
    sleep 3
done
echo "Kafka is ready."

create_topic() {
    local topic="$1"
    local partitions="${2:-3}"
    local replication="${3:-1}"

    if kafka-topics --bootstrap-server "$KAFKA_BROKER" --describe --topic "$topic" > /dev/null 2>&1; then
        echo "Topic '$topic' already exists, skipping."
    else
        echo "Creating topic '$topic' (partitions=$partitions, replication=$replication)..."
        kafka-topics --bootstrap-server "$KAFKA_BROKER" \
            --create \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor "$replication" \
            --config retention.ms=86400000 \
            --config cleanup.policy=delete
        echo "  Created '$topic'."
    fi
}

# Main event topics
create_topic "clickstream.events" 6 1
create_topic "clickstream.aggregated" 3 1
create_topic "clickstream.dlq" 1 1

echo ""
echo "All topics ready:"
kafka-topics --bootstrap-server "$KAFKA_BROKER" --list
echo ""
echo "Topic setup complete."
