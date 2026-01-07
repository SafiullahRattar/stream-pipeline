"""
Unit tests for the dead letter queue.

Tests DLQ message formatting, metadata enrichment, and edge cases
using a mocked Kafka producer to avoid needing a running broker.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


# We need to mock the Kafka producer and metrics before importing DLQ
@pytest.fixture(autouse=True)
def _mock_kafka_and_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock Kafka producer and Prometheus metrics for all tests."""
    # Mock confluent_kafka.Producer
    mock_producer_class = MagicMock()
    mock_producer_instance = MagicMock()
    mock_producer_class.return_value = mock_producer_instance
    monkeypatch.setattr("src.sink.dead_letter.Producer", mock_producer_class)

    # Mock metrics
    mock_metrics = MagicMock()
    monkeypatch.setattr("src.sink.dead_letter.get_metrics", lambda: mock_metrics)


from src.sink.dead_letter import DeadLetterQueue


class TestDeadLetterQueue:
    def test_send_with_bytes_payload(self) -> None:
        dlq = DeadLetterQueue()
        dlq.send(
            original_value=b'{"event_id": "test-123"}',
            error_reason="Validation failed",
            source_topic="clickstream.events",
            source_partition=0,
            source_offset=42,
        )
        # Verify produce was called
        dlq._producer.produce.assert_called_once()
        call_kwargs = dlq._producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "clickstream.dlq"

        # Verify the DLQ message structure
        value = call_kwargs.kwargs["value"]
        msg = json.loads(value.decode("utf-8"))
        assert msg["error_reason"] == "Validation failed"
        assert msg["source_topic"] == "clickstream.events"
        assert msg["source_partition"] == 0
        assert msg["source_offset"] == 42
        assert "failed_at" in msg
        assert "failed_at_iso" in msg

    def test_send_with_dict_payload(self) -> None:
        dlq = DeadLetterQueue()
        original = {"event_id": "test-456", "event_type": "INVALID"}
        dlq.send(
            original_value=original,
            error_reason="Invalid event_type",
            source_topic="clickstream.events",
            source_partition=1,
            source_offset=99,
        )
        call_kwargs = dlq._producer.produce.call_args
        value = json.loads(call_kwargs.kwargs["value"].decode("utf-8"))
        assert "test-456" in value["original_payload"]
        assert value["error_reason"] == "Invalid event_type"

    def test_send_with_binary_non_utf8_payload(self) -> None:
        dlq = DeadLetterQueue()
        # Non-UTF-8 binary data should be base64-encoded
        binary_data = bytes(range(256))
        dlq.send(
            original_value=binary_data,
            error_reason="Corrupt data",
            source_topic="clickstream.events",
            source_partition=0,
            source_offset=0,
        )
        call_kwargs = dlq._producer.produce.call_args
        value = json.loads(call_kwargs.kwargs["value"].decode("utf-8"))
        assert value["original_payload"]  # Should be base64-encoded
        assert value["error_reason"] == "Corrupt data"

    def test_key_format(self) -> None:
        dlq = DeadLetterQueue()
        dlq.send(
            original_value=b"test",
            error_reason="error",
            source_topic="my.topic",
            source_partition=3,
            source_offset=777,
        )
        call_kwargs = dlq._producer.produce.call_args
        key = call_kwargs.kwargs["key"]
        assert key == b"my.topic:3:777"

    def test_produce_failure_does_not_raise(self) -> None:
        dlq = DeadLetterQueue()
        dlq._producer.produce.side_effect = Exception("Kafka is down")

        # Should not raise -- DLQ failures must not crash the pipeline
        dlq.send(
            original_value=b"test",
            error_reason="error",
            source_topic="test",
            source_partition=0,
            source_offset=0,
        )

    def test_flush_delegates_to_producer(self) -> None:
        dlq = DeadLetterQueue()
        dlq._producer.flush.return_value = 0
        remaining = dlq.flush(timeout=10.0)
        dlq._producer.flush.assert_called_once_with(timeout=10.0)
        assert remaining == 0

    def test_close_flushes(self) -> None:
        dlq = DeadLetterQueue()
        dlq._producer.flush.return_value = 0
        dlq.close()
        dlq._producer.flush.assert_called_once()
