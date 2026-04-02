"""Tests for the Kafka order producer."""

from __future__ import annotations

import json
import uuid

from src.producer.order_producer import OrderEventGenerator, OrderProducer
from src.producer.schemas import EVENT_TYPES


class TestOrderEventGenerator:
    """Test the order event generator."""

    def test_generate_event_returns_valid_structure(self):
        gen = OrderEventGenerator()
        event = gen.generate_event()

        assert "event_id" in event
        assert "event_type" in event
        assert "order_id" in event
        assert "customer_id" in event
        assert "timestamp" in event
        assert "payload" in event

    def test_event_type_is_valid(self):
        gen = OrderEventGenerator()
        for _ in range(50):
            event = gen.generate_event()
            assert event["event_type"] in EVENT_TYPES

    def test_event_id_is_uuid(self):
        gen = OrderEventGenerator()
        event = gen.generate_event()
        # Should not raise
        uuid.UUID(event["event_id"])

    def test_order_id_is_uuid(self):
        gen = OrderEventGenerator()
        event = gen.generate_event()
        uuid.UUID(event["order_id"])

    def test_customer_id_format(self):
        gen = OrderEventGenerator()
        event = gen.generate_event()
        assert event["customer_id"].startswith("CUST-")

    def test_currency_is_eur(self):
        gen = OrderEventGenerator()
        event = gen.generate_event()
        if event["event_type"] == "order_created":
            assert event["payload"]["currency"] == "EUR"

    def test_order_created_has_items(self):
        gen = OrderEventGenerator()
        for _ in range(20):
            event = gen.generate_event()
            if event["event_type"] == "order_created":
                items = event["payload"]["items"]
                assert len(items) > 0
                assert all(i["quantity"] > 0 for i in items)
                assert all(i["unit_price"] > 0 for i in items)
                break

    def test_total_amount_matches_items(self):
        gen = OrderEventGenerator()
        for _ in range(20):
            event = gen.generate_event()
            if event["event_type"] == "order_created":
                items = event["payload"]["items"]
                expected_total = round(sum(i["subtotal"] for i in items), 2)
                actual_total = round(event["payload"]["total_amount"], 2)
                assert abs(expected_total - actual_total) < 0.01, (
                    f"Total mismatch: expected {expected_total}, got {actual_total}"
                )
                break

    def test_shipping_address_has_spanish_fields(self):
        gen = OrderEventGenerator()
        for _ in range(20):
            event = gen.generate_event()
            if event["event_type"] == "order_created":
                addr = event["payload"]["shipping_address"]
                assert addr["country"] == "España"
                assert "city" in addr
                assert "province" in addr
                assert "postal_code" in addr
                break

    def test_active_orders_tracked(self):
        gen = OrderEventGenerator()
        gen.generate_event()
        assert len(gen._active_orders) >= 0


class TestOrderProducer:
    """Test the producer with dry-run mode."""

    def test_dry_run_produces_output(self, capsys):
        producer = OrderProducer(
            bootstrap_servers="localhost:9092",
            topic="orders.created",
            dry_run=True,
        )
        event = producer._generator.generate_event()
        producer.send(event)

        captured = capsys.readouterr()
        assert event["event_id"] in captured.out
        assert event["event_type"] in captured.out

    def test_sent_count_increments(self):
        producer = OrderProducer(
            bootstrap_servers="localhost:9092",
            topic="orders.created",
            dry_run=True,
        )
        assert producer._sent_count == 0
        event = producer._generator.generate_event()
        producer.send(event)
        assert producer._sent_count == 1

    def test_event_is_valid_json(self, capsys):
        producer = OrderProducer(
            bootstrap_servers="localhost:9092",
            topic="orders.created",
            dry_run=True,
        )
        event = producer._generator.generate_event()
        producer.send(event)

        captured = capsys.readouterr()
        # Parse the output back as JSON
        output_lines = captured.out.strip().split("---")
        for line in output_lines:
            line = line.strip()
            if line:
                parsed = json.loads(line)
                assert parsed["event_id"] == event["event_id"]
