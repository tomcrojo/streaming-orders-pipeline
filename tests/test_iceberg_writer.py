"""Tests for Iceberg writer."""

from __future__ import annotations

from src.storage.table_definitions import (
    ORDER_EVENTS_SCHEMA,
    ORDER_METRICS_SCHEMA,
    ORDER_QUALITY_SCHEMA,
    ORDER_ANOMALIES_SCHEMA,
    TABLE_DEFINITIONS,
)


class TestTableDefinitions:
    """Test Iceberg table schema definitions."""

    def test_order_events_schema_has_required_fields(self):
        field_names = [f.name for f in ORDER_EVENTS_SCHEMA.fields]
        assert "event_id" in field_names
        assert "event_type" in field_names
        assert "order_id" in field_names
        assert "customer_id" in field_names
        assert "event_timestamp" in field_names
        assert "payload" in field_names
        assert "processing_time" in field_names

    def test_order_metrics_schema_has_required_fields(self):
        field_names = [f.name for f in ORDER_METRICS_SCHEMA.fields]
        assert "window_start" in field_names
        assert "window_end" in field_names
        assert "region" in field_names
        assert "order_count" in field_names
        assert "total_revenue" in field_names
        assert "cancellation_rate" in field_names

    def test_order_quality_schema_has_required_fields(self):
        field_names = [f.name for f in ORDER_QUALITY_SCHEMA.fields]
        assert "check_timestamp" in field_names
        assert "records_checked" in field_names
        assert "records_valid" in field_names
        assert "records_invalid" in field_names
        assert "quarantine_rate" in field_names

    def test_order_anomalies_schema_has_required_fields(self):
        field_names = [f.name for f in ORDER_ANOMALIES_SCHEMA.fields]
        assert "anomaly_type" in field_names
        assert "detected_at" in field_names

    def test_event_id_is_not_nullable(self):
        event_id_field = next(
            f for f in ORDER_EVENTS_SCHEMA.fields if f.name == "event_id"
        )
        assert event_id_field.nullable is False

    def test_all_tables_have_definitions(self):
        expected_tables = {
            "nessie.default.order_events",
            "nessie.default.order_metrics",
            "nessie.default.order_quality",
            "nessie.default.order_anomalies",
        }
        assert set(TABLE_DEFINITIONS.keys()) == expected_tables

    def test_all_tables_have_partitioning(self):
        for table_name, definition in TABLE_DEFINITIONS.items():
            assert "partitioning" in definition, f"{table_name} missing partitioning"
            assert len(definition["partitioning"]) > 0, (
                f"{table_name} has empty partitioning"
            )

    def test_all_tables_have_properties(self):
        for table_name, definition in TABLE_DEFINITIONS.items():
            assert "properties" in definition, f"{table_name} missing properties"
