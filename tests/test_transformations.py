"""Tests for streaming transformations."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-transformations")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestParseEvents:
    """Test JSON event parsing."""

    def test_parse_valid_json(self, spark):
        from src.consumer.transformations import parse_events

        schema = T.StructType(
            [
                T.StructField("key", T.BinaryType()),
                T.StructField("value", T.BinaryType()),
                T.StructField("topic", T.StringType()),
                T.StructField("partition", T.IntegerType()),
                T.StructField("offset", T.LongType()),
                T.StructField("timestamp", T.TimestampType()),
            ]
        )

        event_json = b"""{
            "event_id": "test-001",
            "event_type": "order_created",
            "order_id": "order-001",
            "customer_id": "CUST-000001",
            "timestamp": "2025-01-15T10:30:00.000000+00:00",
            "payload": {
                "items": [{"product_id": "P1", "product_name": "Test", "quantity": 1, "unit_price": 10.0, "subtotal": 10.0}],
                "total_amount": 10.0,
                "currency": "EUR",
                "status": "created"
            }
        }"""

        rows = [(None, event_json, "orders.created", 0, 0, None)]
        raw_df = spark.createDataFrame(rows, schema)

        parsed = parse_events(raw_df)
        result = parsed.collect()

        assert len(result) == 1
        row = result[0]
        assert row.event_id == "test-001"
        assert row.event_type == "order_created"
        assert row.order_id == "order-001"


class TestEnrichOrders:
    """Test order enrichment."""

    def test_enrich_adds_derived_columns(self, spark):
        from src.consumer.transformations import enrich_orders

        data = [
            (
                "evt-1",
                "order_created",
                "ord-1",
                "C1",
                "2025-01-15T10:30:00.000000+00:00",
                {
                    "total_amount": 50.0,
                    "shipping_address": {"city": "Madrid", "province": "Madrid"},
                    "payment_method": "credit_card",
                    "status": "created",
                    "items": [{"subtotal": 50.0}],
                },
                None,
                None,
                None,
                None,
            ),
        ]

        schema = T.StructType(
            [
                T.StructField("event_id", T.StringType()),
                T.StructField("event_type", T.StringType()),
                T.StructField("order_id", T.StringType()),
                T.StructField("customer_id", T.StringType()),
                T.StructField("event_timestamp", T.StringType()),
                T.StructField(
                    "payload",
                    T.StructType(
                        [
                            T.StructField("total_amount", T.DoubleType()),
                            T.StructField(
                                "shipping_address",
                                T.StructType(
                                    [
                                        T.StructField("city", T.StringType()),
                                        T.StructField("province", T.StringType()),
                                    ]
                                ),
                            ),
                            T.StructField("payment_method", T.StringType()),
                            T.StructField("status", T.StringType()),
                            T.StructField(
                                "items",
                                T.ArrayType(
                                    T.StructType(
                                        [
                                            T.StructField("subtotal", T.DoubleType()),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    ),
                ),
                T.StructField("processing_time", T.TimestampType()),
                T.StructField("kafka_topic", T.StringType()),
                T.StructField("kafka_partition", T.IntegerType()),
                T.StructField("kafka_offset", T.LongType()),
            ]
        )

        df = spark.createDataFrame(data, schema)
        # Convert timestamp string to actual timestamp
        df = df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

        enriched = enrich_orders(df)
        result = enriched.collect()[0]

        assert result.order_hour == 10
        assert result.shipping_city == "Madrid"
        assert result.shipping_province == "Madrid"
        assert result.payment_method == "credit_card"
        assert result.revenue_bucket == "medium"


class TestQualityChecks:
    """Test data quality checks."""

    def test_valid_records_pass(self, spark):
        from src.consumer.quality_checks import apply_quality_checks

        event_timestamp = datetime.now(timezone.utc).isoformat()

        data = [
            (
                "evt-1",
                "order_created",
                "ord-1",
                "C1",
                event_timestamp,
                {"total_amount": 50.0},
            ),
        ]

        schema = T.StructType(
            [
                T.StructField("event_id", T.StringType()),
                T.StructField("event_type", T.StringType()),
                T.StructField("order_id", T.StringType()),
                T.StructField("customer_id", T.StringType()),
                T.StructField("event_timestamp", T.StringType()),
                T.StructField(
                    "payload",
                    T.StructType(
                        [
                            T.StructField("total_amount", T.DoubleType()),
                        ]
                    ),
                ),
            ]
        )

        df = spark.createDataFrame(data, schema)
        df = df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

        valid, invalid = apply_quality_checks(df)
        assert valid.count() == 1
        assert invalid.count() == 0

    def test_null_order_id_is_quarantined(self, spark):
        from src.consumer.quality_checks import apply_quality_checks

        data = [
            (
                "evt-1",
                "order_created",
                None,
                "C1",
                "2025-01-15T10:30:00+00:00",
                {"total_amount": 50.0},
            ),
        ]

        schema = T.StructType(
            [
                T.StructField("event_id", T.StringType()),
                T.StructField("event_type", T.StringType()),
                T.StructField("order_id", T.StringType()),
                T.StructField("customer_id", T.StringType()),
                T.StructField("event_timestamp", T.StringType()),
                T.StructField(
                    "payload",
                    T.StructType(
                        [
                            T.StructField("total_amount", T.DoubleType()),
                        ]
                    ),
                ),
            ]
        )

        df = spark.createDataFrame(data, schema)
        df = df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

        valid, invalid = apply_quality_checks(df)
        assert valid.count() == 0
        assert invalid.count() == 1
        reason = invalid.collect()[0].failure_reason
        assert reason == "null_order_id"

    def test_invalid_event_type_is_quarantined(self, spark):
        from src.consumer.quality_checks import apply_quality_checks

        data = [
            (
                "evt-1",
                "invalid_type",
                "ord-1",
                "C1",
                "2025-01-15T10:30:00+00:00",
                {"total_amount": 50.0},
            ),
        ]

        schema = T.StructType(
            [
                T.StructField("event_id", T.StringType()),
                T.StructField("event_type", T.StringType()),
                T.StructField("order_id", T.StringType()),
                T.StructField("customer_id", T.StringType()),
                T.StructField("event_timestamp", T.StringType()),
                T.StructField(
                    "payload",
                    T.StructType(
                        [
                            T.StructField("total_amount", T.DoubleType()),
                        ]
                    ),
                ),
            ]
        )

        df = spark.createDataFrame(data, schema)
        df = df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

        valid, invalid = apply_quality_checks(df)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_revenue_out_of_bounds_is_quarantined(self, spark):
        from src.consumer.quality_checks import apply_quality_checks

        data = [
            (
                "evt-1",
                "order_created",
                "ord-1",
                "C1",
                "2025-01-15T10:30:00+00:00",
                {"total_amount": 999999.0},
            ),
        ]

        schema = T.StructType(
            [
                T.StructField("event_id", T.StringType()),
                T.StructField("event_type", T.StringType()),
                T.StructField("order_id", T.StringType()),
                T.StructField("customer_id", T.StringType()),
                T.StructField("event_timestamp", T.StringType()),
                T.StructField(
                    "payload",
                    T.StructType(
                        [
                            T.StructField("total_amount", T.DoubleType()),
                        ]
                    ),
                ),
            ]
        )

        df = spark.createDataFrame(data, schema)
        df = df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

        valid, invalid = apply_quality_checks(df)
        assert valid.count() == 0
        assert invalid.count() == 1
