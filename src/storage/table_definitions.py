"""Iceberg table schemas for the order processing pipeline."""

from __future__ import annotations

from pyspark.sql import types as T

ORDER_EVENTS_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("order_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("event_timestamp", T.TimestampType(), False),
        T.StructField("payload", T.StringType(), True),
        T.StructField("processing_time", T.TimestampType(), False),
        T.StructField("shipping_city", T.StringType(), True),
        T.StructField("shipping_province", T.StringType(), True),
        T.StructField("payment_method", T.StringType(), True),
        T.StructField("order_status", T.StringType(), True),
        T.StructField("revenue_bucket", T.StringType(), True),
        T.StructField("item_count", T.IntegerType(), True),
        T.StructField("order_hour", T.IntegerType(), True),
        T.StructField("order_day_of_week", T.IntegerType(), True),
        T.StructField("is_weekend", T.BooleanType(), True),
    ]
)

ORDER_METRICS_SCHEMA = T.StructType(
    [
        T.StructField("window_start", T.TimestampType(), False),
        T.StructField("window_end", T.TimestampType(), False),
        T.StructField("region", T.StringType(), True),
        T.StructField("event_count", T.LongType(), False),
        T.StructField("order_count", T.LongType(), False),
        T.StructField("total_revenue", T.DoubleType(), True),
        T.StructField("avg_order_value", T.DoubleType(), True),
        T.StructField("unique_customers", T.LongType(), True),
        T.StructField("cancelled_count", T.LongType(), True),
        T.StructField("cancellation_rate", T.DoubleType(), True),
    ]
)

ORDER_QUALITY_SCHEMA = T.StructType(
    [
        T.StructField("check_timestamp", T.TimestampType(), False),
        T.StructField("records_checked", T.LongType(), False),
        T.StructField("records_valid", T.LongType(), False),
        T.StructField("records_invalid", T.LongType(), False),
        T.StructField("quarantine_rate", T.DoubleType(), True),
    ]
)

ORDER_ANOMALIES_SCHEMA = T.StructType(
    [
        T.StructField("anomaly_type", T.StringType(), False),
        T.StructField("detected_at", T.TimestampType(), False),
        T.StructField("order_id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("total_amount", T.DoubleType(), True),
        T.StructField("cancel_rate", T.DoubleType(), True),
        T.StructField("details", T.StringType(), True),
    ]
)

TABLE_DEFINITIONS = {
    "nessie.default.order_events": {
        "schema": ORDER_EVENTS_SCHEMA,
        "partitioning": ["hours(event_timestamp)"],
        "properties": {
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "5",
            "history.expire.max-snapshot-age-ms": "604800000",
        },
    },
    "nessie.default.order_metrics": {
        "schema": ORDER_METRICS_SCHEMA,
        "partitioning": ["hours(window_start)"],
        "properties": {
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "5",
        },
    },
    "nessie.default.order_quality": {
        "schema": ORDER_QUALITY_SCHEMA,
        "partitioning": ["hours(check_timestamp)"],
        "properties": {
            "write.metadata.delete-after-commit.enabled": "true",
        },
    },
    "nessie.default.order_anomalies": {
        "schema": ORDER_ANOMALIES_SCHEMA,
        "partitioning": ["hours(detected_at)"],
        "properties": {
            "write.metadata.delete-after-commit.enabled": "true",
        },
    },
}
