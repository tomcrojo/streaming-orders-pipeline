"""Iceberg table management and streaming write operations."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.storage.table_definitions import TABLE_DEFINITIONS

logger = logging.getLogger("iceberg_writer")

DEFAULT_CHECKPOINT = "checkpoints/streaming"


def create_tables_if_not_exists(spark: SparkSession) -> None:
    """Create all Iceberg tables if they do not already exist."""
    for table_name, definition in TABLE_DEFINITIONS.items():
        try:
            if spark.catalog.tableExists(table_name):
                logger.info("Table %s already exists, skipping creation", table_name)
                continue
        except Exception:
            pass

        logger.info("Creating Iceberg table: %s", table_name)

        partitioning = definition.get("partitioning", [])
        partition_sql = ""
        if partitioning:
            partition_sql = f"PARTITIONED BY ({', '.join(partitioning)})"

        properties = definition.get("properties", {})
        props_sql = ""
        if properties:
            props_items = [f"'{k}' = '{v}'" for k, v in properties.items()]
            props_sql = f"TBLPROPERTIES ({', '.join(props_items)})"

        schema = definition["schema"]
        columns = []
        for field in schema.fields:
            col_def = f"`{field.name}` {field.dataType.simpleString()}"
            if not field.nullable:
                col_def += " NOT NULL"
            columns.append(col_def)

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(columns)}
        )
        USING iceberg
        {partition_sql}
        {props_sql}
        """

        try:
            spark.sql(ddl)
            logger.info("Created table: %s", table_name)
        except Exception as e:
            logger.warning(
                "Could not create table %s: %s (may already exist via auto-creation)",
                table_name,
                e,
            )


def write_raw_events(
    df: DataFrame,
    checkpoint_dir: str = f"{DEFAULT_CHECKPOINT}/raw_events",
) -> Any:
    """Write enriched raw events to Iceberg order_events table."""
    # Select columns matching the Iceberg schema, serialize payload to JSON string
    output = df.select(
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        F.col("event_timestamp").alias("event_timestamp"),
        F.to_json("payload").alias("payload"),
        F.col("processing_time"),
        "shipping_city",
        "shipping_province",
        "payment_method",
        "order_status",
        "revenue_bucket",
        "item_count",
        "order_hour",
        "order_day_of_week",
        "is_weekend",
    )

    query = (
        output.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .option("path", "nessie.default.order_events")
        .queryName("raw_events")
        .start()
    )

    logger.info("Started raw_events write stream (checkpoint: %s)", checkpoint_dir)
    return query


def write_metrics(
    metrics_df: DataFrame,
    checkpoint_dir: str = f"{DEFAULT_CHECKPOINT}/metrics",
) -> Any:
    """Write windowed metrics to Iceberg order_metrics table."""
    # Flatten window struct into columns
    output = metrics_df.select(
        F.col("tumbling_window.start").alias("window_start"),
        F.col("tumbling_window.end").alias("window_end"),
        "region",
        "event_count",
        "order_count",
        "total_revenue",
        "avg_order_value",
        "unique_customers",
        "cancelled_count",
        "cancellation_rate",
    )

    query = (
        output.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .option("path", "nessie.default.order_metrics")
        .queryName("order_metrics")
        .start()
    )

    logger.info("Started order_metrics write stream (checkpoint: %s)", checkpoint_dir)
    return query


def write_quality_metrics(
    quality_df: DataFrame,
    checkpoint_dir: str = f"{DEFAULT_CHECKPOINT}/quality",
) -> Any:
    """Write quality metrics to Iceberg order_quality table."""
    query = (
        quality_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .option("path", "nessie.default.order_quality")
        .queryName("order_quality")
        .start()
    )

    logger.info("Started order_quality write stream (checkpoint: %s)", checkpoint_dir)
    return query


def write_anomalies(
    anomalies_df: DataFrame,
    checkpoint_dir: str = f"{DEFAULT_CHECKPOINT}/anomalies",
) -> Any:
    """Write detected anomalies to Iceberg order_anomalies table."""
    output = anomalies_df.select(
        "anomaly_type",
        F.current_timestamp().alias("detected_at"),
        "order_id",
        "customer_id",
        F.col("region"),
        F.col("payload.total_amount").alias("total_amount"),
        "cancel_rate",
        F.lit(None).cast("string").alias("details"),
    )

    query = (
        output.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .option("path", "nessie.default.order_anomalies")
        .queryName("order_anomalies")
        .start()
    )

    logger.info("Started order_anomalies write stream (checkpoint: %s)", checkpoint_dir)
    return query


def time_travel_query(
    spark: SparkSession,
    table: str,
    timestamp: str,
) -> DataFrame:
    """Execute a time-travel query on an Iceberg table.

    Args:
        spark: SparkSession
        table: Full table name (e.g., 'nessie.default.order_events')
        timestamp: ISO timestamp or snapshot ID
    """
    return spark.sql(f"SELECT * FROM {table} FOR SYSTEM_TIME AS OF '{timestamp}'")


def expire_old_snapshots(
    spark: SparkSession, table: str, older_than_ms: int = 604800000
) -> None:
    """Expire snapshots older than the given duration (default 7 days)."""
    spark.sql(f"""
        CALL nessie.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{older_than_ms}'
        )
    """)
    logger.info("Expired snapshots for %s older than %dms", table, older_than_ms)


def compact_small_files(
    spark: SparkSession, table: str, target_file_size: int = 134217728
) -> None:
    """Compact small files in an Iceberg table (default target: 128MB)."""
    spark.sql(f"""
        CALL nessie.system.rewrite_data_files(
            table => '{table}',
            strategy => 'binpack',
            target_file_size_bytes => {target_file_size}
        )
    """)
    logger.info(
        "Compacted files in %s to target size %d bytes", table, target_file_size
    )
