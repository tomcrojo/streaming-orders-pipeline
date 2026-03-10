"""Data quality checks on the streaming order events."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.consumer.config import VALID_EVENT_TYPES


def apply_quality_checks(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Apply all quality checks and split into valid/invalid DataFrames.

    Returns:
        Tuple of (valid_df, invalid_df) where invalid_df includes a reason column.
    """
    now = datetime.now(timezone.utc)

    checks = (
        F.when(F.col("order_id").isNull(), F.lit("null_order_id"))
        .when(
            (F.col("event_type") == "order_created")
            & (
                F.col("payload.total_amount").isNull()
                | (F.col("payload.total_amount") <= 0)
            ),
            F.lit("invalid_total_amount"),
        )
        .when(
            ~F.col("event_type").isin(VALID_EVENT_TYPES),
            F.lit("invalid_event_type"),
        )
        .when(
            (F.col("payload.total_amount").isNotNull())
            & (
                (F.col("payload.total_amount") < 0.01)
                | (F.col("payload.total_amount") > 50000)
            ),
            F.lit("revenue_out_of_bounds"),
        )
        .when(
            F.col("event_timestamp")
            > F.lit(now + timedelta(minutes=5)).cast("timestamp"),
            F.lit("timestamp_in_future"),
        )
        .when(
            F.col("event_timestamp")
            < F.lit(now - timedelta(hours=24)).cast("timestamp"),
            F.lit("timestamp_too_old"),
        )
        .otherwise(F.lit("valid"))
        .alias("quality_check_result")
    )

    with_check = df.withColumn("quality_check_result", checks)

    valid_df = with_check.filter(F.col("quality_check_result") == "valid").drop(
        "quality_check_result"
    )

    invalid_df = with_check.filter(
        F.col("quality_check_result") != "valid"
    ).withColumnRenamed("quality_check_result", "failure_reason")

    return valid_df, invalid_df


def compute_quality_metrics(valid_df: DataFrame, invalid_df: DataFrame) -> DataFrame:
    """Aggregate quality metrics per micro-batch.

    Returns DataFrame with columns:
      check_timestamp, records_checked, records_valid, records_invalid, quarantine_rate
    """
    valid_count = valid_df.groupBy().agg(F.count("*").alias("valid_count"))
    invalid_count = invalid_df.groupBy().agg(F.count("*").alias("invalid_count"))

    return (
        valid_count.crossJoin(invalid_count)
        .withColumn("check_timestamp", F.current_timestamp())
        .withColumn("records_checked", F.col("valid_count") + F.col("invalid_count"))
        .withColumn("records_valid", F.col("valid_count"))
        .withColumn("records_invalid", F.col("invalid_count"))
        .withColumn(
            "quarantine_rate",
            F.round(F.col("records_invalid") / F.col("records_checked"), 4),
        )
        .select(
            "check_timestamp",
            "records_checked",
            "records_valid",
            "records_invalid",
            "quarantine_rate",
        )
    )


def quarantine_sink(
    invalid_df: DataFrame, bootstrap_servers: str, topic: str = "orders.quarantine"
) -> None:
    """Write invalid records to Kafka quarantine topic."""
    quarantine_query = (
        invalid_df.select(
            F.col("order_id").cast("string").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .option("checkpointLocation", "checkpoints/quarantine")
        .outputMode("append")
        .start()
    )
    return quarantine_query
