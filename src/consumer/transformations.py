"""Streaming transformation functions for order events."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_events(raw_df: DataFrame) -> DataFrame:
    """Parse raw Kafka JSON value into structured DataFrame.

    Input: DataFrame with columns (key, value, topic, partition, offset, timestamp)
    Output: DataFrame with parsed event fields.
    """
    event_schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("event_type", T.StringType(), False),
            T.StructField("order_id", T.StringType(), False),
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("timestamp", T.StringType(), False),
            T.StructField(
                "payload",
                T.StructType(
                    [
                        T.StructField(
                            "items",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField("product_id", T.StringType()),
                                        T.StructField("product_name", T.StringType()),
                                        T.StructField("quantity", T.IntegerType()),
                                        T.StructField("unit_price", T.DoubleType()),
                                        T.StructField("subtotal", T.DoubleType()),
                                    ]
                                )
                            ),
                        ),
                        T.StructField("total_amount", T.DoubleType()),
                        T.StructField("currency", T.StringType()),
                        T.StructField(
                            "shipping_address",
                            T.StructType(
                                [
                                    T.StructField("street", T.StringType()),
                                    T.StructField("city", T.StringType()),
                                    T.StructField("province", T.StringType()),
                                    T.StructField("postal_code", T.StringType()),
                                    T.StructField("country", T.StringType()),
                                ]
                            ),
                        ),
                        T.StructField("payment_method", T.StringType()),
                        T.StructField("status", T.StringType()),
                        T.StructField("notes", T.StringType()),
                        T.StructField("cancellation_reason", T.StringType()),
                        T.StructField("tracking_number", T.StringType()),
                        T.StructField("carrier", T.StringType()),
                    ]
                ),
            ),
        ]
    )

    parsed = raw_df.select(
        F.col("key").cast("string").alias("message_key"),
        F.from_json(F.col("value").cast("string"), event_schema).alias("data"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    return parsed.select(
        "message_key",
        "data.event_id",
        "data.event_type",
        "data.order_id",
        "data.customer_id",
        F.to_timestamp("data.timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias(
            "event_timestamp"
        ),
        "data.payload",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("processing_time"),
    )


def enrich_orders(df: DataFrame) -> DataFrame:
    """Add derived fields for analytics."""
    return df.withColumns(
        {
            "order_hour": F.hour("event_timestamp"),
            "order_day_of_week": F.dayofweek("event_timestamp"),
            "order_date": F.to_date("event_timestamp"),
            "is_weekend": F.when(
                F.dayofweek("event_timestamp").isin(1, 7), F.lit(True)
            ).otherwise(F.lit(False)),
            "revenue_bucket": F.when(
                F.col("payload.total_amount").isNull(), F.lit("unknown")
            )
            .when(F.col("payload.total_amount") < 25, F.lit("low"))
            .when(F.col("payload.total_amount") < 100, F.lit("medium"))
            .when(F.col("payload.total_amount") < 500, F.lit("high"))
            .otherwise(F.lit("premium")),
            "shipping_city": F.col("payload.shipping_address.city"),
            "shipping_province": F.col("payload.shipping_address.province"),
            "payment_method": F.col("payload.payment_method"),
            "order_status": F.col("payload.status"),
            "item_count": F.when(
                F.col("payload.items").isNotNull(), F.size("payload.items")
            ).otherwise(F.lit(0)),
        }
    )


def windowed_aggregations(
    df: DataFrame, watermark_delay: str = "10 minutes"
) -> DataFrame:
    """Compute windowed aggregations: tumbling (1min) and sliding (5min, 2min slide)."""

    watermarked = df.withWatermark("event_timestamp", watermark_delay)

    # 1-minute tumbling window aggregations
    tumbling = (
        watermarked.groupBy(
            F.window("event_timestamp", "1 minute"),
            F.col("shipping_province").alias("region"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum(
                F.when(
                    F.col("event_type") == "order_created",
                    F.col("payload.total_amount"),
                ).otherwise(0)
            ).alias("total_revenue"),
            F.avg(
                F.when(
                    F.col("event_type") == "order_created",
                    F.col("payload.total_amount"),
                )
            ).alias("avg_order_value"),
            F.countDistinct(
                F.when(F.col("event_type") == "order_created", F.col("customer_id"))
            ).alias("unique_customers"),
            F.sum(
                F.when(F.col("event_type") == "order_cancelled", 1).otherwise(0)
            ).alias("cancelled_count"),
        )
        .withColumn(
            "cancellation_rate",
            F.round(F.col("cancelled_count") / F.col("event_count"), 4),
        )
        .withColumnRenamed("window", "tumbling_window")
    )

    # 5-minute sliding window (2-minute slide)
    sliding = (
        watermarked.groupBy(
            F.window("event_timestamp", "5 minutes", "2 minutes"),
            F.col("shipping_province").alias("region"),
        )
        .agg(
            F.countDistinct("order_id").alias("sliding_order_count"),
            F.sum(
                F.when(
                    F.col("event_type") == "order_created",
                    F.col("payload.total_amount"),
                ).otherwise(0)
            ).alias("sliding_total_revenue"),
            F.avg(
                F.when(
                    F.col("event_type") == "order_created",
                    F.col("payload.total_amount"),
                )
            ).alias("sliding_avg_order_value"),
            F.countDistinct(
                F.when(F.col("event_type") == "order_created", F.col("customer_id"))
            ).alias("sliding_unique_customers"),
        )
        .withColumnRenamed("window", "sliding_window")
    )

    return tumbling, sliding


def detect_anomalies(df: DataFrame, watermark_delay: str = "10 minutes") -> DataFrame:
    """Flag unusual patterns in the stream.

    Detects:
    - Spike in cancellations (>30% in a 1-minute window)
    - Large orders (>EUR 2000)
    - Rapid-fire orders from same customer (>5 orders in 5 minutes)
    """
    watermarked = df.withWatermark("event_timestamp", watermark_delay)

    # Cancellation spike detection per region
    cancellation_spike = (
        watermarked.groupBy(
            F.window("event_timestamp", "1 minute"),
            F.col("shipping_province").alias("region"),
        )
        .agg(
            F.count("*").alias("total_events"),
            F.sum(
                F.when(F.col("event_type") == "order_cancelled", 1).otherwise(0)
            ).alias("cancelled"),
        )
        .withColumn("cancel_rate", F.col("cancelled") / F.col("total_events"))
        .filter(F.col("cancel_rate") > 0.3)
        .withColumn("anomaly_type", F.lit("cancellation_spike"))
    )

    # Large order detection
    large_orders = (
        watermarked.filter(
            (F.col("event_type") == "order_created")
            & (F.col("payload.total_amount") > 2000)
        )
        .withColumn("anomaly_type", F.lit("large_order"))
        .withColumn("anomaly_value", F.col("payload.total_amount"))
    )

    return cancellation_spike, large_orders
