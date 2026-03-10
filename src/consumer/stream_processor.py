"""Spark Structured Streaming consumer — main entry point."""

from __future__ import annotations

import argparse
import logging
import signal
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.consumer.config import (
    CHECKPOINT_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MAX_OFFSETS_PER_TRIGGER,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_SECRET_KEY,
    TRIGGER_INTERVAL,
    WATERMARK_DELAY,
    WAREHOUSE,
    NESSIE_URI,
)
from src.consumer.quality_checks import apply_quality_checks, compute_quality_metrics
from src.consumer.transformations import (
    detect_anomalies,
    enrich_orders,
    parse_events,
    windowed_aggregations,
)
from src.storage.iceberg_writer import (
    create_tables_if_not_exists,
    write_raw_events,
    write_metrics,
    write_quality_metrics,
    write_anomalies,
)
from src.storage.table_definitions import (
    ORDER_EVENTS_SCHEMA,
    ORDER_METRICS_SCHEMA,
    ORDER_QUALITY_SCHEMA,
    ORDER_ANOMALIES_SCHEMA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("stream_processor")


def create_spark_session() -> SparkSession:
    """Create a SparkSession configured for Iceberg + Kafka."""
    builder = (
        SparkSession.builder.appName("OrderStreamProcessor")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.rest.RESTCatalog",
        )
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .config(
            "spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.nessie.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.access-key-id", S3_ACCESS_KEY)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", S3_SECRET_KEY)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    )

    return builder.getOrCreate()


def build_kafka_source(spark: SparkSession) -> "DataFrame":
    """Build a streaming DataFrame from Kafka source."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )


def run_pipeline(spark: SparkSession, checkpoint_dir: str) -> None:
    """Execute the main streaming pipeline."""
    logger.info("Starting order stream processor")
    logger.info("Kafka: %s, Topic: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    logger.info("Checkpoint dir: %s", checkpoint_dir)

    # Ensure Iceberg tables exist
    create_tables_if_not_exists(spark)

    # 1. Kafka source
    raw_stream = build_kafka_source(spark)

    # 2. Parse events
    parsed = parse_events(raw_stream)

    # 3. Apply quality checks
    valid_events, invalid_events = apply_quality_checks(parsed)

    # 4. Enrich valid events
    enriched = enrich_orders(valid_events)

    # 5. Windowed aggregations
    tumbling_agg, sliding_agg = windowed_aggregations(enriched, WATERMARK_DELAY)

    # 6. Anomaly detection
    cancellation_spikes, large_order_anomalies = detect_anomalies(
        enriched, WATERMARK_DELAY
    )

    # 7. Quality metrics
    quality_metrics = compute_quality_metrics(valid_events, invalid_events)

    # 8. Start all streaming writes
    queries = []

    # Write raw events to Iceberg
    raw_query = write_raw_events(
        enriched,
        checkpoint_dir=f"{checkpoint_dir}/raw_events",
    )
    queries.append(raw_query)

    # Write tumbling aggregations to Iceberg
    tumbling_query = write_metrics(
        tumbling_agg,
        checkpoint_dir=f"{checkpoint_dir}/tumbling_metrics",
    )
    queries.append(tumbling_query)

    # Write quality metrics
    quality_query = write_quality_metrics(
        quality_metrics,
        checkpoint_dir=f"{checkpoint_dir}/quality_metrics",
    )
    queries.append(quality_query)

    # Write anomalies to Iceberg
    anomaly_query_1 = write_anomalies(
        cancellation_spikes,
        checkpoint_dir=f"{checkpoint_dir}/anomalies_cancellation",
    )
    queries.append(anomaly_query_1)

    anomaly_query_2 = write_anomalies(
        large_order_anomalies,
        checkpoint_dir=f"{checkpoint_dir}/anomalies_large_orders",
    )
    queries.append(anomaly_query_2)

    # Write invalid events to quarantine Kafka topic
    quarantine_query = (
        invalid_events.select(
            F.col("order_id").cast("string").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "orders.quarantine")
        .option("checkpointLocation", f"{checkpoint_dir}/quarantine")
        .outputMode("append")
        .queryName("quarantine")
        .start()
    )
    queries.append(quarantine_query)

    logger.info("Started %d streaming queries", len(queries))

    # Graceful shutdown
    def _signal_handler(sig, frame):
        logger.info("Shutdown signal received, stopping queries...")
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Wait for termination
    for q in queries:
        try:
            q.awaitTermination()
        except Exception as e:
            logger.error("Query terminated with error: %s", e)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming order processor"
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=CHECKPOINT_DIR,
        help="Checkpoint directory for streaming state",
    )
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        run_pipeline(spark, args.checkpoint_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
