"""Consumer / Spark Streaming configuration."""

from __future__ import annotations

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders.created")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "checkpoints/streaming")

# Iceberg catalog
NESSIE_URI = os.getenv("NESSIE_URI", "http://localhost:19120/api/v1")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password123")
WAREHOUSE = os.getenv("WAREHOUSE", "s3a://warehouse/")

# Streaming trigger
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))

# Watermark
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")

VALID_EVENT_TYPES = [
    "order_created",
    "order_item_added",
    "order_confirmed",
    "order_shipped",
    "order_delivered",
    "order_cancelled",
]
