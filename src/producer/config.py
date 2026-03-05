"""Producer configuration."""

from __future__ import annotations

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders.created")

# Producer settings for exactly-once semantics
PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 2147483647,
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
    "compression.type": "snappy",
    "linger.ms": 20,
    "batch.size": 32768,
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.kbytes": 1048576,
    "message.send.max.retries": 10,
    "retry.backoff.ms": 100,
}

# Customer pool size
CUSTOMER_POOL_SIZE = int(os.getenv("CUSTOMER_POOL_SIZE", "10000"))

# Product catalog size
PRODUCT_CATALOG_SIZE = int(os.getenv("PRODUCT_CATALOG_SIZE", "500"))

# Default generation parameters
DEFAULT_RATE = float(os.getenv("PRODUCER_RATE", "10"))  # events per second
DEFAULT_DURATION = int(os.getenv("PRODUCER_DURATION", "0"))  # 0 = infinite
