"""Collect streaming metrics from Iceberg tables for dashboard consumption."""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("metrics_collector")


class InMemoryMetricsStore:
    """Thread-safe in-memory metrics store for real-time dashboard updates.

    Used when Iceberg/Spark is not available or for live gauge-style metrics.
    """

    def __init__(self, max_window_seconds: int = 3600) -> None:
        self._lock = threading.Lock()
        self._max_window = max_window_seconds
        self._counters: dict[str, list[tuple[float, float]]] = defaultdict(list)
        self._gauges: dict[str, float] = {}
        self._latest_events: list[dict[str, Any]] = []

    def increment_counter(self, name: str, value: float = 1.0) -> None:
        with self._lock:
            now = time.time()
            self._counters[name].append((now, value))
            self._prune_counter(name, now)

    def _prune_counter(self, name: str, now: float) -> None:
        cutoff = now - self._max_window
        self._counters[name] = [(t, v) for t, v in self._counters[name] if t >= cutoff]

    def get_counter_rate(self, name: str, window_seconds: int = 60) -> float:
        with self._lock:
            now = time.time()
            cutoff = now - window_seconds
            events = [(t, v) for t, v in self._counters.get(name, []) if t >= cutoff]
            if not events:
                return 0.0
            elapsed = now - events[0][0] if len(events) > 1 else window_seconds
            total = sum(v for _, v in events)
            return total / max(elapsed, 1.0)

    def get_counter_total(self, name: str, window_seconds: int = 3600) -> float:
        with self._lock:
            now = time.time()
            cutoff = now - window_seconds
            return sum(v for t, v in self._counters.get(name, []) if t >= cutoff)

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def get_gauge(self, name: str) -> float:
        with self._lock:
            return self._gauges.get(name, 0.0)

    def add_event(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._latest_events.append(event)
            if len(self._latest_events) > 1000:
                self._latest_events = self._latest_events[-500:]

    def get_recent_events(self, count: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            return self._latest_events[-count:]

    def get_all_metrics(self) -> dict[str, Any]:
        with self._lock:
            return {
                "events_per_second": self.get_counter_rate("events", 60),
                "orders_last_hour": self.get_counter_total("orders", 3600),
                "revenue_last_hour": self.get_counter_total("revenue", 3600),
                "cancellation_rate": self.get_gauge("cancellation_rate"),
                "avg_processing_latency_ms": self.get_gauge(
                    "avg_processing_latency_ms"
                ),
                "active_orders": self.get_gauge("active_orders"),
                "total_events_processed": self.get_counter_total(
                    "events", self._max_window
                ),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }


# Global singleton
metrics_store = InMemoryMetricsStore()


def record_event_processed(
    event_type: str, revenue: float = 0.0, latency_ms: float = 0.0
) -> None:
    """Record a processed event in the metrics store."""
    metrics_store.increment_counter("events")
    metrics_store.set_gauge("avg_processing_latency_ms", latency_ms)

    if event_type == "order_created":
        metrics_store.increment_counter("orders")
        metrics_store.increment_counter("revenue", revenue)
    elif event_type == "order_cancelled":
        metrics_store.increment_counter("cancellations")

    # Update cancellation rate
    cancellations = metrics_store.get_counter_total("cancellations", 3600)
    orders = metrics_store.get_counter_total("orders", 3600)
    if orders > 0:
        metrics_store.set_gauge("cancellation_rate", round(cancellations / orders, 4))
