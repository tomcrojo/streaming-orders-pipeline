# Architecture — Real-Time Order Processing Pipeline

## Overview

This pipeline implements an event-driven architecture for processing e-commerce orders in real-time. It demonstrates production-grade streaming data engineering patterns using the Apache ecosystem.

## Event Flow

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────────┐
│  Kafka Producer  │────▶│ Kafka Topics │────▶│ Spark Structured        │
│  (order events)  │     │              │     │ Streaming Consumer      │
│                  │     │ orders.      │     │                         │
│  - order_created │     │  created     │     │ 1. Parse JSON events    │
│  - confirmed     │     │              │     │ 2. Quality checks       │
│  - shipped       │     │ orders.      │     │ 3. Enrich orders        │
│  - delivered     │     │  quarantine  │     │ 4. Windowed aggregations│
│  - cancelled     │     │              │     │ 5. Anomaly detection    │
└─────────────────┘     │ orders.      │     └──────────┬──────────────┘
                         │  metrics     │                │
                         └──────┬───────┘                │
                                │                        ▼
                    ┌───────────┴────────────┐  ┌───────────────────┐
                    │   Iceberg Tables       │  │ Quarantine Topic  │
                    │                        │  │ (invalid records) │
                    │ - order_events         │  └───────────────────┘
                    │ - order_metrics        │
                    │ - order_quality        │  ┌───────────────────┐
                    │ - order_anomalies      │  │ Monitoring        │
                    └────────────────────────┘  │ Dashboard         │
                                                │ (Streamlit)       │
                                                └───────────────────┘
```

## Data Flow Detail

```
Order Created
    │
    ▼
[Parse JSON] ──▶ [Quality Check] ──┬──▶ [Valid] ──▶ [Enrich] ──┬──▶ [Raw Events → Iceberg]
                                    │                             │
                                    │                             ├──▶ [Tumbling Window 1min] ──▶ [Metrics → Iceberg]
                                    │                             │
                                    │                             ├──▶ [Sliding Window 5min/2min] ──▶ [Metrics → Iceberg]
                                    │                             │
                                    │                             └──▶ [Anomaly Detection] ──▶ [Anomalies → Iceberg]
                                    │
                                    └──▶ [Invalid] ──▶ [Quarantine Topic]
                                                          │
                                                          └──▶ [Quality Metrics → Iceberg]
```

## Exactly-Once Semantics

The pipeline achieves exactly-once processing through multiple mechanisms:

1. **Producer side**: `enable.idempotence=true`, `acks=all`, `retries=MAX_INT`
2. **Consumer side**: Spark Structured Streaming checkpointing
3. **Storage side**: Iceberg's ACID transactions with optimistic concurrency

Checkpoint state is stored locally and tracks:
- Kafka offset per partition
- Window aggregation state
- Output commit log

On failure recovery, the consumer replays from the last committed offset.

## Watermarking Strategy

```python
df.withWatermark("event_timestamp", "10 minutes")
```

- **Delay tolerance**: 10 minutes
- **Rationale**: Network delays, clock skew, and out-of-order delivery can cause late arrivals. 10 minutes covers most real-world scenarios without excessive state retention.
- **Late data handling**: Records arriving after the watermark are dropped from windowed aggregations but still written to the raw events table.
- **State cleanup**: Spark automatically cleans up aggregation state older than `window_end + watermark_delay`.

## Schema Evolution

The pipeline handles schema evolution at multiple levels:

### Producer side
- New fields in event payloads are optional (no schema registry required for JSON)
- Consumer uses permissive JSON parsing — unknown fields are preserved in the payload string

### Iceberg side
- `ALTER TABLE ... ADD COLUMN` for new columns
- Existing queries against older snapshots continue to work
- New queries can reference evolved columns

### Spark side
- `spark.sql.streaming.schemaInference` enabled for Kafka JSON
- `failOnDataLoss=false` to handle topic compaction/deletion gracefully

## Failure Handling

| Failure Mode | Detection | Recovery |
|---|---|---|
| Kafka broker down | Connection timeout | Producer retries (exponential backoff) |
| Spark executor crash | YARN/K8s restart | Checkpoint recovery, replay from offset |
| Iceberg write failure | Spark streaming exception | Checkpoint not advanced, retry on next micro-batch |
| Bad data | Quality checks | Quarantined to separate topic |
| Network partition | Heartbeat timeout | Consumer group rebalance |

## Scaling Considerations

### Kafka
- Partitions determine consumer parallelism (6 partitions = up to 6 concurrent consumers)
- Increase partitions for higher throughput

### Spark
- `maxOffsetsPerTrigger` controls micro-batch size
- Add workers for horizontal scaling
- Tune `spark.sql.shuffle.partitions` for aggregation parallelism

### Iceberg
- Small file compaction runs periodically (128MB target)
- Snapshot expiration removes old versions (7-day retention)
- Partition pruning by `hours(event_timestamp)` accelerates queries

## Production Cost Estimation (AWS)

| Component | Instance | Monthly Cost |
|---|---|---|
| MSK (Kafka) | 3x kafka.m5.large | ~$650 |
| EMR (Spark) | 1x master + 2x worker (m5.xlarge) | ~$500 |
| S3 (Iceberg storage) | 500GB | ~$12 |
| Glue (Nessie alt.) | Catalog + Crawlers | ~$30 |
| **Total** | | **~$1,200/month** |

For GCP equivalent: use Confluent Cloud or Confluent for Kafka, Dataproc for Spark, and BigLake for Iceberg.

## Key Design Decisions

1. **KRaft over Zookeeper**: Kafka 3.7+ supports KRaft mode, eliminating the Zookeeper dependency. Simpler ops, faster startup.

2. **Nessie catalog**: Git-like versioning for Iceberg tables. Supports branching — useful for testing schema changes in isolation.

3. **MinIO over S3**: Local development without AWS credentials. Production uses actual S3/GCS.

4. **JSON over Avro**: Simpler local development (no schema registry). Production would use Avro + Schema Registry for type safety and evolution.

5. **Streamlit dashboard**: Lightweight, no external dependencies. Production would use Grafana + Prometheus for alerting.
