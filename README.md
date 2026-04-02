# Real-Time Order Processing Pipeline

Event-driven data pipeline processing e-commerce orders in real-time using Apache Kafka, Spark Structured Streaming, and Apache Iceberg. Demonstrates streaming data engineering patterns including event sourcing, exactly-once processing, schema evolution, and time-travel queries.

## Architecture

```
                                    ┌─────────────────────────────────────────────────────────┐
                                    │                   Spark Structured Streaming             │
                                    │  ┌─────────┐   ┌──────────┐   ┌──────────┐   ┌──────┐ │
┌───────────────┐   ┌──────────┐   │  │ Parse   │──▶│ Quality  │──▶│ Enrich   │──▶│Agg   │ │   ┌─────────────┐
│  Kafka        │──▶│ Kafka    │──▶│  │ Events  │   │ Checks   │   │ Orders   │   │Windows│ │──▶│ Iceberg     │
│  Producer     │   │ Topics   │   │  └─────────┘   └────┬─────┘   └──────────┘   └──────┘ │   │ Tables      │
│              │   │          │   │                      │                                     │   │             │
│ • order_     │   │ • orders │   │                      ▼                                     │   │ • order_    │
│   created    │   │   .create│   │               ┌─────────────┐                              │   │   events    │
│ • confirmed  │   │ • orders │   │               │ Quarantine  │──────────────────────────────│──▶│ • order_    │
│ • shipped    │   │   .quar  │   │               │ Topic       │                              │   │   metrics   │
│ • delivered  │   │ • orders │   │               └─────────────┘                              │   │ • order_    │
│ • cancelled  │   │   .metr  │   │                                                            │   │   quality   │
└───────────────┘   └──────────┘   └─────────────────────────────────────────────────────────┘   │ • order_    │
                                                                                                 │   anomalies │
                                                    ┌──────────────────────────────┐             └─────────────┘
                                                    │    Monitoring Dashboard       │                    │
                                                    │    (Streamlit + Plotly)        │◀──────────────────┘
                                                    └──────────────────────────────┘
```

### Data Flow

```
Order Created
    │
    ▼
[Parse JSON] ──▶ [Quality Check] ──┬──▶ [Valid] ──▶ [Enrich] ──┬──▶ [Raw Events → Iceberg]
                                    │                             ├──▶ [1-min Tumbling] ──▶ [Metrics]
                                    │                             ├──▶ [5-min Sliding] ──▶ [Metrics]
                                    │                             └──▶ [Anomaly Detection] ──▶ [Anomalies]
                                    │
                                    └──▶ [Invalid] ──▶ [Quarantine]
```

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- 8GB+ RAM available for Docker

### 1. Start Infrastructure

```bash
chmod +x scripts/*.sh
./scripts/start_pipeline.sh
```

This starts:
- **Kafka** (KRaft mode, no Zookeeper) on `localhost:9092`
- **Kafka UI** on `http://localhost:8080`
- **MinIO** (S3-compatible) on `localhost:9000` / console on `localhost:9001`
- **Nessie** (Iceberg catalog) on `localhost:19120`
- **Spark Master** on `localhost:8081`
- **Spark Worker**

### 2. Run the Producer

```bash
# Dry-run (prints events to stdout, no Kafka needed)
python -m src.producer.order_producer --dry-run --rate 5

# Live to Kafka
python -m src.producer.order_producer --rate 10
```

### 3. Run the Stream Processor

```bash
python -m src.consumer.stream_processor --checkpoint-dir checkpoints/streaming
```

### 4. Open the Dashboard

```bash
streamlit run src/monitoring/dashboard.py
```

### 5. Stop Everything

```bash
./scripts/stop_pipeline.sh
```

## Key Concepts

### Event Sourcing

Every state change in the order lifecycle is captured as an immutable event. The current state of an order is derived by replaying events in sequence. This provides a complete audit trail and enables temporal queries.

### Exactly-Once Semantics

The pipeline achieves end-to-end exactly-once processing through:
- **Producer**: Idempotent writes (`enable.idempotence=true`, `acks=all`)
- **Consumer**: Spark checkpointing tracks Kafka offsets and output commit state
- **Storage**: Iceberg ACID transactions with optimistic concurrency control

### Watermarking

Spark Structured Streaming uses watermarks to handle late-arriving data:

```python
df.withWatermark("event_timestamp", "10 minutes")
```

Records arriving within 10 minutes of the window boundary are included in the aggregation. Late records beyond the watermark are dropped from windowed computations but still persisted in the raw events table.

### Schema Evolution

- New optional fields in JSON events are handled gracefully (permissive parsing)
- Iceberg supports `ALTER TABLE ADD COLUMN` without rewriting data
- Time-travel queries can access historical schemas via snapshot IDs

## Tech Stack

| Technology | Version | Role |
|---|---|---|
| Apache Kafka | 3.7 | Event streaming backbone (KRaft mode) |
| Apache Spark | 3.5 | Structured Streaming consumer & processor |
| Apache Iceberg | 1.4 | Lakehouse table format with time travel |
| Nessie | 0.82 | Iceberg catalog (git-like branching) |
| MinIO | latest | S3-compatible object storage |
| Python | 3.11+ | Producer, consumer, dashboard |
| Streamlit | 1.30+ | Monitoring dashboard |
| Plotly | 5.18+ | Interactive charts |
| Docker Compose | 3.9 | Local dev environment |

## Project Structure

```
streaming-orders-pipeline/
├── src/
│   ├── producer/           # Kafka order event generator
│   │   ├── order_producer.py
│   │   ├── schemas.py
│   │   └── config.py
│   ├── consumer/           # Spark Structured Streaming
│   │   ├── stream_processor.py
│   │   ├── transformations.py
│   │   ├── quality_checks.py
│   │   └── config.py
│   ├── storage/            # Iceberg table management
│   │   ├── iceberg_writer.py
│   │   └── table_definitions.py
│   └── monitoring/         # Streamlit dashboard
│       ├── metrics_collector.py
│       └── dashboard.py
├── config/                 # YAML configuration
├── scripts/                # Start/stop/data generation
├── tests/                  # Unit tests
├── docs/                   # Architecture documentation
└── docker-compose.yml      # Full local environment
```

## Producer Details

The producer generates realistic Spanish e-commerce data:
- **10,000 simulated customers** with Spanish names and cities
- **500 products** across 5 categories (Electrónica, Hogar, Ropa, Deportes, Libros)
- **EUR pricing** with realistic ranges
- **Order lifecycle**: created → confirmed → shipped → delivered (with cancellation at any step)
- **Configurable rate**: `--rate` events per second (default: 10)
- **Dry-run mode**: `--dry-run` prints to stdout without Kafka

## Consumer Details

Spark Structured Streaming pipeline:
- **JSON parsing** with nested schema support
- **Quality checks**: null IDs, invalid types, revenue bounds, timestamp range
- **Enrichment**: hour/day-of-week extraction, revenue buckets, weekend flags
- **Windowed aggregations**: 1-min tumbling, 5-min sliding (2-min slide)
- **Anomaly detection**: cancellation spikes (>30%), large orders (>EUR 2000)
- **Quarantine sink**: invalid records → `orders.quarantine` topic

## Configuration

All configuration is in `config/` YAML files:
- `kafka_topics.yaml` — topic definitions and retention
- `spark_config.yaml` — Spark/Iceberg/Kafka settings
- `quality_rules.yaml` — data quality validation rules

Environment variables override defaults:
- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `CHECKPOINT_DIR` (default: `checkpoints/streaming`)
- `PRODUCER_RATE` (default: `10`)

## Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_producer.py -v

# Lint
ruff check src/ tests/
ruff format --check src/ tests/
```

## Data Quality

Quality checks run inline during streaming processing — invalid records are quarantined before they reach Iceberg tables.

### Validation Rules

| Check | Rule | Action |
|-------|------|--------|
| Null order ID | `order_id IS NULL` | Quarantine |
| Invalid total | `total_amount IS NULL` or `<= 0` on `order_created` | Quarantine |
| Unknown event type | `event_type NOT IN valid_types` | Quarantine |
| Revenue bounds | `total_amount < 0.01` or `> 50000` | Quarantine |
| Future timestamp | `event_timestamp > now + 5 min` | Quarantine |
| Stale timestamp | `event_timestamp < now - 24 hours` | Quarantine |

### Quality Metrics

Per micro-batch metrics are computed and written to an Iceberg table (`order_quality`):

- `records_checked` — total records in the batch
- `records_valid` — passed all checks
- `records_invalid` — sent to quarantine
- `quarantine_rate` — `invalid / checked` (alert if > 5%)

### Quarantine Topic

Invalid records are written to Kafka topic `orders.quarantine` for manual inspection and replay. The quarantine stream preserves the original message plus a `failure_reason` field.

## Anomaly Detection

Two anomaly detectors run on the enriched stream:

| Detector | Threshold | Window | Description |
|----------|-----------|--------|-------------|
| Cancellation spike | > 30% cancellation rate | 1-minute tumbling, by region | Indicates fraud or system issues |
| Large order | > EUR 2,000 | Per event | Flags unusually high-value orders for review |

Anomalies are written to the `order_anomalies` Iceberg table with timestamp, region, anomaly type, and the triggering value.

## Iceberg Tables

| Table | Description | Partitioning |
|-------|-------------|-------------|
| `order_events` | All enriched order events | `order_date` |
| `order_metrics` | Windowed aggregations (1-min tumbling) | `window.start` |
| `order_quality` | Per-batch quality metrics | `check_timestamp` |
| `order_anomalies` | Detected anomalies | `event_timestamp` |

### Time Travel

Iceberg supports time-travel queries for debugging and auditing:

```sql
-- Query events as of 1 hour ago
SELECT * FROM nessie.default.order_events
FOR SYSTEM_TIME AS OF (CURRENT_TIMESTAMP - INTERVAL '1' HOUR)

-- List snapshots
SELECT * FROM nessie.default.order_events.snapshots
```

## Local Development with Docker

Docker Compose spins up the full streaming stack locally.

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Local Docker Environment                                    │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌──────────────┐  │
│  │  Kafka   │  │  MinIO   │  │ Nessie │  │ Spark Master │  │
│  │ (KRaft)  │  │ (S3mock) │  │(Iceberg│  │  + Worker    │  │
│  │          │  │          │  │catalog)│  │              │  │
│  └──────────┘  └──────────┘  └────────┘  └──────────────┘  │
│                                                              │
│  ┌──────────────┐                                           │
│  │  Kafka UI    │  (http://localhost:8080)                  │
│  └──────────────┘                                           │
└──────────────────────────────────────────────────────────────┘
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka | localhost:9092 | — |
| Kafka UI | http://localhost:8080 | — |
| MinIO S3 | localhost:9000 | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Nessie Catalog | localhost:19120 | — |
| Spark Master UI | http://localhost:8081 | — |

### Tear Down

```bash
# Stop services (preserves data)
./scripts/stop_pipeline.sh

# Stop and delete all data
docker compose down -v
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Kafka connection refused | Ensure `./scripts/start_pipeline.sh` completed; check `docker compose ps` |
| Spark job OOM | Increase Docker memory to 8GB+; reduce `maxOffsetsPerTrigger` in config |
| Iceberg table not found | Run `create_tables_if_not_exists` or restart the stream processor |
| Checkpoint errors | Delete the `checkpoints/` directory and restart (loses state) |
| MinIO access denied | Verify `minioadmin` credentials in `config/spark_config.yaml` |
| Dashboard shows no data | Confirm the producer is running and events are in Kafka (`docker compose logs kafka`) |

## License

MIT
