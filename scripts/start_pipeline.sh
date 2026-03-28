#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo " Starting Real-Time Order Pipeline"
echo "============================================"

# 1. Start Docker services
echo ""
echo "[1/6] Starting Docker Compose services..."
cd "$PROJECT_DIR"
docker-compose up -d

# 2. Wait for Kafka to be healthy
echo ""
echo "[2/6] Waiting for Kafka to be ready..."
MAX_WAIT=120
WAITED=0
until docker-compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    sleep 3
    WAITED=$((WAITED + 3))
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "ERROR: Kafka did not become healthy within ${MAX_WAIT}s"
        exit 1
    fi
    echo "  Waiting... (${WAITED}s)"
done
echo "  Kafka is ready."

# 3. Create Kafka topics
echo ""
echo "[3/6] Creating Kafka topics..."
docker-compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic orders.created \
    --partitions 6 --replication-factor 1 \
    --config retention.ms=604800000 2>/dev/null && echo "  Created orders.created" || echo "  orders.created exists"

docker-compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic orders.quarantine \
    --partitions 3 --replication-factor 1 \
    --config retention.ms=2592000000 2>/dev/null && echo "  Created orders.quarantine" || echo "  orders.quarantine exists"

docker-compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic orders.metrics \
    --partitions 3 --replication-factor 1 \
    --config retention.ms=2592000000 2>/dev/null && echo "  Created orders.metrics" || echo "  orders.metrics exists"

# 4. Wait for MinIO + Nessie
echo ""
echo "[4/6] Waiting for MinIO and Nessie..."
WAITED=0
until curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; do
    sleep 2
    WAITED=$((WAITED + 2))
    if [ $WAITED -ge 60 ]; then break; fi
done
echo "  MinIO is ready."

WAITED=0
until curl -sf http://localhost:19120/api/v1/config >/dev/null 2>&1; do
    sleep 2
    WAITED=$((WAITED + 2))
    if [ $WAITED -ge 60 ]; then break; fi
done
echo "  Nessie is ready."

# 5. Start Spark consumer (background)
echo ""
echo "[5/6] Starting Spark Structured Streaming consumer..."
echo "  (Run manually: python -m src.consumer.stream_processor)"
echo "  Or submit to Spark cluster: spark-submit --master spark://localhost:7077 ..."

# 6. Start producer (background or dry-run)
echo ""
echo "[6/6] To start the order producer:"
echo "  Dry run (no Kafka needed): python -m src.producer.order_producer --dry-run --rate 5"
echo "  Live to Kafka:             python -m src.producer.order_producer --rate 10"

echo ""
echo "============================================"
echo " Services started!"
echo "============================================"
echo ""
echo "  Kafka UI:     http://localhost:8080"
echo "  MinIO Console: http://localhost:9001  (admin / password123)"
echo "  Nessie API:   http://localhost:19120"
echo "  Spark Master: http://localhost:8081"
echo ""
echo "  Dashboard:    streamlit run src/monitoring/dashboard.py"
echo "============================================"
