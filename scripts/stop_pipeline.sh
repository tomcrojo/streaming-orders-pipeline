#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo " Stopping Real-Time Order Pipeline"
echo "============================================"

cd "$PROJECT_DIR"

# Stop Spark streaming queries gracefully if running
echo ""
echo "[1/3] Stopping Spark streaming queries..."
# Queries are stopped via signal handler in stream_processor.py

# Stop producer if running
echo ""
echo "[2/3] Stopping producer..."
pkill -f "src.producer.order_producer" 2>/dev/null && echo "  Producer stopped." || echo "  No producer running."

# Stop Docker services
echo ""
echo "[3/3] Stopping Docker Compose services..."
docker-compose down

echo ""
echo "============================================"
echo " Pipeline stopped."
echo "============================================"
echo ""
echo "To remove volumes (clears all data): docker-compose down -v"
