#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo " Generating Test Dataset"
echo "============================================"

EVENTS=${1:-1000}
OUTPUT_DIR="${PROJECT_DIR}/data/test"

mkdir -p "$OUTPUT_DIR"

echo ""
echo "Generating $EVENTS test events to $OUTPUT_DIR/events.jsonl..."

cd "$PROJECT_DIR"
python -m src.producer.order_producer \
    --rate 1000 \
    --duration 2 \
    --dry-run 2>/dev/null | \
    grep -v '^---$' | \
    python -c "
import sys, json
count = 0
buf = ''
for line in sys.stdin:
    buf += line
    try:
        obj = json.loads(buf)
        print(json.dumps(obj, ensure_ascii=False))
        buf = ''
        count += 1
    except json.JSONDecodeError:
        continue
print(f'# Generated {count} events', file=sys.stderr)
" > "$OUTPUT_DIR/events.jsonl" 2>&1 || true

EVENT_COUNT=$(wc -l < "$OUTPUT_DIR/events.jsonl" | tr -d ' ')
echo "  Generated $EVENT_COUNT events to $OUTPUT_DIR/events.jsonl"

echo ""
echo "============================================"
echo " Test data generated."
echo "============================================"
