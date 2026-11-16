#!/usr/bin/env bash
# compress_logs.sh — Compress Spark event logs and copy to event_logs/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SPARK_EVENTS="${SPARK_EVENTS_DIR:-$HOME/spark-events}"
DEST="$PROJECT_DIR/event_logs"

mkdir -p "$DEST"

echo "▶ Compressing event logs from $SPARK_EVENTS → $DEST"

count=0
for log in "$SPARK_EVENTS"/*; do
    if [[ -f "$log" ]]; then
        name=$(basename "$log")
        if [[ ! -f "$DEST/${name}.gz" ]]; then
            gzip -c "$log" > "$DEST/${name}.gz"
            echo "  ✓ $name → ${name}.gz"
            count=$((count + 1))
        fi
    fi
done

echo ""
echo "✓ Compressed $count new event log(s)."
echo "  Don't forget: git add event_logs/ && git commit -m 'Add event logs'"
