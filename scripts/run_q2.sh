#!/usr/bin/env bash
# run_q2.sh — Run Query 2 experiments
# Config: 4 executors × (1 core, 2 GB)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SPARK_HOME="${SPARK_HOME:-$HOME/tools/spark}"

SUBMIT_COMMON=(
    "$SPARK_HOME/bin/spark-submit"
    --master spark://localhost:7077
    --driver-memory 2g
    --executor-cores 1
    --executor-memory 2g
    --total-executor-cores 4
    --conf spark.eventLog.enabled=true
    --conf "spark.eventLog.dir=file://$HOME/spark-events"
)

for api in df sql; do
    echo "═══════ Query 2 — $api ═══════"
    for run in 1 2 3; do
        echo "── Run $run/3 ──"
        "${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q2.py" \
            --api "$api" --run-id "$run"
    done
done

echo ""
echo "✓ Query 2 experiments complete."
