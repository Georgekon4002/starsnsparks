#!/usr/bin/env bash
# run_q6.sh — Run Query 6 experiments
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

echo "═══════ Query 6 — NEAREST BY ═══════"
for run in 1 2 3; do
    "${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q6.py" \
        --version nearest --run-id "$run"
done

echo "═══════ Query 6 — ORDER BY LIMIT (for plan comparison) ═══════"
"${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q6.py" \
    --version orderby --run-id 1

echo ""
echo "✓ Query 6 experiments complete."
