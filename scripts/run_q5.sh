#!/usr/bin/env bash
# run_q5.sh — Run Query 5 experiments (3 executor configurations)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SPARK_HOME="${SPARK_HOME:-$HOME/tools/spark}"

SUBMIT_BASE=(
    "$SPARK_HOME/bin/spark-submit"
    --master spark://localhost:7077
    --driver-memory 2g
    --conf spark.eventLog.enabled=true
    --conf "spark.eventLog.dir=file://$HOME/spark-events"
)

# Config A: 1 executor × 4 cores, 8 GB
echo "═══════ Query 5 — Config A: 1×(4c, 8GB) ═══════"
for run in 1 2 3; do
    "${SUBMIT_BASE[@]}" \
        --executor-cores 4 --executor-memory 8g --total-executor-cores 4 \
        "$PROJECT_DIR/src/q5.py" --run-id "$run" --tag "1x4c8g"
done

# Config B: 2 executors × 2 cores, 4 GB
echo "═══════ Query 5 — Config B: 2×(2c, 4GB) ═══════"
for run in 1 2 3; do
    "${SUBMIT_BASE[@]}" \
        --executor-cores 2 --executor-memory 4g --total-executor-cores 4 \
        "$PROJECT_DIR/src/q5.py" --run-id "$run" --tag "2x2c4g"
done

# Config C: 4 executors × 1 core, 2 GB
echo "═══════ Query 5 — Config C: 4×(1c, 2GB) ═══════"
for run in 1 2 3; do
    "${SUBMIT_BASE[@]}" \
        --executor-cores 1 --executor-memory 2g --total-executor-cores 4 \
        "$PROJECT_DIR/src/q5.py" --run-id "$run" --tag "4x1c2g"
done

echo ""
echo "✓ Query 5 experiments complete."
