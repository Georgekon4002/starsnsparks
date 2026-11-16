#!/usr/bin/env bash
# run_q1.sh — Run Query 1 experiments
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

echo "═══════ Query 1 — DataFrame (no cache) ═══════"
for run in 1 2 3; do
    echo "── Run $run/3 ──"
    "${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q1.py" \
        --api df --cache none --run-id "$run"
done

echo "═══════ Query 1 — DataFrame (with cache) ═══════"
for run in 1 2 3; do
    echo "── Run $run/3 ──"
    "${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q1.py" \
        --api df --cache filtered --run-id "$run"
done

echo "═══════ Query 1 — SQL (no cache) ═══════"
for run in 1 2 3; do
    echo "── Run $run/3 ──"
    "${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q1.py" \
        --api sql --cache none --run-id "$run"
done

echo "═══════ Query 1 — SQL (with cache) ═══════"
for run in 1 2 3; do
    echo "── Run $run/3 ──"
    "${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q1.py" \
        --api sql --cache filtered --run-id "$run"
done

echo ""
echo "═══════ Query 1 — Cache full review table (expect spill/OOM) ═══════"
"${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q1.py" \
    --api df --cache full --run-id 1

echo ""
echo "✓ Query 1 experiments complete."
echo "  Results in: $PROJECT_DIR/results/q1.csv"
