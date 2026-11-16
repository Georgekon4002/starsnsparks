#!/usr/bin/env bash
# run_q3.sh — Run Query 3 experiments (join hints, AQE)
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

# Default join strategy
for api in df sql; do
    echo "═══════ Query 3 — $api (default join) ═══════"
    for run in 1 2 3; do
        "${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q3.py" \
            --api "$api" --hint default --run-id "$run"
    done
done

# Join hint experiments (DataFrame only)
for hint in broadcast merge shuffle_hash; do
    echo "═══════ Query 3 — df (hint=$hint) ═══════"
    for run in 1 2 3; do
        "${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q3.py" \
            --api df --hint "$hint" --run-id "$run"
    done
done

# SHUFFLE_REPLICATE_NL — explain only (no execution)
echo "═══════ Query 3 — SHUFFLE_REPLICATE_NL (explain only) ═══════"
"${SUBMIT_COMMON[@]}" "$PROJECT_DIR/src/q3.py" \
    --api df --hint shuffle_replicate_nl --run-id 1 --explain-only

# AQE on vs off (best strategy)
echo "═══════ Query 3 — AQE off ═══════"
for run in 1 2 3; do
    "${SUBMIT_COMMON[@]}" \
        --conf spark.sql.adaptive.enabled=false \
        "$PROJECT_DIR/src/q3.py" \
        --api df --hint default --run-id "$run" --tag aqe_off
done

echo ""
echo "✓ Query 3 experiments complete."
