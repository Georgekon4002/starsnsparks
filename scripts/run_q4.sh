#!/usr/bin/env bash
# run_q4.sh — Run Query 4 experiments (nearest-neighbor, scaling)
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

# Version (a): without NEAREST BY — 4 exec × (1c, 2GB)
echo "═══════ Query 4(a) — without NEAREST BY ═══════"
for run in 1 2 3; do
    "${SUBMIT_BASE[@]}" \
        --executor-cores 1 --executor-memory 2g --total-executor-cores 4 \
        "$PROJECT_DIR/src/q4.py" --version no_nearest --run-id "$run"
done

# Version (b): with NEAREST BY — 4 exec × (1c, 2GB)
echo "═══════ Query 4(b) — with NEAREST BY ═══════"
for run in 1 2 3; do
    "${SUBMIT_BASE[@]}" \
        --executor-cores 1 --executor-memory 2g --total-executor-cores 4 \
        "$PROJECT_DIR/src/q4.py" --version nearest --run-id "$run"
done

# Scaling test: 2 exec × 3GB, varying cores (1, 2, 4)
for cores in 1 2 4; do
    total=$((2 * cores))
    echo "═══════ Query 4 — scaling: 2 exec × ${cores}c × 3GB ═══════"
    for run in 1 2 3; do
        "${SUBMIT_BASE[@]}" \
            --executor-cores "$cores" --executor-memory 3g \
            --total-executor-cores "$total" \
            "$PROJECT_DIR/src/q4.py" --version nearest --run-id "$run" \
            --tag "scale_${cores}c"
    done
done

echo ""
echo "✓ Query 4 experiments complete."
