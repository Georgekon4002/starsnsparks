#!/usr/bin/env bash
# benchmark.sh — Run ALL experiments in sequence
#
# This is the "fire and forget" script. It runs every query's
# experiments one after another. Total time: ~2-4 hours.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "╔══════════════════════════════════════════════════════╗"
echo "║         Stars & Sparks — Full Benchmark              ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

START=$(date +%s)

bash "$SCRIPT_DIR/run_bench_formats.sh"
bash "$SCRIPT_DIR/run_q1.sh"
bash "$SCRIPT_DIR/run_q2.sh"
bash "$SCRIPT_DIR/run_q3.sh"
bash "$SCRIPT_DIR/run_q4.sh"
bash "$SCRIPT_DIR/run_q5.sh"
bash "$SCRIPT_DIR/run_q6.sh"

END=$(date +%s)
ELAPSED=$((END - START))
echo ""
echo "✓ All experiments complete in $((ELAPSED / 60)) minutes."
echo "  Results: results/"
echo "  Event logs: ~/spark-events/"
echo ""
echo "Next: bash scripts/compress_logs.sh"
