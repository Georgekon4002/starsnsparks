#!/usr/bin/env bash
# run_bench_formats.sh — Run the format benchmark (Requirement 0)
#
# Runs each variant 3 times in separate Spark applications,
# then prints the summary (median + range).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SPARK_HOME="${SPARK_HOME:-$HOME/tools/spark}"
RESULTS_FILE="$PROJECT_DIR/results/bench_formats.csv"

# Clear previous results
mkdir -p "$PROJECT_DIR/results"
> "$RESULTS_FILE"

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

for variant in json_infer json_explicit parquet; do
    for run in 1 2 3; do
        echo ""
        echo "════════════════════════════════════════════════════"
        echo "  $variant — run $run / 3"
        echo "════════════════════════════════════════════════════"
        "${SUBMIT_COMMON[@]}" \
            "$PROJECT_DIR/src/bench_formats.py" \
            --variant "$variant" --run-id "$run"
    done
done

echo ""
echo "── Results ──────────────────────────────────────────"
cat "$RESULTS_FILE"
echo ""
echo "Compute median & range with:"
echo "  python3 -c \"import statistics, csv"
echo "  data = list(csv.reader(open('$RESULTS_FILE')))"
echo "  for v in ['json_infer','json_explicit','parquet']:"
echo "      times = [float(r[2]) for r in data if r[0]==v]"
echo "      print(f'{v}: median={statistics.median(times):.3f}s  range={max(times)-min(times):.3f}s')\""
