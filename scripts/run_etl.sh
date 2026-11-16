#!/usr/bin/env bash
# run_etl.sh — spark-submit the ETL pipeline (Requirement 0)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SPARK_HOME="${SPARK_HOME:-$HOME/tools/spark}"

"$SPARK_HOME/bin/spark-submit" \
    --master spark://localhost:7077 \
    --driver-memory 2g \
    --executor-cores 2 \
    --executor-memory 2g \
    --total-executor-cores 4 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=file://$HOME/spark-events \
    "$PROJECT_DIR/src/etl_prepare.py"
