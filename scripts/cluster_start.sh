#!/usr/bin/env bash
# cluster_start.sh — Start Spark standalone cluster + History Server
# Usage: bash scripts/cluster_start.sh
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────
SPARK_HOME="${SPARK_HOME:-$HOME/tools/spark}"
SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR:-$HOME/spark-events}"

# Worker resources — adjust for your hardware
export SPARK_WORKER_CORES=8
export SPARK_WORKER_MEMORY=8g

# ── Pre-flight checks ─────────────────────────────────────────────
if [[ ! -d "$SPARK_HOME" ]]; then
    echo "ERROR: SPARK_HOME=$SPARK_HOME does not exist." >&2
    exit 1
fi

java_version=$(java -version 2>&1 | head -1)
echo "Java: $java_version"
echo "Spark: $SPARK_HOME"
echo "Worker: ${SPARK_WORKER_CORES} cores, ${SPARK_WORKER_MEMORY} memory"

# ── Create event-log directory ─────────────────────────────────────
mkdir -p "$SPARK_EVENTS_DIR"

# ── Start master ───────────────────────────────────────────────────
echo ""
echo "▶ Starting master..."
"$SPARK_HOME/sbin/start-master.sh"

# Give the master a moment to bind
sleep 2

# ── Start worker ───────────────────────────────────────────────────
echo "▶ Starting worker (cores=${SPARK_WORKER_CORES}, mem=${SPARK_WORKER_MEMORY})..."
"$SPARK_HOME/sbin/start-worker.sh" spark://localhost:7077

# ── Start History Server ───────────────────────────────────────────
echo "▶ Starting History Server..."
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://${SPARK_EVENTS_DIR}"
"$SPARK_HOME/sbin/start-history-server.sh"

echo ""
echo "✓ Cluster is running."
echo "  Master UI:         http://localhost:8080"
echo "  Worker UI:         http://localhost:8081"
echo "  History Server:    http://localhost:18080"
echo "  Master URL:        spark://localhost:7077"
echo ""
echo "  Submit jobs with:  spark-submit --master spark://localhost:7077 ..."
