#!/usr/bin/env bash
# cluster_stop.sh — Stop Spark standalone cluster + History Server
# Usage: bash scripts/cluster_stop.sh
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-$HOME/tools/spark}"

echo "▶ Stopping History Server..."
"$SPARK_HOME/sbin/stop-history-server.sh" 2>/dev/null || true

echo "▶ Stopping worker..."
"$SPARK_HOME/sbin/stop-worker.sh" 2>/dev/null || true

echo "▶ Stopping master..."
"$SPARK_HOME/sbin/stop-master.sh" 2>/dev/null || true

echo ""
echo "✓ Cluster stopped."
