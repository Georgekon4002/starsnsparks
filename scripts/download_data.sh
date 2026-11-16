#!/usr/bin/env bash
# download_data.sh — Download GloVe and verify all datasets
# Yelp must be downloaded manually (requires accepting terms).
# ACS acs_zcta.csv is included in the repository.
#
# Usage: bash scripts/download_data.sh
set -euo pipefail

RAW_DIR="${RAW_DIR:-$HOME/advdb/raw}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$RAW_DIR"
cd "$RAW_DIR"

# ── GloVe 6B 100d ─────────────────────────────────────────────────
if [[ -f "glove.6B.100d.txt" ]]; then
    echo "✓ glove.6B.100d.txt already exists, skipping download."
else
    echo "▶ Downloading GloVe 6B (862 MB compressed)..."
    wget -q --show-progress https://nlp.stanford.edu/data/glove.6B.zip
    echo "▶ Extracting glove.6B.100d.txt..."
    unzip -j glove.6B.zip glove.6B.100d.txt
    rm glove.6B.zip
    echo "✓ GloVe downloaded and extracted."
fi

# ── ACS ────────────────────────────────────────────────────────────
if [[ -f "acs_zcta.csv" ]]; then
    echo "✓ acs_zcta.csv already exists."
else
    if [[ -f "$REPO_DIR/acs_zcta.csv" ]]; then
        cp "$REPO_DIR/acs_zcta.csv" "$RAW_DIR/"
        echo "✓ Copied acs_zcta.csv from repository."
    else
        echo "⚠ acs_zcta.csv not found. Download it from helios and place in $RAW_DIR/"
    fi
fi

# ── Yelp ───────────────────────────────────────────────────────────
echo ""
if [[ -f "yelp_academic_dataset_business.json" ]]; then
    echo "✓ Yelp dataset files found."
else
    echo "⚠ Yelp dataset not found."
    echo "  1. Go to https://www.yelp.com/dataset"
    echo "  2. Click 'Download Dataset', accept terms"
    echo "  3. Download the JSON version (yelp_dataset.tar, ~4.35 GB)"
    echo "  4. Move it here and extract:"
    echo "     mv ~/Downloads/yelp_dataset.tar $RAW_DIR/"
    echo "     cd $RAW_DIR && tar -xvf yelp_dataset.tar"
    echo ""
fi

# ── Verify line counts ────────────────────────────────────────────
echo ""
echo "▶ Verifying line counts..."
echo "  (Expected: business=150346, review=6990280, glove=400000, acs=33775)"
echo ""

check_lines() {
    local file="$1"
    local expected="$2"
    local name="$3"
    if [[ -f "$file" ]]; then
        actual=$(wc -l < "$file")
        if (( actual == expected || actual == expected - 1 || actual == expected + 1 )); then
            echo "  ✓ $name: $actual lines (expected ~$expected)"
        else
            echo "  ✗ $name: $actual lines (expected ~$expected) — MISMATCH!"
        fi
    else
        echo "  - $name: file not found"
    fi
}

check_lines "yelp_academic_dataset_business.json" 150346  "business.json"
check_lines "yelp_academic_dataset_review.json"   6990280 "review.json"
check_lines "yelp_academic_dataset_user.json"     1987897 "user.json"
check_lines "glove.6B.100d.txt"                   400000  "glove.6B.100d.txt"
check_lines "acs_zcta.csv"                        33775   "acs_zcta.csv"

echo ""
echo "Done. Once all files are in place, run:  bash scripts/run_etl.sh"
