"""
bench_formats.py — Requirement 0: Compare JSON vs Parquet performance.

Query: "average review stars per year"
Three variants:
  (a) JSON with schema inference
  (b) JSON with explicit schema
  (c) Parquet (partitioned by year)

For each variant we:
  1. Print explain(mode="formatted") to see ReadSchema, PartitionFilters,
     PushedFilters.
  2. Measure wall-clock time with write.format("noop").

This script runs ONE iteration.  The measurement protocol requires ≥ 3
runs in separate Spark applications.  Use scripts/run_bench_formats.sh
to do that.

Usage:
    spark-submit src/bench_formats.py --variant {json_infer|json_explicit|parquet} [--run-id N]

Design decisions:
  - We use write.format("noop") instead of collect() because the result
    (one row per year) is small enough for collect(), but noop is the
    canonical action recommended by the assignment for consistency.
  - Each variant loads data from scratch in its own Spark application
    to avoid caching effects.
"""

import argparse
import os
import sys
import time

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from common import get_spark, RAW_DIR, PARQUET_DIR, REVIEW_SCHEMA, timed_run


def avg_stars_per_year(df):
    """Core query: average review stars grouped by year."""
    return (
        df
        .withColumn("year", F.year(F.col("date")))
        .groupBy("year")
        .agg(F.avg("stars").alias("avg_stars"))
        .orderBy("year")
    )


def run_json_infer(spark):
    """Variant (a): JSON with schema inference."""
    path = os.path.join(RAW_DIR, "yelp_academic_dataset_review.json")
    print(f"\n── JSON (schema inference) ── {path}")

    df = spark.read.json(path)  # no schema → Spark infers
    result = avg_stars_per_year(df)

    print("\n[explain]")
    result.explain(mode="formatted")

    elapsed = timed_run(
        lambda: result.write.format("noop").mode("overwrite").save(),
        label="json_infer"
    )
    return elapsed


def run_json_explicit(spark):
    """Variant (b): JSON with explicit schema."""
    path = os.path.join(RAW_DIR, "yelp_academic_dataset_review.json")
    print(f"\n── JSON (explicit schema) ── {path}")

    df = spark.read.json(path, schema=REVIEW_SCHEMA)
    result = avg_stars_per_year(df)

    print("\n[explain]")
    result.explain(mode="formatted")

    elapsed = timed_run(
        lambda: result.write.format("noop").mode("overwrite").save(),
        label="json_explicit"
    )
    return elapsed


def run_parquet(spark):
    """Variant (c): Parquet (pre-partitioned by year)."""
    path = os.path.join(PARQUET_DIR, "review")
    print(f"\n── Parquet ── {path}")

    df = spark.read.parquet(path)
    # In the Parquet version the year column already exists (partition column).
    result = (
        df
        .groupBy("year")
        .agg(F.avg("stars").alias("avg_stars"))
        .orderBy("year")
    )

    print("\n[explain]")
    result.explain(mode="formatted")

    elapsed = timed_run(
        lambda: result.write.format("noop").mode("overwrite").save(),
        label="parquet"
    )
    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Format benchmark (R0)")
    parser.add_argument(
        "--variant",
        choices=["json_infer", "json_explicit", "parquet"],
        required=True,
        help="Which data-loading variant to benchmark.",
    )
    parser.add_argument(
        "--run-id", type=int, default=1,
        help="Run index (for unique appName in event logs).",
    )
    args = parser.parse_args()

    app_name = f"R0-bench-{args.variant}-run{args.run_id}"
    spark = get_spark(app_name)

    dispatch = {
        "json_infer":   run_json_infer,
        "json_explicit": run_json_explicit,
        "parquet":      run_parquet,
    }

    elapsed = dispatch[args.variant](spark)

    # Write timing to results/ for later aggregation
    results_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "results"
    )
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, "bench_formats.csv"), "a") as f:
        f.write(f"{args.variant},{args.run_id},{elapsed:.3f}\n")

    spark.stop()


if __name__ == "__main__":
    main()
