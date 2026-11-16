"""
etl_prepare.py — Requirement 0: Convert raw data to Parquet.

Converts:
  1. Yelp business.json  → parquet/business/
  2. Yelp review.json    → parquet/review/  (partitioned by year)
  3. Yelp user.json      → parquet/user/
  4. GloVe 6B 100d       → parquet/glove/   (word STRING, vec ARRAY<FLOAT>)
  5. ACS acs_zcta.csv    → parquet/acs/     (zcta as STRING)

Usage:
    spark-submit src/etl_prepare.py

Design decisions:
  - We declare explicit schemas (no inference) so Spark doesn't scan
    the full file just to figure out types, and so we control exactly
    which fields and types we work with.
  - Reviews are partitioned by year because multiple queries filter on
    year ranges; partition pruning avoids reading unneeded data.
  - attributes and hours are read as plain STRING.  At query time we
    call parse_json() to get a VARIANT.  This avoids schema-inference
    issues and nested-struct explosions during ETL.
  - GloVe is read as text (not CSV) because some "words" are bare
    punctuation or quotation marks that confuse CSV parsing (Tip 9).
    We split each line, take the first token as the word, and cast
    the remaining 100 tokens to ARRAY<FLOAT>.
  - ACS zcta is STRING to preserve leading zeros (Tip 6).
"""

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

# Allow running from repo root: python src/etl_prepare.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from common import (
    get_spark, RAW_DIR, PARQUET_DIR,
    BUSINESS_SCHEMA, REVIEW_SCHEMA, USER_SCHEMA, ACS_SCHEMA,
)


def convert_business(spark):
    """Read Yelp business JSON with explicit schema, write Parquet."""
    path_in  = os.path.join(RAW_DIR, "yelp_academic_dataset_business.json")
    path_out = os.path.join(PARQUET_DIR, "business")

    print(f"\n▶ Business: {path_in} → {path_out}")
    df = spark.read.json(path_in, schema=BUSINESS_SCHEMA)
    print(f"  Rows: {df.count()}")
    df.write.mode("overwrite").parquet(path_out)
    print(f"  ✓ Written to {path_out}")


def convert_review(spark):
    """Read Yelp review JSON, add year column, write Parquet partitioned by year."""
    path_in  = os.path.join(RAW_DIR, "yelp_academic_dataset_review.json")
    path_out = os.path.join(PARQUET_DIR, "review")

    print(f"\n▶ Review: {path_in} → {path_out}")
    df = spark.read.json(path_in, schema=REVIEW_SCHEMA)

    # Extract year from the date string for partitioning.
    # The date field is "YYYY-MM-DD HH:MM:SS"; year() works on strings
    # that Spark can parse as timestamps/dates.
    df = df.withColumn("year", F.year(F.col("date")))

    print(f"  Rows: {df.count()}")
    df.write.mode("overwrite").partitionBy("year").parquet(path_out)
    print(f"  ✓ Written to {path_out} (partitioned by year)")


def convert_user(spark):
    """Read Yelp user JSON with explicit schema, write Parquet."""
    path_in  = os.path.join(RAW_DIR, "yelp_academic_dataset_user.json")
    path_out = os.path.join(PARQUET_DIR, "user")

    print(f"\n▶ User: {path_in} → {path_out}")
    df = spark.read.json(path_in, schema=USER_SCHEMA)
    print(f"  Rows: {df.count()}")
    df.write.mode("overwrite").parquet(path_out)
    print(f"  ✓ Written to {path_out}")


def convert_glove(spark):
    """
    Read GloVe 6B 100d as text, parse into (word, vec ARRAY<FLOAT>).

    Each line: "word 0.123 -0.456 ... " (101 space-separated tokens).
    We split on whitespace, take element 0 as the word, and cast
    elements 1..100 to ARRAY<FLOAT> using slice + transform.

    Why text and not CSV?  Tip 9: some "words" are quotation marks
    or other punctuation that break CSV parsing.
    """
    path_in  = os.path.join(RAW_DIR, "glove.6B.100d.txt")
    path_out = os.path.join(PARQUET_DIR, "glove")

    print(f"\n▶ GloVe: {path_in} → {path_out}")

    raw = spark.read.text(path_in)

    # Split each line into an array of strings
    parts = raw.select(F.split(F.col("value"), " ").alias("parts"))

    # First element is the word; elements 2..101 (1-indexed) are the vector
    df = parts.select(
        F.col("parts")[0].alias("word"),
        # slice(parts, 2, 100) takes elements at 1-based positions 2..101
        # transform casts each string element to float
        F.transform(
            F.slice(F.col("parts"), 2, 100),
            lambda x: x.cast(FloatType())
        ).alias("vec"),
    )

    print(f"  Rows: {df.count()}")
    df.write.mode("overwrite").parquet(path_out)
    print(f"  ✓ Written to {path_out}")


def convert_acs(spark):
    """
    Read ACS ZCTA CSV with zcta as STRING (Tip 6: preserve leading zeros).

    The CSV uses ';' as delimiter and has a header row.
    """
    path_in  = os.path.join(RAW_DIR, "acs_zcta.csv")
    path_out = os.path.join(PARQUET_DIR, "acs")

    print(f"\n▶ ACS: {path_in} → {path_out}")
    df = spark.read.csv(path_in, schema=ACS_SCHEMA, sep=";", header=True)
    print(f"  Rows: {df.count()}")
    df.write.mode("overwrite").parquet(path_out)
    print(f"  ✓ Written to {path_out}")


def report_sizes():
    """Print disk sizes for JSON vs Parquet comparison."""
    import subprocess

    print("\n── Disk sizes ──────────────────────────────────────")
    datasets = {
        "business": ("yelp_academic_dataset_business.json", "business"),
        "review":   ("yelp_academic_dataset_review.json",   "review"),
        "user":     ("yelp_academic_dataset_user.json",     "user"),
    }
    for name, (json_file, pq_dir) in datasets.items():
        json_path = os.path.join(RAW_DIR, json_file)
        pq_path   = os.path.join(PARQUET_DIR, pq_dir)
        try:
            json_size = os.path.getsize(json_path)
        except OSError:
            json_size = 0
        try:
            pq_size = int(subprocess.check_output(
                ["du", "-sb", pq_path], text=True
            ).split()[0])
        except Exception:
            pq_size = 0

        json_mb = json_size / (1024 * 1024)
        pq_mb   = pq_size / (1024 * 1024)
        ratio   = json_size / pq_size if pq_size else 0
        print(f"  {name:12s}  JSON: {json_mb:8.1f} MB   "
              f"Parquet: {pq_mb:8.1f} MB   ratio: {ratio:.1f}×")


def main():
    spark = get_spark("ETL-prepare")

    convert_business(spark)
    convert_review(spark)
    convert_user(spark)
    convert_glove(spark)
    convert_acs(spark)
    report_sizes()

    spark.stop()
    print("\n✓ ETL complete.")


if __name__ == "__main__":
    main()
