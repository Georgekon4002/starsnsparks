"""
q1.py — Query 1: Review Length Categories (15%)

Filter: reviews 2015-2021 with useful >= 1.
Categorize by text length (characters):
  Short        < 250
  Medium       250 – 749
  Long         750 – 1999
  Very Long    ≥ 2000

(a) Per category (desc by avg useful): count, avg stars, avg useful.
(b) Per year × category: percentage of that year's reviews in each category.

Both DataFrame and SQL implementations.

Usage:
    spark-submit src/q1.py --api {df|sql} --cache {none|filtered|full} --run-id N

Design decisions:
  - length() counts characters (not bytes) — Tip 2.
  - Year filter should leverage PartitionFilters on the Parquet review table.
  - The useful >= 1 filter should appear as a PushedFilter on Parquet.
  - When --cache filtered: we cache the filtered+categorized DataFrame before
    running (a) and (b) sequentially, to show the benefit of avoiding re-reads.
  - When --cache full: we cache the ENTIRE review table with all columns
    (including text). With 4 exec × 2GB = 8 GB total executor memory, the full
    review Parquet is too large to fit → expect disk spill or OOM.
"""

import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.insert(0, os.path.dirname(__file__))
from common import get_spark, PARQUET_DIR, timed_run


def categorize_length_df(df):
    """Add a 'length_category' column based on character count of 'text'."""
    return df.withColumn(
        "length_category",
        F.when(F.length("text") < 250, "Short")
         .when(F.length("text") < 750, "Medium")
         .when(F.length("text") < 2000, "Long")
         .otherwise("Very Long")
    )


def query1_df(spark, review_path, do_cache="none"):
    """DataFrame API implementation of Query 1."""

    reviews = spark.read.parquet(review_path)

    # ── Filters ────────────────────────────────────────────────────
    # year is a partition column → PartitionFilter
    # useful >= 1 → PushedFilter (predicate pushdown on Parquet)
    filtered = (
        reviews
        .filter((F.col("year") >= 2015) & (F.col("year") <= 2021))
        .filter(F.col("useful") >= 1)
    )

    # Add length category
    categorized = categorize_length_df(filtered)

    # ── Print explain for the report ───────────────────────────────
    print("\n[explain — Query 1 DF]")
    categorized.select("year", "stars", "useful", "length_category") \
        .explain(mode="formatted")

    # ── Cache experiment ───────────────────────────────────────────
    if do_cache == "filtered":
        # Cache the filtered+categorized DataFrame (without 'text')
        cached = categorized.select(
            "year", "stars", "useful", "length_category"
        ).cache()
        # Materialize the cache
        cached.count()
        work = cached
    elif do_cache == "full":
        # Cache the ENTIRE review table — expect OOM/spill
        print("\n⚠  Caching FULL review table (with text). This may OOM.")
        cached_full = reviews.cache()
        cached_full.count()
        # Then apply filters
        work = categorize_length_df(
            cached_full
            .filter((F.col("year") >= 2015) & (F.col("year") <= 2021))
            .filter(F.col("useful") >= 1)
        )
    else:
        work = categorized

    # ── Part (a): Per category stats ───────────────────────────────
    print("\n── Part (a): per-category stats ──")

    part_a = (
        work
        .groupBy("length_category")
        .agg(
            F.count("*").alias("review_count"),
            F.avg("stars").alias("avg_stars"),
            F.avg("useful").alias("avg_useful"),
        )
        .orderBy(F.desc("avg_useful"))
    )

    elapsed_a = timed_run(
        lambda: part_a.collect(),
        label="Q1a-df"
    )
    part_a.show(truncate=False)

    # ── Part (b): Per year × category percentage ───────────────────
    print("\n── Part (b): year × category percentages ──")

    year_cat_counts = (
        work
        .groupBy("year", "length_category")
        .agg(F.count("*").alias("cat_count"))
    )

    year_totals = (
        work
        .groupBy("year")
        .agg(F.count("*").alias("year_total"))
    )

    part_b = (
        year_cat_counts
        .join(year_totals, "year")
        .withColumn("percentage", F.round(F.col("cat_count") / F.col("year_total") * 100, 2))
        .select("year", "length_category", "cat_count", "percentage")
        .orderBy("year", "length_category")
    )

    elapsed_b = timed_run(
        lambda: part_b.collect(),
        label="Q1b-df"
    )
    part_b.show(50, truncate=False)

    # ── Cleanup ────────────────────────────────────────────────────
    if do_cache in ("filtered", "full"):
        spark.catalog.clearCache()

    return elapsed_a, elapsed_b


def query1_sql(spark, review_path, do_cache="none"):
    """SQL implementation of Query 1."""

    reviews = spark.read.parquet(review_path)
    reviews.createOrReplaceTempView("reviews")

    if do_cache == "filtered":
        spark.sql("""
            CACHE TABLE filtered_reviews AS
            SELECT year, stars, useful,
                   CASE
                     WHEN length(text) < 250  THEN 'Short'
                     WHEN length(text) < 750  THEN 'Medium'
                     WHEN length(text) < 2000 THEN 'Long'
                     ELSE 'Very Long'
                   END AS length_category
            FROM reviews
            WHERE year BETWEEN 2015 AND 2021
              AND useful >= 1
        """)
        table_name = "filtered_reviews"
    elif do_cache == "full":
        print("\n⚠  Caching FULL review table via SQL. This may OOM.")
        spark.sql("CACHE TABLE reviews")
        table_name = None  # we'll inline the query
    else:
        table_name = None

    # Build source expression
    if table_name:
        src = table_name
    else:
        src = """(
            SELECT year, stars, useful,
                   CASE
                     WHEN length(text) < 250  THEN 'Short'
                     WHEN length(text) < 750  THEN 'Medium'
                     WHEN length(text) < 2000 THEN 'Long'
                     ELSE 'Very Long'
                   END AS length_category
            FROM reviews
            WHERE year BETWEEN 2015 AND 2021
              AND useful >= 1
        )"""

    # ── Part (a) ───────────────────────────────────────────────────
    sql_a = f"""
        SELECT length_category,
               COUNT(*)        AS review_count,
               AVG(stars)      AS avg_stars,
               AVG(useful)     AS avg_useful
        FROM {src} t
        GROUP BY length_category
        ORDER BY avg_useful DESC
    """

    print("\n[explain — Query 1 SQL part (a)]")
    spark.sql(sql_a).explain(mode="formatted")

    part_a = spark.sql(sql_a)
    elapsed_a = timed_run(lambda: part_a.collect(), label="Q1a-sql")
    part_a.show(truncate=False)

    # ── Part (b) ───────────────────────────────────────────────────
    sql_b = f"""
        SELECT year, length_category,
               COUNT(*) AS cat_count,
               ROUND(COUNT(*) * 100.0
                     / SUM(COUNT(*)) OVER (PARTITION BY year), 2)
                 AS percentage
        FROM {src} t
        GROUP BY year, length_category
        ORDER BY year, length_category
    """

    part_b = spark.sql(sql_b)
    elapsed_b = timed_run(lambda: part_b.collect(), label="Q1b-sql")
    part_b.show(50, truncate=False)

    if do_cache == "filtered":
        spark.sql("UNCACHE TABLE filtered_reviews")
    elif do_cache == "full":
        spark.sql("UNCACHE TABLE reviews")

    return elapsed_a, elapsed_b


def main():
    parser = argparse.ArgumentParser(description="Query 1: Review Length Categories")
    parser.add_argument("--api", choices=["df", "sql"], required=True)
    parser.add_argument("--cache", choices=["none", "filtered", "full"], default="none")
    parser.add_argument("--run-id", type=int, default=1)
    args = parser.parse_args()

    app_name = f"Q1-{args.api}-cache_{args.cache}-run{args.run_id}"
    spark = get_spark(app_name)

    review_path = os.path.join(PARQUET_DIR, "review")

    if args.api == "df":
        elapsed_a, elapsed_b = query1_df(spark, review_path, args.cache)
    else:
        elapsed_a, elapsed_b = query1_sql(spark, review_path, args.cache)

    # Log timings
    results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, "q1.csv"), "a") as f:
        f.write(f"{args.api},{args.cache},{args.run_id},"
                f"{elapsed_a:.3f},{elapsed_b:.3f}\n")

    spark.stop()


if __name__ == "__main__":
    main()
