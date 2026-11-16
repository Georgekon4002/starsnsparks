"""
q2.py — Query 2: Top-3 Categories per State (15%)

For states with >= 1000 businesses, find top-3 categories by avg review stars.
Only categories with >= 500 reviews in the state are considered.

Per category: review_count, avg_stars, pct_5_star.
Ordered by state, then desc avg_stars; ties broken by higher review count.

SQL version MUST use row_number() window function.

Usage:
    spark-submit src/q2.py --api {df|sql} --run-id N

Design decisions:
  - We explode categories on ", " to get one row per (business, category).
    A review counts toward ALL categories of its business (Tip 1).
  - The join is review ⋈ business on business_id.  Both tables are large,
    so the optimizer will likely choose SortMergeJoin.
  - We filter states with >= 1000 businesses BEFORE joining with reviews
    to reduce the join's right side.  Actually, the assignment says "states
    with at least 1000 businesses" — this is a filter on the business table,
    not on the joined result.
"""

import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.insert(0, os.path.dirname(__file__))
from common import get_spark, PARQUET_DIR, timed_run


def query2_df(spark, business_path, review_path):
    """DataFrame API implementation of Query 2."""

    business = spark.read.parquet(business_path)
    reviews  = spark.read.parquet(review_path)

    # ── States with >= 1000 businesses ─────────────────────────────
    valid_states = (
        business
        .groupBy("state")
        .agg(F.count("*").alias("biz_count"))
        .filter(F.col("biz_count") >= 1000)
        .select("state")
    )

    # ── Explode categories ─────────────────────────────────────────
    biz_cats = (
        business
        .join(valid_states, "state")  # keep only qualifying states
        .filter(F.col("categories").isNotNull())
        .select(
            "business_id", "state",
            F.explode(F.split(F.col("categories"), ", ")).alias("category")
        )
    )

    # ── Join reviews with business-categories ──────────────────────
    joined = reviews.join(biz_cats, "business_id")

    # ── Aggregate per (state, category) ────────────────────────────
    stats = (
        joined
        .groupBy("state", "category")
        .agg(
            F.count("*").alias("review_count"),
            F.avg(reviews["stars"]).alias("avg_stars"),
            F.round(
                F.sum(F.when(reviews["stars"] == 5, 1).otherwise(0))
                / F.count("*") * 100, 1
            ).alias("pct_5_star"),
        )
        .filter(F.col("review_count") >= 500)
    )

    # ── Top 3 per state ────────────────────────────────────────────
    w = Window.partitionBy("state").orderBy(
        F.desc("avg_stars"), F.desc("review_count")
    )
    top3 = (
        stats
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= 3)
        .drop("rn")
        .orderBy("state", F.desc("avg_stars"), F.desc("review_count"))
    )

    # ── Explain ────────────────────────────────────────────────────
    print("\n[explain — Query 2 DF]")
    top3.explain(mode="formatted")

    # ── Execute ────────────────────────────────────────────────────
    elapsed = timed_run(lambda: top3.collect(), label="Q2-df")
    top3.show(60, truncate=False)

    return elapsed


def query2_sql(spark, business_path, review_path):
    """SQL implementation of Query 2 (must use row_number())."""

    spark.read.parquet(business_path).createOrReplaceTempView("business")
    spark.read.parquet(review_path).createOrReplaceTempView("reviews")

    sql = """
    WITH valid_states AS (
        SELECT state
        FROM business
        GROUP BY state
        HAVING COUNT(*) >= 1000
    ),
    biz_cats AS (
        SELECT b.business_id, b.state, cat.category
        FROM business b
        JOIN valid_states vs ON b.state = vs.state
        LATERAL VIEW explode(split(b.categories, ', ')) cat AS category
        WHERE b.categories IS NOT NULL
    ),
    stats AS (
        SELECT
            bc.state,
            bc.category,
            COUNT(*)          AS review_count,
            AVG(r.stars)      AS avg_stars,
            ROUND(SUM(CASE WHEN r.stars = 5 THEN 1 ELSE 0 END)
                  * 100.0 / COUNT(*), 1)  AS pct_5_star
        FROM reviews r
        JOIN biz_cats bc ON r.business_id = bc.business_id
        GROUP BY bc.state, bc.category
        HAVING COUNT(*) >= 500
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY state
                   ORDER BY avg_stars DESC, review_count DESC
               ) AS rn
        FROM stats
    )
    SELECT state, category, review_count, avg_stars, pct_5_star
    FROM ranked
    WHERE rn <= 3
    ORDER BY state, avg_stars DESC, review_count DESC
    """

    result = spark.sql(sql)

    print("\n[explain — Query 2 SQL]")
    result.explain(mode="formatted")

    elapsed = timed_run(lambda: result.collect(), label="Q2-sql")
    result.show(60, truncate=False)

    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Query 2: Top-3 Categories per State")
    parser.add_argument("--api", choices=["df", "sql"], required=True)
    parser.add_argument("--run-id", type=int, default=1)
    args = parser.parse_args()

    app_name = f"Q2-{args.api}-run{args.run_id}"
    spark = get_spark(app_name)

    business_path = os.path.join(PARQUET_DIR, "business")
    review_path   = os.path.join(PARQUET_DIR, "review")

    if args.api == "df":
        elapsed = query2_df(spark, business_path, review_path)
    else:
        elapsed = query2_sql(spark, business_path, review_path)

    results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, "q2.csv"), "a") as f:
        f.write(f"{args.api},{args.run_id},{elapsed:.3f}\n")

    spark.stop()


if __name__ == "__main__":
    main()
