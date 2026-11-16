"""
q3.py — Query 3: Per Registration-Year User Analysis (15%)

Per year(yelping_since):
  (a) total review count
  (b) avg review stars
  (c) avg "strictness": mean(review.stars - business.stars)
  (d) % of reviews written in a year when the author was elite
  (e) count of users with zero reviews in review.json

Order by registration year ascending.

Both DataFrame and SQL.  Experiments with join hints and AQE.

Usage:
    spark-submit src/q3.py --api {df|sql} --hint {default|broadcast|merge|shuffle_hash|shuffle_replicate_nl} --run-id N [--explain-only] [--tag TAG]

Design decisions:
  - Elite field (Tip 3): "20,20" should be "2020". We split on comma, trim,
    and normalize: 2-digit tokens that could be year halves are handled by
    checking if two consecutive 2-digit tokens form a 4-digit year.
    Actually, the simpler approach: split on comma, check each token —
    if it's 4 digits and a valid year, use it; if it's 2 digits, skip it
    (it's part of a corrupted 4-digit year).  We'll inspect the actual data
    to calibrate this.
  - For (d): a review is "elite" if year(review.date) appears in the
    cleaned elite list of its author.
  - For (e): LEFT ANTI JOIN user → review on user_id.
"""

import argparse
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))
from common import get_spark, PARQUET_DIR, timed_run


def clean_elite_udf():
    """
    Return a Column expression that parses the 'elite' field into
    an array of valid 4-digit year integers.

    The elite field is a comma-separated string like "2015,2016,2017"
    but may contain anomalies like "20,20" (corrupted "2020").

    Strategy:
      1. Replace "20,20" with "2020" (the known anomaly from Tip 3).
      2. Split on comma.
      3. Trim whitespace.
      4. Keep only 4-digit tokens that look like years (2004-2022).
    """
    # Pre-clean: fix the known "20,20" → "2020" anomaly
    cleaned = F.regexp_replace(F.col("elite"), r"20,20", "2020")
    # Split, trim, filter to valid 4-digit years
    tokens = F.split(cleaned, ",")
    valid_years = F.filter(
        F.transform(tokens, lambda x: F.trim(x)),
        lambda x: x.rlike(r"^\d{4}$")
    )
    return valid_years


def query3_df(spark, review_path, user_path, business_path, hint="default"):
    """DataFrame API implementation of Query 3."""

    reviews  = spark.read.parquet(review_path)
    users    = spark.read.parquet(user_path)
    business = spark.read.parquet(business_path)

    # ── Prepare users with registration year and cleaned elite ─────
    users_prep = users.select(
        "user_id",
        F.year(F.col("yelping_since")).alias("reg_year"),
        clean_elite_udf().alias("elite_years"),
    )

    # ── Join reviews with users ────────────────────────────────────
    if hint == "broadcast":
        joined_ru = reviews.join(F.broadcast(users_prep), "user_id")
    elif hint == "merge":
        joined_ru = reviews.hint("MERGE").join(users_prep, "user_id")
    elif hint == "shuffle_hash":
        joined_ru = reviews.hint("SHUFFLE_HASH").join(users_prep, "user_id")
    elif hint == "shuffle_replicate_nl":
        joined_ru = reviews.hint("SHUFFLE_REPLICATE_NL").join(users_prep, "user_id")
    else:
        joined_ru = reviews.join(users_prep, "user_id")

    # ── Join with business for strictness ──────────────────────────
    joined = joined_ru.join(
        business.select("business_id", business["stars"].alias("biz_stars")),
        "business_id"
    )

    # ── Compute metrics ────────────────────────────────────────────
    review_year = F.year(F.col("date")).alias("review_year")

    # (d) is_elite: check if the review year is in the user's elite years
    is_elite = F.array_contains(
        F.col("elite_years"),
        F.year(F.col("date")).cast("string")
    )

    metrics = (
        joined
        .withColumn("review_year", F.year(F.col("date")))
        .withColumn("strictness", F.col("stars") - F.col("biz_stars"))
        .withColumn("is_elite", is_elite)
        .groupBy("reg_year")
        .agg(
            F.count("*").alias("review_count"),                          # (a)
            F.avg("stars").alias("avg_stars"),                            # (b)
            F.avg("strictness").alias("avg_strictness"),                  # (c)
            F.round(
                F.sum(F.when(F.col("is_elite"), 1).otherwise(0))
                / F.count("*") * 100, 2
            ).alias("pct_elite_reviews"),                                 # (d)
        )
        .orderBy("reg_year")
    )

    # ── (e) Users with zero reviews ────────────────────────────────
    all_users = users.select(
        "user_id",
        F.year(F.col("yelping_since")).alias("reg_year"),
    )
    review_users = reviews.select("user_id").distinct()

    zero_review_users = (
        all_users
        .join(review_users, "user_id", "left_anti")
        .groupBy("reg_year")
        .agg(F.count("*").alias("zero_review_count"))
    )

    # ── Combine (a-d) with (e) ─────────────────────────────────────
    result = metrics.join(zero_review_users, "reg_year", "left") \
        .na.fill(0, ["zero_review_count"]) \
        .orderBy("reg_year")

    # ── Explain ────────────────────────────────────────────────────
    print(f"\n[explain — Query 3 DF, hint={hint}]")
    result.explain(mode="formatted")

    return result


def query3_sql(spark, review_path, user_path, business_path, hint="default"):
    """SQL implementation of Query 3."""

    spark.read.parquet(review_path).createOrReplaceTempView("reviews")
    spark.read.parquet(user_path).createOrReplaceTempView("users")
    spark.read.parquet(business_path).createOrReplaceTempView("business")

    hint_clause = ""
    if hint == "broadcast":
        hint_clause = "/*+ BROADCAST(u) */"
    elif hint == "merge":
        hint_clause = "/*+ MERGE(r, u) */"
    elif hint == "shuffle_hash":
        hint_clause = "/*+ SHUFFLE_HASH(r, u) */"
    elif hint == "shuffle_replicate_nl":
        hint_clause = "/*+ SHUFFLE_REPLICATE_NL(r, u) */"

    sql = f"""
    WITH users_prep AS (
        SELECT
            user_id,
            YEAR(yelping_since) AS reg_year,
            FILTER(
                TRANSFORM(SPLIT(REGEXP_REPLACE(elite, '20,20', '2020'), ','),
                          x -> TRIM(x)),
                x -> x RLIKE '^\\\\d{{4}}$'
            ) AS elite_years
        FROM users
    ),
    joined AS (
        SELECT {hint_clause}
            r.stars AS review_stars,
            r.date AS review_date,
            b.stars AS biz_stars,
            u.reg_year,
            u.elite_years
        FROM reviews r
        JOIN users_prep u ON r.user_id = u.user_id
        JOIN business b ON r.business_id = b.business_id
    ),
    metrics AS (
        SELECT
            reg_year,
            COUNT(*) AS review_count,
            AVG(review_stars) AS avg_stars,
            AVG(review_stars - biz_stars) AS avg_strictness,
            ROUND(SUM(CASE WHEN ARRAY_CONTAINS(elite_years,
                            CAST(YEAR(review_date) AS STRING))
                       THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
              AS pct_elite_reviews
        FROM joined
        GROUP BY reg_year
    ),
    zero_reviews AS (
        SELECT
            YEAR(u.yelping_since) AS reg_year,
            COUNT(*) AS zero_review_count
        FROM users u
        LEFT ANTI JOIN reviews r ON u.user_id = r.user_id
        GROUP BY YEAR(u.yelping_since)
    )
    SELECT
        m.reg_year,
        m.review_count,
        m.avg_stars,
        m.avg_strictness,
        m.pct_elite_reviews,
        COALESCE(z.zero_review_count, 0) AS zero_review_count
    FROM metrics m
    LEFT JOIN zero_reviews z ON m.reg_year = z.reg_year
    ORDER BY m.reg_year
    """

    result = spark.sql(sql)

    print(f"\n[explain — Query 3 SQL, hint={hint}]")
    result.explain(mode="formatted")

    return result


def main():
    parser = argparse.ArgumentParser(description="Query 3: Per-Registration-Year Analysis")
    parser.add_argument("--api", choices=["df", "sql"], required=True)
    parser.add_argument("--hint", default="default",
                        choices=["default", "broadcast", "merge",
                                 "shuffle_hash", "shuffle_replicate_nl"])
    parser.add_argument("--run-id", type=int, default=1)
    parser.add_argument("--explain-only", action="store_true",
                        help="Print explain() without executing the query.")
    parser.add_argument("--tag", default="", help="Extra tag for the appName.")
    args = parser.parse_args()

    tag = f"-{args.tag}" if args.tag else ""
    app_name = f"Q3-{args.api}-{args.hint}{tag}-run{args.run_id}"
    spark = get_spark(app_name)

    review_path   = os.path.join(PARQUET_DIR, "review")
    user_path     = os.path.join(PARQUET_DIR, "user")
    business_path = os.path.join(PARQUET_DIR, "business")

    if args.api == "df":
        result = query3_df(spark, review_path, user_path, business_path, args.hint)
    else:
        result = query3_sql(spark, review_path, user_path, business_path, args.hint)

    if args.explain_only:
        print("\n(explain-only mode — skipping execution)")
    else:
        elapsed = timed_run(lambda: result.collect(), label=f"Q3-{args.api}-{args.hint}")
        result.show(30, truncate=False)

        results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")
        os.makedirs(results_dir, exist_ok=True)
        with open(os.path.join(results_dir, "q3.csv"), "a") as f:
            f.write(f"{args.api},{args.hint},{args.tag},{args.run_id},{elapsed:.3f}\n")

    spark.stop()


if __name__ == "__main__":
    main()
