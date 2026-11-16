"""
q4.py — Query 4: Nearest-Neighbor Restaurants in PA (15%)

For open PA restaurants (state='PA', is_open=1, 'Restaurants' in categories):
  - Find the 5 nearest other restaurants (haversine distance).
  - Per postal_code: count, avg NN distance (km), avg stars, avg NN stars.
  - Pearson correlation: restaurant stars vs avg NN stars.

Two versions:
  (a) Without NEAREST BY — self-join + window ranking.
  (b) With NEAREST BY — Spark 4.x syntax.

Usage:
    spark-submit src/q4.py --version {no_nearest|nearest} --run-id N [--tag TAG]

Design decisions:
  - Haversine (Tip 4): d = 2R·asin(√(sin²(Δφ/2) + cos(φ₁)·cos(φ₂)·sin²(Δλ/2)))
    with R = 6371 km.  Built-in functions only.
  - NEAREST BY (Tip 5): request k+1=6 and exclude self.
  - Version (a) uses a self-join on a condition q.business_id != t.business_id,
    computes haversine for all pairs, then takes top 5 per restaurant via
    a Window function.  This produces ~N² pairs (N ≈ 3000-4000 PA restaurants).
"""

import argparse
import os
import sys
import math

from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.insert(0, os.path.dirname(__file__))
from common import get_spark, PARQUET_DIR, timed_run

R_EARTH = 6371  # km


def haversine_expr(lat1, lon1, lat2, lon2):
    """
    Build a Spark Column expression for the haversine distance in km.

    d = 2R · asin(√(sin²(Δφ/2) + cos(φ₁)·cos(φ₂)·sin²(Δλ/2)))

    All inputs are Column expressions in degrees; we convert to radians.
    """
    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dlam = F.radians(lon2 - lon1)

    a = (
        F.pow(F.sin(dphi / 2), 2) +
        F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(dlam / 2), 2)
    )
    return 2 * R_EARTH * F.asin(F.sqrt(a))


def get_pa_restaurants(spark, business_path):
    """Load open PA restaurants."""
    business = spark.read.parquet(business_path)
    return (
        business
        .filter(F.col("state") == "PA")
        .filter(F.col("is_open") == 1)
        .filter(F.col("categories").contains("Restaurants"))
        .select("business_id", "postal_code", "latitude", "longitude",
                F.col("stars").alias("biz_stars"))
    )


def query4_no_nearest(spark, business_path):
    """Version (a): without NEAREST BY — self-join + window."""

    pa = get_pa_restaurants(spark, business_path)
    pa_q = pa.alias("q")
    pa_t = pa.alias("t")

    # Self-join: every pair where q != t
    pairs = (
        pa_q.join(pa_t, F.col("q.business_id") != F.col("t.business_id"))
        .select(
            F.col("q.business_id").alias("q_id"),
            F.col("q.postal_code").alias("q_postal"),
            F.col("q.biz_stars").alias("q_stars"),
            F.col("t.business_id").alias("t_id"),
            F.col("t.biz_stars").alias("t_stars"),
            haversine_expr(
                F.col("q.latitude"), F.col("q.longitude"),
                F.col("t.latitude"), F.col("t.longitude")
            ).alias("distance_km"),
        )
    )

    # Rank by distance, take top 5 per restaurant
    w = Window.partitionBy("q_id").orderBy("distance_km")
    nn5 = (
        pairs
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= 5)
    )

    # Per postal_code aggregation
    per_restaurant = (
        nn5
        .groupBy("q_id", "q_postal", "q_stars")
        .agg(
            F.avg("distance_km").alias("avg_nn_dist"),
            F.avg("t_stars").alias("avg_nn_stars"),
        )
    )

    per_postal = (
        per_restaurant
        .groupBy("q_postal")
        .agg(
            F.count("*").alias("restaurant_count"),
            F.avg("avg_nn_dist").alias("avg_nn_distance_km"),
            F.avg("q_stars").alias("avg_stars"),
            F.avg("avg_nn_stars").alias("avg_nn_stars"),
        )
        .withColumnRenamed("q_postal", "postal_code")
        .orderBy(F.desc("restaurant_count"))
    )

    # Pearson correlation
    pearson = per_restaurant.select(
        F.corr("q_stars", "avg_nn_stars").alias("pearson_r")
    )

    print("\n[explain — Query 4 (no NEAREST BY)]")
    per_postal.explain(mode="formatted")

    elapsed = timed_run(lambda: per_postal.collect(), label="Q4-no_nearest")
    per_postal.show(30, truncate=False)

    print("\nPearson correlation (stars vs avg NN stars):")
    pearson.show()

    return elapsed


def query4_nearest(spark, business_path):
    """Version (b): with NEAREST BY."""

    pa = get_pa_restaurants(spark, business_path)
    pa.createOrReplaceTempView("pa_restaurants")

    # NEAREST BY: find 6 nearest (including self), then exclude self
    nn_sql = """
    SELECT
        q.business_id AS q_id,
        q.postal_code AS q_postal,
        q.biz_stars   AS q_stars,
        t.business_id AS t_id,
        t.biz_stars   AS t_stars,
        2 * 6371 * ASIN(SQRT(
            POWER(SIN(RADIANS(t.latitude - q.latitude) / 2), 2) +
            COS(RADIANS(q.latitude)) * COS(RADIANS(t.latitude)) *
            POWER(SIN(RADIANS(t.longitude - q.longitude) / 2), 2)
        )) AS distance_km
    FROM pa_restaurants q
    JOIN pa_restaurants t
    EXACT NEAREST 6 BY DISTANCE (
        2 * 6371 * ASIN(SQRT(
            POWER(SIN(RADIANS(t.latitude - q.latitude) / 2), 2) +
            COS(RADIANS(q.latitude)) * COS(RADIANS(t.latitude)) *
            POWER(SIN(RADIANS(t.longitude - q.longitude) / 2), 2)
        ))
    )
    WHERE q.business_id != t.business_id
    """

    nn5 = spark.sql(nn_sql)

    # Per postal_code aggregation
    per_restaurant = (
        nn5
        .groupBy("q_id", "q_postal", "q_stars")
        .agg(
            F.avg("distance_km").alias("avg_nn_dist"),
            F.avg("t_stars").alias("avg_nn_stars"),
        )
    )

    per_postal = (
        per_restaurant
        .groupBy("q_postal")
        .agg(
            F.count("*").alias("restaurant_count"),
            F.avg("avg_nn_dist").alias("avg_nn_distance_km"),
            F.avg("q_stars").alias("avg_stars"),
            F.avg("avg_nn_stars").alias("avg_nn_stars"),
        )
        .withColumnRenamed("q_postal", "postal_code")
        .orderBy(F.desc("restaurant_count"))
    )

    # Pearson correlation
    pearson = per_restaurant.select(
        F.corr("q_stars", "avg_nn_stars").alias("pearson_r")
    )

    print("\n[explain — Query 4 (NEAREST BY)]")
    per_postal.explain(mode="formatted")

    elapsed = timed_run(lambda: per_postal.collect(), label="Q4-nearest")
    per_postal.show(30, truncate=False)

    print("\nPearson correlation (stars vs avg NN stars):")
    pearson.show()

    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Query 4: NN Restaurants PA")
    parser.add_argument("--version", choices=["no_nearest", "nearest"], required=True)
    parser.add_argument("--run-id", type=int, default=1)
    parser.add_argument("--tag", default="")
    args = parser.parse_args()

    tag = f"-{args.tag}" if args.tag else ""
    app_name = f"Q4-{args.version}{tag}-run{args.run_id}"
    spark = get_spark(app_name)

    business_path = os.path.join(PARQUET_DIR, "business")

    if args.version == "no_nearest":
        elapsed = query4_no_nearest(spark, business_path)
    else:
        elapsed = query4_nearest(spark, business_path)

    results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, "q4.csv"), "a") as f:
        f.write(f"{args.version},{args.tag},{args.run_id},{elapsed:.3f}\n")

    spark.stop()


if __name__ == "__main__":
    main()
