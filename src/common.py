"""
common.py — Shared utilities for the Stars & Sparks project.

Provides:
  - get_spark()       : build a SparkSession with project defaults
  - SCHEMAS           : explicit StructType schemas for all datasets
  - timed_run()       : wall-clock measurement helper (single run)
  - DATA_DIR / PARQUET_DIR : default data paths
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, ArrayType,
)

# ── Data paths (override with environment variables) ───────────────
RAW_DIR     = os.environ.get("RAW_DIR",     os.path.expanduser("~/advdb/raw"))
PARQUET_DIR = os.environ.get("PARQUET_DIR", os.path.expanduser("~/advdb/parquet"))

# ── Explicit schemas ───────────────────────────────────────────────
# Yelp business: attributes and hours are kept as STRING and converted
# to VARIANT with parse_json() at query time (see Tip 7).
BUSINESS_SCHEMA = StructType([
    StructField("business_id",  StringType(),  False),
    StructField("name",         StringType(),  True),
    StructField("address",      StringType(),  True),
    StructField("city",         StringType(),  True),
    StructField("state",        StringType(),  True),
    StructField("postal_code",  StringType(),  True),
    StructField("latitude",     DoubleType(),  True),
    StructField("longitude",    DoubleType(),  True),
    StructField("stars",        FloatType(),   True),
    StructField("review_count", IntegerType(), True),
    StructField("is_open",      IntegerType(), True),
    StructField("attributes",   StringType(),  True),   # JSON object → VARIANT
    StructField("categories",   StringType(),  True),
    StructField("hours",        StringType(),  True),   # JSON object → VARIANT
])

# Yelp review
REVIEW_SCHEMA = StructType([
    StructField("review_id",   StringType(),  False),
    StructField("user_id",     StringType(),  False),
    StructField("business_id", StringType(),  False),
    StructField("stars",       FloatType(),   True),
    StructField("useful",      IntegerType(), True),
    StructField("funny",       IntegerType(), True),
    StructField("cool",        IntegerType(), True),
    StructField("text",        StringType(),  True),
    StructField("date",        StringType(),  True),
])

# Yelp user
USER_SCHEMA = StructType([
    StructField("user_id",        StringType(),  False),
    StructField("name",           StringType(),  True),
    StructField("review_count",   IntegerType(), True),
    StructField("yelping_since",  StringType(),  True),
    StructField("useful",         IntegerType(), True),
    StructField("funny",          IntegerType(), True),
    StructField("cool",           IntegerType(), True),
    StructField("elite",          StringType(),  True),
    StructField("friends",        StringType(),  True),
    StructField("fans",           IntegerType(), True),
    StructField("average_stars",  FloatType(),   True),
    StructField("compliment_hot",         IntegerType(), True),
    StructField("compliment_more",        IntegerType(), True),
    StructField("compliment_profile",     IntegerType(), True),
    StructField("compliment_cute",        IntegerType(), True),
    StructField("compliment_list",        IntegerType(), True),
    StructField("compliment_note",        IntegerType(), True),
    StructField("compliment_plain",       IntegerType(), True),
    StructField("compliment_cool",        IntegerType(), True),
    StructField("compliment_funny",       IntegerType(), True),
    StructField("compliment_writer",      IntegerType(), True),
    StructField("compliment_photos",      IntegerType(), True),
])

# ACS ZCTA — zcta MUST be STRING (leading zeros, Tip 6)
ACS_SCHEMA = StructType([
    StructField("zcta",                    StringType(),  False),
    StructField("median_household_income", IntegerType(), True),
    StructField("population",             IntegerType(), True),
])

# GloVe — word + 100-dimensional float vector
GLOVE_SCHEMA = StructType([
    StructField("word", StringType(),            False),
    StructField("vec",  ArrayType(FloatType()),  False),
])


def get_spark(app_name: str = "starsnsparks", **extra_conf) -> SparkSession:
    """
    Build a SparkSession with project defaults.

    The master URL is set by the SPARK_MASTER env var (default:
    spark://localhost:7077) so the same code works on the cluster
    and in local mode for quick tests.

    Extra Spark conf entries can be passed as keyword arguments, e.g.:
        get_spark("Q1-run1", **{"spark.executor.memory": "2g"})
    """
    master = os.environ.get("SPARK_MASTER", "spark://localhost:7077")
    events = os.environ.get("SPARK_EVENTS_DIR",
                            os.path.expanduser("~/spark-events"))

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{events}")
        .config("spark.driver.memory", "2g")
    )

    for key, val in extra_conf.items():
        builder = builder.config(key, str(val))

    return builder.getOrCreate()


def timed_run(action_fn, label: str = "") -> float:
    """
    Measure wall-clock time of a Spark action.

    Parameters
    ----------
    action_fn : callable
        A zero-argument function that triggers the Spark action,
        e.g. ``lambda: df.collect()`` or ``lambda: df.write.format("noop").mode("overwrite").save()``.
    label : str
        Optional label printed alongside the timing.

    Returns
    -------
    float
        Elapsed wall-clock seconds.

    Notes
    -----
    This measures a SINGLE run.  The measurement protocol requires
    ≥ 3 runs each in a NEW Spark application (i.e., a new spark-submit
    invocation).  The run scripts handle the outer loop and statistics.
    """
    start = time.time()
    result = action_fn()
    elapsed = time.time() - start

    tag = f" [{label}]" if label else ""
    print(f"⏱  {elapsed:.3f}s{tag}")
    return elapsed
