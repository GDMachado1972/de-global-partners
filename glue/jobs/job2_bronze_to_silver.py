"""Glue Job 2 — bronze_to_silver  (SDD §9.1)

Reads Bronze Delta, applies the SDD §4.3 cleaning rules, regenerates the 2020-2024
calendar (decision D1), and writes conformed Silver Delta tables. All transformation
logic lives in glue/lib so it is unit-tested off-cluster (see tests/).

Run modes:
  --source_mode delta  (default) : read Bronze Delta tables written by Job 1
  --source_mode csv               : read raw CSVs from --landing_path (R5 fallback,
                                     unblocks Silver/Gold before SQL Server is wired)

Prod: executed by AWS Glue (Spark 3.3 / Glue 4.0), Delta on the classpath.
Local: `spark-submit job2_bronze_to_silver.py --source_mode csv --landing_path ...`
"""
import sys

try:                                   # Glue runtime
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    _GLUE = True
except Exception:                      # local / CI
    _GLUE = False

from glue.lib.cleaning import clean_order_items, clean_order_item_options
from glue.lib.calendar_gen import generate_calendar
from glue.lib.schema import standardize_casing


def _args(argv):
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--source_mode", default="delta", choices=["delta", "csv"])
    p.add_argument("--landing_path", default=None)      # csv mode
    p.add_argument("--bronze_path", default="s3://global-partners-dev-bronze")
    p.add_argument("--silver_path", default="s3://global-partners-dev-silver")
    known, _ = p.parse_known_args(argv)
    return known


def build_silver(spark, a):
    if a.source_mode == "csv":
        base = a.landing_path.rstrip("/")
        oi = (spark.read.option("header", True).option("multiLine", True)
              .option("quote", '"').option("escape", '"').csv(f"{base}/order_items.csv"))
        op = spark.read.option("header", True).csv(f"{base}/order_item_options.csv")
    else:
        oi = spark.read.format("delta").load(f"{a.bronze_path}/b_order_items")
        op = spark.read.format("delta").load(f"{a.bronze_path}/b_order_item_options")

    s_items = clean_order_items(oi)
    s_options = clean_order_item_options(op)

    # D1: generate calendar from the true order-date span
    b = s_items.selectExpr("min(order_date) lo", "max(order_date) hi").collect()[0]
    s_date_dim = generate_calendar(spark, b["lo"].isoformat(), b["hi"].isoformat())

    # order-grain net revenue (net == gross here; options additive, no discounts — D2)
    from pyspark.sql import functions as F
    opt_by_line = (s_options.groupBy("order_id", "lineitem_id")
                   .agg(F.sum("option_line").alias("opt_revenue")))
    s_order_lines = (s_items.join(opt_by_line, ["order_id", "lineitem_id"], "left")
                     .withColumn("opt_revenue", F.coalesce("opt_revenue", F.lit(0.0)))
                     .withColumn("net_line", F.round(F.col("gross_line") + F.col("opt_revenue"), 2)))
    s_orders = (s_order_lines.groupBy("order_id", "user_id", "restaurant_id",
                                      "is_loyalty", "is_guest", "order_date")
                .agg(F.round(F.sum("net_line"), 2).alias("order_net")))
    return dict(s_order_items=s_items, s_order_item_options=s_options,
                s_date_dim=s_date_dim, s_order_lines=s_order_lines, s_orders=s_orders)


def write_delta(tables, silver_path):
    for name, df in tables.items():
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").save(f"{silver_path.rstrip('/')}/{name}"))


def main(argv):
    a = _args(argv)
    if _GLUE:
        sc = SparkContext(); spark = GlueContext(sc).spark_session
    else:
        from glue.lib.spark_session import get_local_spark
        spark = get_local_spark("bronze_to_silver", with_delta=True)
    tables = build_silver(spark, a)
    write_delta(tables, a.silver_path)
    for n, df in tables.items():
        print(f"[silver] {n}: {df.count():,} rows")


if __name__ == "__main__":
    main(sys.argv[1:])
