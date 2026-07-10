"""Glue Job 3 — silver_to_gold_clv_daily  (SDD §7.2, §9.1)

Builds the PRIMARY fact g_fact_customer_clv_daily (grain user_id x snapshot_date):
cumulative net revenue / orders / recency and as-of-date High/Medium/Low tiers per
identified customer. Daily production run appends one snapshot for run_date; --backfill
generates month-end snapshots across history for the evolution curve.
"""
import sys

try:
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    _GLUE = True
except Exception:
    _GLUE = False

from glue.lib.gold import build_orders, build_clv_daily, daily_snapshot, month_end_snapshots


def _args(argv):
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--source_mode", default="delta", choices=["delta", "csv"])
    p.add_argument("--landing_path", default=None)
    p.add_argument("--silver_path", default="s3://global-partners-dev-silver")
    p.add_argument("--gold_path", default="s3://global-partners-dev-gold")
    p.add_argument("--run_date", default=None)          # snapshot_date for daily append
    p.add_argument("--backfill", action="store_true")   # month-end snapshots across history
    k, _ = p.parse_known_args(argv)
    return k


def _load_orders(spark, a):
    if a.source_mode == "csv":
        from types import SimpleNamespace
        from glue.jobs.job2_bronze_to_silver import build_silver
        s = build_silver(spark, SimpleNamespace(source_mode="csv", landing_path=a.landing_path,
                         bronze_path=None, silver_path=a.silver_path))
        return s["s_orders"], s["s_date_dim"]
    orders = spark.read.format("delta").load(f"{a.silver_path}/s_orders")
    cal = spark.read.format("delta").load(f"{a.silver_path}/s_date_dim")
    return orders, cal


def main(argv):
    a = _args(argv)
    if _GLUE:
        spark = GlueContext(SparkContext()).spark_session
    else:
        from glue.lib.spark_session import get_local_spark
        spark = get_local_spark("gold_clv_daily", with_delta=True)

    orders, cal = _load_orders(spark, a)
    if a.backfill:
        b = orders.selectExpr("min(order_date) lo", "max(order_date) hi").collect()[0]
        snaps = month_end_snapshots(spark, cal, b["lo"].isoformat(), b["hi"].isoformat())
    else:
        rd = a.run_date or orders.selectExpr("max(order_date)").collect()[0][0].isoformat()
        snaps = daily_snapshot(spark, rd)

    clv = build_clv_daily(orders, snaps)
    # dynamic partition overwrite: replaces only the snapshot_date(s) in this run's data,
    # leaving other dates untouched — idempotent re-run of the same run_date (or --backfill)
    # instead of accumulating duplicate rows the way a plain append would.
    (clv.write.format("delta").mode("overwrite").partitionBy("snapshot_date")
        .option("partitionOverwriteMode", "dynamic")
        .option("mergeSchema", "true").save(f"{a.gold_path.rstrip('/')}/g_fact_customer_clv_daily"))
    print(f"[gold] g_fact_customer_clv_daily rows written: {clv.count():,}")


if __name__ == "__main__":
    main(sys.argv[1:])
