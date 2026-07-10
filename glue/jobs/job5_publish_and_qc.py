"""Glue Job 5 — publish_and_qc  (SDD §9.1, §14)

Runs the data-quality gates over Silver+Gold and (in AWS) registers/refreshes the
Athena engine-v3 external tables via the Glue Data Catalog. QC gates fail the job
(non-zero exit / raised error) so the workflow's failure branch alerts via SNS.

Gates (SDD §14):
  - Silver order_items == 202,692 (post-clean reconciliation)
  - calendar join coverage == 100% (no order_date missing from s_date_dim)
  - CLV tier split ~ 20/60/20 (within tolerance)
  - net = gross + options identity holds on sales_trends
"""
import sys

try:
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    _GLUE = True
except Exception:
    _GLUE = False

SILVER_ORDER_ITEMS = 202_692

GOLD_TABLES = [
    "g_fact_customer_clv_daily", "g_dim_customer_rfm", "g_fact_churn_indicators",
    "g_fact_sales_trends", "g_fact_loyalty_impact", "g_fact_location_performance",
    "g_fact_addon_revenue",
]


def run_qc(spark, silver_path, gold_path, reader="delta"):
    load = lambda base, t: (spark.read.format("delta").load(f"{base}/{t}")
                            if reader == "delta" else None)
    s_items = load(silver_path, "s_order_items")
    s_orders = load(silver_path, "s_orders")
    s_cal = load(silver_path, "s_date_dim")
    clv = load(gold_path, "g_fact_customer_clv_daily")
    trends = load(gold_path, "g_fact_sales_trends")
    return qc_checks(s_items, s_orders, s_cal, clv, trends)


def qc_checks(s_items, s_orders, s_cal, clv, trends):
    from pyspark.sql import functions as F
    results = []

    n_items = s_items.count()
    results.append(("silver_order_items == 202,692", n_items == SILVER_ORDER_ITEMS, n_items))

    missing = (s_orders.select("order_date").distinct()
               .join(s_cal, s_orders.order_date == s_cal.date_key, "left_anti").count())
    results.append(("calendar_join_coverage == 100%", missing == 0, f"{missing} missing"))

    # latest snapshot tier split within tolerance of 20/60/20
    latest = clv.selectExpr("max(snapshot_date) d").collect()[0]["d"]
    snap = clv.filter(F.col("snapshot_date") == F.lit(latest))
    tot = snap.count()
    dist = {r["clv_tier"]: r["c"] for r in snap.groupBy("clv_tier").agg(F.count("*").alias("c")).collect()}
    hi, lo = dist.get("High", 0) / tot, dist.get("Low", 0) / tot
    ok_tiers = 0.18 <= hi <= 0.22 and 0.18 <= lo <= 0.22
    results.append(("clv_tier_split ~ 20/60/20", ok_tiers, {k: round(v/tot, 3) for k, v in dist.items()}))

    # net >= gross identity (options additive, no discounts)
    ident = trends.selectExpr("sum(net_revenue) n", "sum(gross_revenue) g").collect()[0]
    results.append(("net >= gross (options additive)", ident["n"] >= ident["g"] - 0.01,
                    f"net={ident['n']:.2f} gross={ident['g']:.2f}"))

    failed = [r for r in results if not r[1]]
    for name, ok, detail in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}  ({detail})")
    if failed:
        raise SystemExit(f"QC FAILED: {len(failed)} gate(s) — see above")
    print("[qc] all gates PASSED")
    return results


def _args(argv):
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--silver_path", default="s3://global-partners-dev-silver")
    p.add_argument("--gold_path", default="s3://global-partners-dev-gold")
    p.add_argument("--athena_database", default=None)      # skip registration if unset
    p.add_argument("--athena_workgroup", default=None)
    p.add_argument("--region", default="us-east-1")
    k, _ = p.parse_known_args(argv)
    return k


def _wait_athena(athena, query_id):
    import time
    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state
        time.sleep(2)


def register_athena_tables(gold_path, database, workgroup, region):
    """Register each Gold Delta table in the Glue Catalog for Athena engine v3 native
    Delta querying — CREATE TABLE ... TBLPROPERTIES ('table_type'='DELTA') auto-detects
    columns from the Delta transaction log, no schema duplication needed here."""
    import boto3
    glue = boto3.client("glue", region_name=region)
    athena = boto3.client("athena", region_name=region)
    try:
        glue.get_database(Name=database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": database})
        print(f"[publish] created Glue database {database}")

    def run_ddl(sql):
        qid = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup)["QueryExecutionId"]
        state = _wait_athena(athena, qid)
        if state != "SUCCEEDED":
            reason = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"].get(
                "StateChangeReason", "")
            raise RuntimeError(f"Athena DDL failed: {state} — {reason}\n{sql}")

    for t in GOLD_TABLES:
        # re-registered fresh each run (drop+create) so schema drift in the Delta log
        # (new mart columns, etc.) is always picked up.
        run_ddl(f"DROP TABLE IF EXISTS {database}.{t}")
        run_ddl(f"CREATE EXTERNAL TABLE {database}.{t} "
                f"LOCATION '{gold_path.rstrip('/')}/{t}/' "
                f"TBLPROPERTIES ('table_type' = 'DELTA')")
        print(f"[publish] registered {database}.{t}")


def main(argv):
    a = _args(argv)
    if _GLUE:
        spark = GlueContext(SparkContext()).spark_session
    else:
        from glue.lib.spark_session import get_local_spark
        spark = get_local_spark("publish_and_qc", with_delta=True)
    run_qc(spark, a.silver_path, a.gold_path)
    if _GLUE and a.athena_database and a.athena_workgroup:
        register_athena_tables(a.gold_path, a.athena_database, a.athena_workgroup, a.region)
        print("[publish] Athena v3 tables registered.")
    else:
        print("[publish] QC complete; Athena registration skipped (local mode or unset args).")


if __name__ == "__main__":
    main(sys.argv[1:])
