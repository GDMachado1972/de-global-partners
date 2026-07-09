"""Glue Job 4 — silver_to_gold_marts  (SDD §7.3, §9.1)

Builds the six analytic marts from Silver: RFM segmentation, churn indicators,
sales trends, loyalty impact, location performance, and the optional add-on revenue
supplement (D2). Runs in parallel with Job 3 after Silver.
"""
import sys

try:
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    _GLUE = True
except Exception:
    _GLUE = False

from glue.lib import gold


def _args(argv):
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--source_mode", default="delta", choices=["delta", "csv"])
    p.add_argument("--landing_path", default=None)
    p.add_argument("--silver_path", default="s3://global-partners-dev-silver")
    p.add_argument("--gold_path", default="s3://global-partners-dev-gold")
    p.add_argument("--analysis_date", default=None)
    k, _ = p.parse_known_args(argv)
    return k


def _load_silver(spark, a):
    if a.source_mode == "csv":
        from types import SimpleNamespace
        from glue.jobs.job2_bronze_to_silver import build_silver
        return build_silver(spark, SimpleNamespace(source_mode="csv", landing_path=a.landing_path,
                            bronze_path=None, silver_path=a.silver_path))
    load = lambda t: spark.read.format("delta").load(f"{a.silver_path}/{t}")
    return {"s_order_lines": load("s_order_lines"), "s_orders": load("s_orders"),
            "s_date_dim": load("s_date_dim")}


def build_marts(s, analysis_date):
    orders, lines, cal = s["s_orders"], s["s_order_lines"], s["s_date_dim"]
    return {
        "g_dim_customer_rfm": gold.build_rfm(orders, analysis_date),
        "g_fact_churn_indicators": gold.build_churn(orders, analysis_date),
        "g_fact_sales_trends": gold.build_sales_trends(lines, cal),
        "g_fact_loyalty_impact": gold.build_loyalty_impact(orders),
        "g_fact_location_performance": gold.build_location_performance(orders),
        "g_fact_addon_revenue": gold.build_addon_revenue(lines),
    }


def main(argv):
    a = _args(argv)
    if _GLUE:
        spark = GlueContext(SparkContext()).spark_session
    else:
        from glue.lib.spark_session import get_local_spark
        spark = get_local_spark("gold_marts", with_delta=True)

    s = _load_silver(spark, a)
    ad = a.analysis_date or s["s_orders"].selectExpr("max(order_date)").collect()[0][0].isoformat()
    marts = build_marts(s, ad)
    for name, df in marts.items():
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .save(f"{a.gold_path.rstrip('/')}/{name}"))
        print(f"[gold] {name}: {df.count():,} rows")


if __name__ == "__main__":
    main(sys.argv[1:])
