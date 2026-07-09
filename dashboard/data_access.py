"""Dashboard data access — two backends, one interface.

  DATA_BACKEND=athena : query the Gold tables via pyathena (engine v3 workgroup).
  DATA_BACKEND=local  : build the Gold marts from the CSVs with the SAME tested PySpark
                        transforms (glue/lib/gold.py) and hand back pandas — no logic
                        duplication, no Delta/network. Used for local dev and the
                        recorded-video descope (SDD §13).

Env:
  local  -> LANDING=/path/to/the/three/csvs
  athena -> ATHENA_S3_STAGING=s3://global-partners-dev-athena-results/athena/
            ATHENA_WORKGROUP=global-partners-dev-wg  ATHENA_DATABASE=global_partners_dev_gold
            AWS_REGION=us-east-1  (+ AWS creds in the environment)
"""
from __future__ import annotations
import os
import sys
import pandas as pd

# ensure repo root importable for lazy glue.* imports regardless of caller
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def money(x) -> str:
    try:
        return f"${x:,.2f}"
    except Exception:
        return "—"


def pct(x) -> str:
    try:
        return f"{x*100:.1f}%"
    except Exception:
        return "—"


try:
    import streamlit as st
    _cache_data = st.cache_data
    _cache_resource = st.cache_resource
except Exception:                      # allow import under pytest without streamlit runtime
    def _identity(*a, **k):
        def deco(fn): return fn
        return deco if not a else a[0]
    _cache_data = _cache_resource = _identity

BACKEND = os.environ.get("DATA_BACKEND", "local").lower()

GOLD_TABLES = [
    "g_fact_customer_clv_daily", "g_dim_customer_rfm", "g_fact_churn_indicators",
    "g_fact_sales_trends", "g_fact_loyalty_impact", "g_fact_location_performance",
    "g_fact_addon_revenue",
]


# ---------------------------------------------------------------- Athena backend
def _athena_conn():
    from pyathena import connect
    return connect(
        s3_staging_dir=os.environ["ATHENA_S3_STAGING"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        work_group=os.environ.get("ATHENA_WORKGROUP", "global-partners-dev-wg"),
        schema_name=os.environ.get("ATHENA_DATABASE", "global_partners_dev_gold"),
    )


@_cache_data(ttl=300, show_spinner=False)
def _athena_query(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, _athena_conn())


# ---------------------------------------------------------------- local backend
def build_local_marts(spark, landing: str) -> dict:
    """Pure builder (also unit-tested): Silver + Gold marts as pandas DataFrames."""
    from types import SimpleNamespace
    from glue.jobs.job2_bronze_to_silver import build_silver
    from glue.lib import gold
    from glue.lib.gold import daily_snapshot

    s = build_silver(spark, SimpleNamespace(source_mode="csv", landing_path=landing,
                                            bronze_path=None, silver_path="/tmp/silver"))
    md = s["s_orders"].selectExpr("max(order_date) d").collect()[0]["d"].isoformat()
    marts = {
        "g_fact_customer_clv_daily": gold.build_clv_daily(s["s_orders"], daily_snapshot(spark, md)),
        "g_dim_customer_rfm": gold.build_rfm(s["s_orders"], md),
        "g_fact_churn_indicators": gold.build_churn(s["s_orders"], md),
        "g_fact_sales_trends": gold.build_sales_trends(s["s_order_lines"], s["s_date_dim"]),
        "g_fact_loyalty_impact": gold.build_loyalty_impact(s["s_orders"]),
        "g_fact_location_performance": gold.build_location_performance(s["s_orders"]),
        "g_fact_addon_revenue": gold.build_addon_revenue(s["s_order_lines"]),
    }
    return {k: v.toPandas() for k, v in marts.items()}


@_cache_resource(show_spinner="Building Gold marts from CSVs (first load only)…")
def _local_marts() -> dict:
    from glue.lib.spark_session import get_local_spark
    landing = os.environ.get("LANDING")
    if not landing:
        raise RuntimeError("LANDING env var (folder with the three CSVs) is required for local backend")
    return build_local_marts(get_local_spark("dashboard-local"), landing)


# ---------------------------------------------------------------- public API
def load(table: str) -> pd.DataFrame:
    if table not in GOLD_TABLES:
        raise KeyError(f"unknown Gold table: {table}")
    if BACKEND == "athena":
        return _athena_query(f'SELECT * FROM "{table}"')
    return _local_marts()[table]
