"""Gold-layer tests (SDD §7, §8, §14)."""
import os
import types
import pytest
from pyspark.sql import functions as F
from glue.lib.gold import (build_clv_daily, build_rfm, build_location_performance,
                           build_loyalty_impact, build_addon_revenue, build_sales_trends,
                           month_end_snapshots)

ORDERS_SCHEMA = ("order_id string, user_id string, restaurant_id string, "
                 "is_loyalty boolean, is_guest boolean, order_date date, order_net double")


def _orders(spark, rows):
    from datetime import date
    data = [(oid, uid, rid, loy, uid is None, date.fromisoformat(d), net)
            for (oid, uid, rid, loy, d, net) in rows]
    return spark.createDataFrame(data, ORDERS_SCHEMA)


def test_clv_tiers_and_guest_exclusion(spark):
    rows = [  # (order_id, user_id, restaurant_id, is_loyalty, date, net)
        ("o1", "u1", "R1", True, "2023-01-05", 10.0),
        ("o2", "u2", "R1", True, "2023-01-06", 50.0),
        ("o3", "u3", "R1", False, "2023-01-07", 100.0),
        ("o4", "u4", "R1", False, "2023-01-08", 200.0),
        ("o5", "u5", "R1", False, "2023-01-09", 500.0),
        ("o6", None, "R1", False, "2023-01-09", 999.0),   # guest -> excluded
    ]
    orders = _orders(spark, rows)
    snaps = spark.createDataFrame([("2023-02-01",)], "snapshot_date string") \
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
    clv = build_clv_daily(orders, snaps)
    assert clv.count() == 5                                  # guest excluded
    assert clv.filter("user_id is null").count() == 0
    tiers = {r["user_id"]: r["clv_tier"] for r in clv.collect()}
    assert tiers["u5"] == "High"                            # top value
    assert tiers["u1"] == "Low"                             # bottom value
    assert tiers["u3"] == "Medium"


def test_clv_cumulative_accumulates(spark):
    rows = [("o1", "u1", "R1", True, "2023-01-01", 10.0),
            ("o2", "u1", "R1", True, "2023-02-01", 20.0)]
    orders = _orders(spark, rows)
    snaps = spark.createDataFrame([("2023-01-15",), ("2023-02-15",)], "snapshot_date string") \
        .withColumn("snapshot_date", F.to_date("snapshot_date"))
    clv = {r["snapshot_date"].isoformat(): r for r in build_clv_daily(orders, snaps).collect()}
    assert clv["2023-01-15"]["cum_net_revenue"] == 10.0 and clv["2023-01-15"]["cum_orders"] == 1
    assert clv["2023-02-15"]["cum_net_revenue"] == 30.0 and clv["2023-02-15"]["cum_orders"] == 2


def test_rfm_segments_are_valid(spark):
    rows = [(f"o{i}", f"u{i%4}", "R1", True, f"2023-0{1+i%6}-10", float(10*i))
            for i in range(1, 21)]
    seg = {r["rfm_segment"] for r in build_rfm(_orders(spark, rows), "2023-07-01").collect()}
    assert seg.issubset({"VIP", "New", "Churn-Risk", "Other"})


# ---------------------------------------------------------------- integration
@pytest.mark.integration
def test_real_gold_outputs(spark, data_dir):
    if not data_dir:
        pytest.skip("DATA_DIR not set")
    import glue.jobs.job2_bronze_to_silver as j2
    ns = types.SimpleNamespace(source_mode="csv", landing_path=data_dir,
                               bronze_path="", silver_path="/tmp/silver")
    t = j2.build_silver(spark, ns)
    orders = t["s_orders"].cache(); lines = t["s_order_lines"].cache(); cal = t["s_date_dim"]

    # CLV monthly — latest snapshot tier split ~ 20/60/20
    b = orders.selectExpr("min(order_date) lo", "max(order_date) hi").collect()[0]
    snaps = month_end_snapshots(spark, cal, b["lo"].isoformat(), b["hi"].isoformat())
    clv = build_clv_daily(orders, snaps).cache()
    assert clv.filter("user_id is null").count() == 0          # no guests
    assert clv.filter("clv_tier is null").count() == 0
    last = clv.selectExpr("max(snapshot_date) d").collect()[0]["d"]
    latest = clv.filter(F.col("snapshot_date") == F.lit(last))
    tot = latest.count()
    hi = latest.filter("clv_tier='High'").count() / tot
    lo = latest.filter("clv_tier='Low'").count() / tot
    assert abs(hi - 0.20) <= 0.06 and abs(lo - 0.20) <= 0.06   # ~20/60/20

    # location performance -> 27 locations (DEV test store already removed in Silver)
    assert build_location_performance(orders).count() == 27
    # loyalty impact -> two cohorts
    assert build_loyalty_impact(orders).count() == 2
    # add-on supplement -> paid/non-paid rows only
    seg = {r["has_paid_modifier"] for r in build_addon_revenue(lines).collect()}
    assert seg.issubset({True, False}) and len(seg) >= 1
    # net == gross everywhere (no discounts, D2)
    st = build_sales_trends(lines, cal)
    assert st.filter(F.col("net_revenue") < F.col("gross_revenue")).count() == 0
