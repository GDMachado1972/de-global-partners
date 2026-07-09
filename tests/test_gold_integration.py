"""Gold-layer INTEGRATION tests — assert locked ground-truth figures against the real
CSVs (via job2.build_silver). Skip when DATA_DIR is unset. Complements the synthetic
unit tests in test_gold.py.
"""
import pytest
from types import SimpleNamespace
from pyspark.sql import functions as F

from glue.lib import gold
from glue.lib.gold import daily_snapshot

IDENTIFIED_CUSTOMERS = 20_059
IDENT_NET_REVENUE = 7_769_264.83
TOTAL_NET = 10_005_987.89
TOTAL_GROSS = 9_920_993.35
LOCATIONS = 27


@pytest.fixture(scope="module")
def silver(spark, data_dir):
    if not data_dir:
        pytest.skip("DATA_DIR not set — skipping Gold integration tests")
    from glue.jobs.job2_bronze_to_silver import build_silver
    a = SimpleNamespace(source_mode="csv", landing_path=data_dir,
                        bronze_path=None, silver_path="/tmp/silver")
    return build_silver(spark, a)


@pytest.fixture(scope="module")
def max_date(silver):
    return silver["s_orders"].selectExpr("max(order_date) d").collect()[0]["d"].isoformat()


@pytest.mark.integration
def test_clv_snapshot_counts_and_tiers(spark, silver, max_date):
    clv = gold.build_clv_daily(silver["s_orders"], daily_snapshot(spark, max_date))
    tot = clv.count()
    assert tot == IDENTIFIED_CUSTOMERS
    dist = {r["clv_tier"]: r["c"]
            for r in clv.groupBy("clv_tier").agg(F.count("*").alias("c")).collect()}
    assert 0.18 <= dist["High"] / tot <= 0.22
    assert 0.18 <= dist["Low"] / tot <= 0.22
    assert 0.56 <= dist["Medium"] / tot <= 0.64
    net = clv.selectExpr("round(sum(cum_net_revenue),2) s").collect()[0]["s"]
    assert net == pytest.approx(IDENT_NET_REVENUE, abs=1.0)


@pytest.mark.integration
def test_sales_trends_reconciles_to_totals(silver):
    st = gold.build_sales_trends(silver["s_order_lines"], silver["s_date_dim"])
    agg = st.selectExpr("round(sum(net_revenue),2) n", "round(sum(gross_revenue),2) g").collect()[0]
    assert agg["n"] == pytest.approx(TOTAL_NET, abs=1.0)
    assert agg["g"] == pytest.approx(TOTAL_GROSS, abs=1.0)
    assert agg["n"] >= agg["g"]


@pytest.mark.integration
def test_location_performance_27_ranked(silver):
    lp = gold.build_location_performance(silver["s_orders"])
    assert lp.count() == LOCATIONS
    ranks = sorted(r["revenue_rank"] for r in lp.select("revenue_rank").collect())
    assert ranks == list(range(1, LOCATIONS + 1))


@pytest.mark.integration
def test_loyalty_and_addon_buckets(silver):
    assert gold.build_loyalty_impact(silver["s_orders"]).count() == 2
    assert gold.build_addon_revenue(silver["s_order_lines"]).count() == 2


@pytest.mark.integration
def test_rfm_and_churn_cover_all_customers(silver, max_date):
    assert gold.build_rfm(silver["s_orders"], max_date).count() == IDENTIFIED_CUSTOMERS
    assert gold.build_churn(silver["s_orders"], max_date).count() == IDENTIFIED_CUSTOMERS


# ---------------------------------------------------------------- QC gate (synthetic)
def test_qc_gates_pass_and_fail(spark):
    from glue.jobs.job5_publish_and_qc import qc_checks, SILVER_ORDER_ITEMS
    items = spark.range(SILVER_ORDER_ITEMS).toDF("id")
    orders = (spark.createDataFrame([("2023-01-01",)], "order_date string")
              .withColumn("order_date", F.to_date("order_date")))
    cal = (spark.createDataFrame([("2023-01-01",)], "date_key string")
           .withColumn("date_key", F.to_date("date_key")))
    tiers = ["High"] * 2 + ["Medium"] * 6 + ["Low"] * 2
    clv = (spark.createDataFrame([("2023-01-01", t) for t in tiers],
                                 "snapshot_date string, clv_tier string")
           .withColumn("snapshot_date", F.to_date("snapshot_date")))
    trends = spark.createDataFrame([(100.0, 90.0)], "net_revenue double, gross_revenue double")
    assert all(ok for _, ok, _ in qc_checks(items, orders, cal, clv, trends))

    with pytest.raises(SystemExit):
        qc_checks(spark.range(5).toDF("id"), orders, cal, clv, trends)
