"""Gold-layer transforms (SDD §7.2, §7.3, §8) — pure PySpark, unit-testable.

Primary : build_clv_daily -> g_fact_customer_clv_daily (grain user_id x snapshot_date)
Marts   : build_rfm, build_churn, build_sales_trends, build_loyalty_impact,
          build_location_performance, build_addon_revenue (optional supplement, D2)
Shared  : build_order_lines / build_orders (conformed order & line grain from Silver)

Revenue identity (SDD §8): net_line = gross_line + Σ(option_price*qty); options are
additive here (no discounts, D2), so net >= gross always. Guests (null user_id) are
excluded from CLV/RFM/churn; retained in sales/location/add-on totals.
"""
from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

HIGH_Q, LOW_Q = 0.80, 0.20            # top 20% / bottom 20% (SDD §7.2)
AT_RISK_DAYS = 45                     # churn threshold (SDD §8)


# ------------------------------------------------------------------ shared grain
def build_order_lines(s_items: DataFrame, s_options: DataFrame) -> DataFrame:
    """Line grain with net_line = gross_line + paid add-ons (SDD §8)."""
    opt = (s_options.groupBy("order_id", "lineitem_id")
           .agg(F.round(F.sum("option_line"), 2).alias("opt_revenue")))
    return (s_items.join(opt, ["order_id", "lineitem_id"], "left")
            .withColumn("opt_revenue", F.coalesce("opt_revenue", F.lit(0.0)))
            .withColumn("net_line", F.round(F.col("gross_line") + F.col("opt_revenue"), 2)))


def build_orders(s_order_lines: DataFrame) -> DataFrame:
    """Order grain with order_net (SDD §7)."""
    return (s_order_lines.groupBy("order_id", "user_id", "restaurant_id",
                                  "is_loyalty", "is_guest", "order_date")
            .agg(F.round(F.sum("net_line"), 2).alias("order_net")))


# ------------------------------------------------------------------ snapshots
def month_end_snapshots(spark: SparkSession, calendar: DataFrame,
                        lo: str, hi: str) -> DataFrame:
    """Month-end snapshot dates within [lo, hi] — tractable historical backfill.
    [ASSUMPTION] cadence = month-end for backfill; production daily run passes a
    single run_date via daily_snapshot()."""
    return (calendar.filter((F.col("date_key") >= F.lit(lo)) & (F.col("date_key") <= F.lit(hi)))
            .groupBy("year", "month").agg(F.max("date_key").alias("snapshot_date"))
            .select("snapshot_date"))


def daily_snapshot(spark: SparkSession, run_date: str) -> DataFrame:
    return spark.createDataFrame([(run_date,)], "snapshot_date string") \
        .withColumn("snapshot_date", F.to_date("snapshot_date"))


# ------------------------------------------------------------------ PRIMARY: CLV daily
def build_clv_daily(orders: DataFrame, snapshots: DataFrame) -> DataFrame:
    """g_fact_customer_clv_daily — cumulative per-customer state as of each snapshot_date,
    with as-of-date High/Medium/Low tiers by cum_net_revenue percentile (SDD §7.2)."""
    o = orders.filter(F.col("user_id").isNotNull())          # exclude guests
    snaps = F.broadcast(snapshots)                            # small (daily=1, monthly~47)
    # range join: attribute each order to every snapshot_date on/after its order_date
    j = o.join(snaps, o.order_date <= snaps.snapshot_date)
    agg = (j.groupBy("user_id", "snapshot_date")
           .agg(F.round(F.sum("order_net"), 2).alias("cum_net_revenue"),
                F.countDistinct("order_id").alias("cum_orders"),
                F.min("order_date").alias("first_order_date"),
                F.max("order_date").alias("last_order_date")))
    agg = (agg
           .withColumn("recency_days", F.datediff("snapshot_date", "last_order_date"))
           .withColumn("avg_order_value",
                       F.round(F.col("cum_net_revenue") / F.col("cum_orders"), 2)))
    # quintiles per snapshot: bucket 5 = High (top 20%), 1 = Low (bottom 20%), 2-4 = Medium
    w = Window.partitionBy("snapshot_date").orderBy(F.col("cum_net_revenue").asc())
    agg = agg.withColumn("_b", F.ntile(5).over(w))
    agg = agg.withColumn("clv_tier",
                         F.when(F.col("_b") == 5, "High")
                          .when(F.col("_b") == 1, "Low")
                          .otherwise("Medium")).drop("_b")
    return agg.select("user_id", "snapshot_date", "cum_net_revenue", "cum_orders",
                      "first_order_date", "last_order_date", "recency_days",
                      "avg_order_value", "clv_tier")


# ------------------------------------------------------------------ MARTS
def build_rfm(orders: DataFrame, analysis_date: str) -> DataFrame:
    o = orders.filter(F.col("user_id").isNotNull())
    base = (o.groupBy("user_id")
            .agg(F.datediff(F.lit(analysis_date), F.max("order_date")).alias("recency"),
                 F.countDistinct("order_id").alias("frequency"),
                 F.round(F.sum("order_net"), 2).alias("monetary")))
    # 1-5 quintile scores: recency lower = better (=> higher score)
    base = (base
            .withColumn("r_score", 6 - F.ntile(5).over(Window.orderBy(F.col("recency").asc())))
            .withColumn("f_score", F.ntile(5).over(Window.orderBy(F.col("frequency").asc())))
            .withColumn("m_score", F.ntile(5).over(Window.orderBy(F.col("monetary").asc()))))
    seg = (F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "VIP")
            .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), "New")
            .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "Churn-Risk")
            .otherwise("Other"))
    return base.withColumn("rfm_segment", seg).withColumn("analysis_date", F.lit(analysis_date))


def build_churn(orders: DataFrame, analysis_date: str, period_days: int = 90) -> DataFrame:
    o = orders.filter(F.col("user_id").isNotNull())
    # per-customer order-date list for inter-order gaps
    dates = o.select("user_id", "order_date").distinct()
    w = Window.partitionBy("user_id").orderBy("order_date")
    gaps = (dates.withColumn("prev", F.lag("order_date").over(w))
            .withColumn("gap", F.datediff("order_date", "prev")))
    gap_avg = gaps.groupBy("user_id").agg(F.round(F.avg("gap"), 1).alias("avg_inter_order_gap_days"))
    # recent vs prior spend windows
    ad = F.lit(analysis_date)
    recent = (o.filter(F.datediff(ad, "order_date") < period_days)
              .groupBy("user_id").agg(F.sum("order_net").alias("recent")))
    prior = (o.filter((F.datediff(ad, "order_date") >= period_days) &
                      (F.datediff(ad, "order_date") < 2 * period_days))
             .groupBy("user_id").agg(F.sum("order_net").alias("prior")))
    base = (o.groupBy("user_id")
            .agg(F.max("order_date").alias("last_order_date"),
                 F.countDistinct("order_id").alias("orders"))
            .withColumn("days_since_last_order", F.datediff(ad, "last_order_date")))
    out = (base.join(gap_avg, "user_id", "left").join(recent, "user_id", "left")
           .join(prior, "user_id", "left")
           .withColumn("pct_spend_change",
                       F.when(F.col("prior").isNull() | (F.col("prior") == 0), None)
                        .otherwise(F.round((F.coalesce("recent", F.lit(0.0)) - F.col("prior"))
                                           / F.col("prior"), 3)))
           .withColumn("at_risk_flag", F.col("days_since_last_order") > AT_RISK_DAYS)
           .withColumn("analysis_date", ad))
    return out.select("user_id", "analysis_date", "days_since_last_order",
                      "avg_inter_order_gap_days", "pct_spend_change", "at_risk_flag", "orders")


def build_sales_trends(order_lines: DataFrame, calendar: DataFrame) -> DataFrame:
    agg = (order_lines.groupBy("order_date", "restaurant_id", "item_category")
           .agg(F.countDistinct("order_id").alias("orders"),
                F.round(F.sum("gross_line"), 2).alias("gross_revenue"),
                F.round(F.sum("net_line"), 2).alias("net_revenue")))
    cal = calendar.select("date_key", "year", "month", "week",
                          "is_weekend", "is_holiday", "holiday_name")
    return agg.join(cal, agg.order_date == cal.date_key, "left").drop("date_key")


def build_loyalty_impact(orders: DataFrame) -> DataFrame:
    per_user = (orders.filter(F.col("user_id").isNotNull())
                .groupBy("is_loyalty", "user_id")
                .agg(F.countDistinct("order_id").alias("orders"),
                     F.sum("order_net").alias("spend")))
    return (per_user.groupBy("is_loyalty")
            .agg(F.countDistinct("user_id").alias("customers"),
                 F.round(F.avg("spend"), 2).alias("avg_spend"),
                 F.round(F.avg("orders"), 3).alias("avg_orders_per_customer"),
                 F.round(F.avg(F.when(F.col("orders") > 1, 1.0).otherwise(0.0)), 3)
                 .alias("repeat_order_rate"),
                 F.round(F.avg("spend"), 2).alias("avg_clv")))


def build_location_performance(orders: DataFrame) -> DataFrame:
    span = orders.selectExpr("datediff(max(order_date), min(order_date)) + 1 d").collect()[0]["d"]
    agg = (orders.groupBy("restaurant_id")   # includes guests (location revenue retains them)
           .agg(F.round(F.sum("order_net"), 2).alias("total_net_revenue"),
                F.countDistinct("order_id").alias("orders"),
                F.countDistinct("user_id").alias("customers"))
           .withColumn("aov", F.round(F.col("total_net_revenue") / F.col("orders"), 2))
           .withColumn("orders_per_day", F.round(F.col("orders") / F.lit(span), 2))
           .withColumn("revenue_rank",
                       F.row_number().over(Window.orderBy(F.col("total_net_revenue").desc()))))
    return agg


def build_addon_revenue(order_lines: DataFrame) -> DataFrame:
    """Optional supplement (D2): paid add-on / modifier revenue contribution.
    NOT a discount analysis — the dataset has no discount signal."""
    per_order = (order_lines.groupBy("order_id")
                 .agg(F.round(F.sum("gross_line"), 2).alias("gross_revenue"),
                      F.round(F.sum("opt_revenue"), 2).alias("addon_revenue"))
                 .withColumn("has_paid_modifier", F.col("addon_revenue") > 0))
    return (per_order.groupBy("has_paid_modifier")
            .agg(F.countDistinct("order_id").alias("orders"),
                 F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
                 F.round(F.sum("addon_revenue"), 2).alias("addon_revenue"),
                 F.round(F.avg("gross_revenue"), 2).alias("aov")))
