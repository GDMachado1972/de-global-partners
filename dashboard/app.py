"""DE Global Partners — Streamlit dashboard (SDD §Step 6).

Six required views + a CLV overview, reading the Gold marts via data_access (Athena in
prod, local PySpark for the demo). Run:

  DATA_BACKEND=local LANDING=/path/to/csvs streamlit run streamlit/app.py
  DATA_BACKEND=athena ATHENA_S3_STAGING=... streamlit run streamlit/app.py
"""
import os
import sys
import pandas as pd
import streamlit as st

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, "..")))   # repo root (glue.*)
sys.path.insert(0, _HERE)                                        # dashboard/ (data_access)
import data_access as da  # noqa: E402
from data_access import money, pct  # noqa: E402

st.set_page_config(page_title="DE Global Partners — CLV & Behavioral Analytics",
                   layout="wide", initial_sidebar_state="expanded")


# ---------------------------------------------------------------- helpers
@st.cache_data(show_spinner=False)
def _load(table: str) -> pd.DataFrame:
    return da.load(table)


# ---------------------------------------------------------------- views
def view_overview():
    st.subheader("Customer Lifetime Value — Overview")
    clv_all = _load("g_fact_customer_clv_daily")
    latest = clv_all["snapshot_date"].max()
    clv = clv_all[clv_all["snapshot_date"] == latest]
    st.caption(f"As-of snapshot: {latest}")
    total_net_all = _load("g_fact_sales_trends")["net_revenue"].sum()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Identified customers", f"{clv['user_id'].nunique():,}")
    c2.metric("Total CLV — identified customers", money(clv["cum_net_revenue"].sum()))
    c3.metric("Total net revenue (all orders, incl. guests)", money(total_net_all))
    c4.metric("Avg CLV / customer", money(clv["cum_net_revenue"].mean()))
    c5.metric("Avg orders / customer", f"{clv['cum_orders'].mean():.2f}")
    st.caption(
        "CLV totals only identified (non-guest) customers; total net revenue includes "
        "guest orders too, so the two figures are not directly comparable."
    )

    st.markdown("**CLV tier distribution** (High = top 20% · Medium = mid 60% · Low = bottom 20%)")
    tiers = (clv.groupby("clv_tier")["user_id"].count()
             .reindex(["High", "Medium", "Low"]).rename("customers"))
    st.bar_chart(tiers)

    st.markdown("**Top customers by cumulative net revenue**")
    top = (clv.sort_values("cum_net_revenue", ascending=False)
           .head(15)[["user_id", "cum_net_revenue", "cum_orders",
                      "avg_order_value", "recency_days", "clv_tier"]].copy())
    top["cum_net_revenue"] = top["cum_net_revenue"].map(money)
    top["avg_order_value"] = top["avg_order_value"].map(money)
    st.dataframe(top, use_container_width=True, hide_index=True)


def view_segmentation():
    st.subheader("Customer Segmentation (RFM)")
    rfm = _load("g_dim_customer_rfm")
    seg = rfm.groupby("rfm_segment")["user_id"].count().rename("customers").sort_values(ascending=False)
    c1, c2 = st.columns([1, 2])
    with c1:
        st.markdown("**Segment sizes**")
        st.dataframe(seg.reset_index(), use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**Customers by segment**")
        st.bar_chart(seg)
    st.markdown("**R / F / M score mix** (1 = low, 5 = high)")
    scores = rfm[["r_score", "f_score", "m_score"]].melt(var_name="score", value_name="value")
    pivot = scores.pivot_table(index="value", columns="score", aggfunc=len, fill_value=0)
    st.bar_chart(pivot)


def view_churn():
    st.subheader("Churn Risk Indicators")
    churn = _load("g_fact_churn_indicators")
    threshold = st.slider("At-risk threshold (days since last order)", 15, 120, 45, step=5)
    at_risk = churn[churn["days_since_last_order"] > threshold]
    c1, c2, c3 = st.columns(3)
    c1.metric("Customers", f"{len(churn):,}")
    c2.metric(f"At risk (> {threshold}d)", f"{len(at_risk):,}")
    c3.metric("At-risk share", pct(len(at_risk) / max(len(churn), 1)))
    st.markdown("**Days since last order — distribution**")
    bins = pd.cut(churn["days_since_last_order"], bins=[0, 15, 30, 45, 60, 90, 180, 10000],
                  labels=["0-15", "16-30", "31-45", "46-60", "61-90", "91-180", "180+"])
    st.bar_chart(bins.value_counts().sort_index())
    st.markdown("**At-risk customers**")
    cols = [c for c in ["user_id", "days_since_last_order", "avg_inter_order_gap_days",
                        "orders"] if c in at_risk.columns]
    st.dataframe(at_risk.sort_values("days_since_last_order", ascending=False)[cols].head(200),
                 use_container_width=True, hide_index=True)


def view_sales_trends():
    st.subheader("Sales Trends & Seasonality")
    st.caption("Analysis window: full order history 2020–2024 (generated calendar, D1).")
    st_df = _load("g_fact_sales_trends")
    st_df["order_date"] = pd.to_datetime(st_df["order_date"])
    cats = sorted(st_df["item_category"].dropna().unique().tolist())
    locs = sorted(st_df["restaurant_id"].dropna().unique().tolist())
    c1, c2 = st.columns(2)
    sel_cat = c1.multiselect("Item category", cats)
    sel_loc = c2.multiselect("Location (restaurant_id)", locs)
    d = st_df
    if sel_cat:
        d = d[d["item_category"].isin(sel_cat)]
    if sel_loc:
        d = d[d["restaurant_id"].isin(sel_loc)]
    monthly = (d.assign(month=d["order_date"].dt.to_period("M").dt.to_timestamp())
               .groupby("month")[["net_revenue", "gross_revenue"]].sum())
    st.markdown("**Monthly revenue (net vs gross)**")
    st.line_chart(monthly)
    k1, k2, k3 = st.columns(3)
    k1.metric("Net revenue", money(d["net_revenue"].sum()))
    k2.metric("Gross revenue", money(d["gross_revenue"].sum()))
    k3.metric("Category order-lines", f"{int(d['orders'].sum()):,}")
    st.caption(
        "g_fact_sales_trends is at grain date × location × item category, so an order "
        "touching multiple categories is counted once per category here — this is not "
        "the distinct-order count."
    )
    st.markdown("**Revenue by item category**")
    by_cat = d.groupby("item_category")["net_revenue"].sum().sort_values(ascending=False).head(15)
    st.bar_chart(by_cat)


def view_loyalty():
    st.subheader("Loyalty Program Impact")
    li = _load("g_fact_loyalty_impact").copy()
    li["cohort"] = li["is_loyalty"].map({True: "Loyalty", False: "Non-loyalty"})
    li = li.set_index("cohort")
    c1, c2, c3 = st.columns(3)
    metric_cols = {"avg_clv": "Avg CLV", "avg_spend": "Avg spend",
                   "avg_orders_per_customer": "Avg orders", "repeat_order_rate": "Repeat rate"}
    st.markdown("**Loyalty vs non-loyalty**")
    show = li[[c for c in metric_cols if c in li.columns] + ["customers"]].rename(columns=metric_cols)
    st.dataframe(show, use_container_width=True)
    st.caption(
        "Loyalty is a per-order flag, not a per-customer attribute — a customer with both "
        "loyalty and non-loyalty orders is counted in both cohorts below, so the two "
        "customer counts do not sum to the total identified-customer count."
    )
    if "avg_clv" in li.columns:
        st.markdown("**Average CLV by cohort**")
        st.bar_chart(li["avg_clv"])
    if "repeat_order_rate" in li.columns:
        st.markdown("**Repeat-order rate by cohort**")
        st.bar_chart(li["repeat_order_rate"])


def view_location():
    st.subheader("Location Performance")
    lp = _load("g_fact_location_performance").sort_values("revenue_rank")
    c1, c2, c3 = st.columns(3)
    c1.metric("Locations", f"{lp['restaurant_id'].nunique():,}")
    c2.metric("Total net revenue", money(lp["total_net_revenue"].sum()))
    c3.metric("Top location revenue", money(lp["total_net_revenue"].max()))
    st.markdown("**Net revenue by location (ranked)**")
    st.bar_chart(lp.set_index("restaurant_id")["total_net_revenue"])
    disp = lp.copy()
    for c in ("total_net_revenue", "aov"):
        if c in disp.columns:
            disp[c] = disp[c].map(money)
    st.dataframe(disp, use_container_width=True, hide_index=True)


def view_pricing_addon():
    st.subheader("Pricing & Discount Effectiveness")
    st.warning(
        "**Data limitation (SME decision D2).** The supplied dataset contains **no discount "
        "signal** — `OPTION_PRICE` and `ITEM_PRICE` have zero negative values in either file. "
        "The originally specified discount-effectiveness analysis therefore **cannot be "
        "performed**. Shown below as an optional supplement is paid **add-on / modifier** "
        "revenue contribution, which is additive (not a discount analysis).")
    ao = _load("g_fact_addon_revenue").copy()
    ao["cohort"] = ao["has_paid_modifier"].map({True: "With paid add-on", False: "No paid add-on"})
    ao = ao.set_index("cohort")
    c1, c2 = st.columns(2)
    c1.metric("Add-on revenue (total)", money(ao["addon_revenue"].sum()))
    if "orders" in ao.columns:
        total_orders = ao["orders"].sum()
        attach = ao["orders"].get("With paid add-on", 0) / max(total_orders, 1)
        c2.metric("Paid-modifier attach rate", pct(attach))
    st.markdown("**Orders by add-on presence**")
    st.bar_chart(ao["orders"])
    st.markdown("**Average order value by add-on presence**")
    if "aov" in ao.columns:
        st.bar_chart(ao["aov"])


VIEWS = {
    "CLV Overview": view_overview,
    "Customer Segmentation": view_segmentation,
    "Churn Risk": view_churn,
    "Sales Trends": view_sales_trends,
    "Loyalty Impact": view_loyalty,
    "Location Performance": view_location,
    "Pricing & Add-on (D2)": view_pricing_addon,
}


def main():
    st.sidebar.title("DE Global Partners")
    st.sidebar.caption("Daily CLV & Behavioral Analytics · Alltown Fresh")
    st.sidebar.caption(f"Backend: `{da.BACKEND}`")
    choice = st.sidebar.radio("View", list(VIEWS.keys()))
    st.sidebar.markdown("---")
    st.sidebar.caption("Numbers are as-of the latest snapshot. CLV = cumulative net "
                       "historical revenue (non-predictive).")
    try:
        VIEWS[choice]()
    except Exception as e:
        st.error(f"Could not load data for this view ({da.BACKEND} backend): {e}")
        if da.BACKEND == "local":
            st.info("Local backend needs `LANDING=/path/to/the/three/csvs`.")


main()   # Streamlit executes the script top-to-bottom
