"""Cleaning-rule tests.

Unit tests use tiny synthetic DataFrames and always run (CI-safe, no data needed).
The integration test runs the real PySpark cleaning on the actual CSVs when DATA_DIR
is set, asserting the canonical post-cleaning figures validated against source.
"""
import os
import pytest
from pyspark.sql import Row
from glue.lib.cleaning import clean_order_items, clean_order_item_options, DEV_APP_NAME

# canonical figures (validated vs source; SDD §1 QA + config)
SILVER_ORDER_ITEMS = 202_692
OPTIONS_DEDUPED = 190_718

# source arrives as all-string columns; explicit schema avoids null-type inference errors
OI_SCHEMA = ("app_name string, restaurant_id string, creation_time_utc string, "
             "order_id string, user_id string, printed_card_number string, "
             "is_loyalty string, currency string, lineitem_id string, "
             "item_category string, item_name string, item_price string, "
             "item_quantity string")


def _oi_row(**kw):
    base = dict(app_name="Alltown Fresh", restaurant_id="R1",
                creation_time_utc="2023-05-01T10:00:00.123Z", order_id="O1",
                user_id="U1", printed_card_number=None, is_loyalty="true",
                currency="USD", lineitem_id="L1", item_category="Coffee",
                item_name="Latte", item_price="4.50", item_quantity="2")
    base.update(kw)
    return Row(**base)


def _oi_df(spark, rows):
    return spark.createDataFrame(rows, schema=OI_SCHEMA)


def test_drops_malformed_and_dev_rows(spark):
    df = _oi_df(spark, [
        _oi_row(),                                             # keep
        _oi_row(lineitem_id=None, item_name=None),             # malformed -> drop
        _oi_row(app_name=DEV_APP_NAME),                        # dev -> drop
        _oi_row(app_name=f"  {DEV_APP_NAME}  "),               # dev (whitespace) -> drop
    ])
    out = clean_order_items(df)
    assert out.count() == 1


def test_casing_and_derived_columns(spark):
    out = clean_order_items(_oi_df(spark, [_oi_row()]))
    assert "app_name" in out.columns
    r = out.collect()[0]
    assert r["is_guest"] is False
    assert r["gross_line"] == pytest.approx(9.0)          # 4.50 * 2
    assert r["order_date"].isoformat() == "2023-05-01"
    assert r["is_loyalty"] is True


def test_guest_flag_on_null_user(spark):
    out = clean_order_items(_oi_df(spark, [_oi_row(user_id=None)]))
    assert out.collect()[0]["is_guest"] is True


def test_guest_flag_on_blank_string_user_id(spark):
    # a JDBC/RDS source can land guests as "" or whitespace instead of NULL (unlike CSV,
    # which already parses empty fields as NULL) — both must normalize to a NULL user_id
    # and is_guest=True, or guest orders collapse onto one bogus "" customer.
    out = clean_order_items(_oi_df(spark, [
        _oi_row(order_id="O1", lineitem_id="L1", user_id=""),
        _oi_row(order_id="O2", lineitem_id="L2", user_id="   "),
    ]))
    rows = {r["order_id"]: r for r in out.collect()}
    assert rows["O1"]["is_guest"] is True
    assert rows["O1"]["user_id"] is None
    assert rows["O2"]["is_guest"] is True
    assert rows["O2"]["user_id"] is None


def test_iso_timestamp_without_fractional_seconds(spark):
    # the 187 rows lacking fractional seconds must still parse (not be dropped)
    out = clean_order_items(_oi_df(spark, [
        _oi_row(creation_time_utc="2021-11-03T08:15:42Z")]))
    assert out.filter("order_ts is null").count() == 0
    assert out.collect()[0]["order_date"].isoformat() == "2021-11-03"


def test_options_dedup(spark):
    def opt(**kw):
        b = dict(ORDER_ID="O1", LINEITEM_ID="L1", OPTION_GROUP_NAME="Milk",
                 OPTION_NAME="Oat", OPTION_PRICE="0.70", OPTION_QUANTITY="1")
        b.update(kw); return Row(**b)
    df = spark.createDataFrame([opt(), opt(), opt(OPTION_NAME="Soy")])  # 2 identical + 1
    out = clean_order_item_options(df)
    assert out.count() == 2
    assert out.filter("option_line is null").count() == 0


# ---------------------------------------------------------------- integration
@pytest.mark.integration
def test_real_data_silver_counts(spark, data_dir):
    if not data_dir:
        pytest.skip("DATA_DIR not set — skipping real-data integration test")
    oi = spark.read.option("header", True).option("multiLine", True) \
        .option("quote", '"').option("escape", '"') \
        .csv(os.path.join(data_dir, "order_items.csv"))
    cleaned = clean_order_items(oi)
    assert cleaned.count() == SILVER_ORDER_ITEMS
    assert cleaned.filter("order_ts is null").count() == 0        # all timestamps parsed

    op = spark.read.option("header", True).csv(
        os.path.join(data_dir, "order_item_options.csv"))
    assert clean_order_item_options(op).count() == OPTIONS_DEDUPED
