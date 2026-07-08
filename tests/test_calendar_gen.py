"""Calendar-generator tests (SDD D1)."""
import os
import pytest
from glue.lib.calendar_gen import generate_calendar
from glue.lib.cleaning import clean_order_items


def test_contiguous_span_and_schema(spark):
    cal = generate_calendar(spark, "2020-04-21", "2024-02-21")
    assert cal.columns == ["date_key", "year", "month", "week", "day_of_week",
                           "is_weekend", "is_holiday", "holiday_name"]
    # inclusive day count for the real order span
    assert cal.count() == 1402
    assert cal.filter("date_key is null").count() == 0
    # no gaps: distinct dates == row count
    assert cal.select("date_key").distinct().count() == 1402
    years = [r[0] for r in cal.select("year").distinct().orderBy("year").collect()]
    assert years == [2020, 2021, 2022, 2023, 2024]


def test_weekend_and_month_types(spark):
    cal = generate_calendar(spark, "2023-01-01", "2023-01-31")
    row = cal.filter("date_key = '2023-01-07'").collect()[0]   # a Saturday
    assert row["is_weekend"] is True
    assert row["month"] == 1
    assert row["day_of_week"] == "Saturday"


@pytest.mark.integration
def test_calendar_covers_all_orders(spark, data_dir):
    if not data_dir:
        pytest.skip("DATA_DIR not set")
    oi = spark.read.option("header", True).option("multiLine", True) \
        .option("quote", '"').option("escape", '"') \
        .csv(os.path.join(data_dir, "order_items.csv"))
    cleaned = clean_order_items(oi)
    bounds = cleaned.selectExpr("min(order_date) lo", "max(order_date) hi").collect()[0]
    cal = generate_calendar(spark, bounds["lo"].isoformat(), bounds["hi"].isoformat())
    # 100% join coverage: every order_date exists in the generated calendar
    missing = (cleaned.select("order_date").distinct()
               .join(cal, cleaned.order_date == cal.date_key, "left_anti").count())
    assert missing == 0
