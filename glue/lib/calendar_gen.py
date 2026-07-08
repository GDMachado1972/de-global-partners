"""Calendar-dimension generator (SDD decision D1, SME-approved).

The shipped date_dim covers only 2023 but orders span 2020-04-21 -> 2024-02-21, so an
inner calendar join would silently drop ~60% of rows. We therefore GENERATE the calendar
in Silver from the true min/max order date, guaranteeing 100% join coverage.

Output schema matches the source date_dim (SDD §4.1) so downstream code is unchanged:
  date_key(date), year, month(int 1-12), week, day_of_week(str),
  is_weekend(bool), is_holiday(bool), holiday_name(str)

Holidays: US federal holidays via the `holidays` library across the spanned years
[ASSUMPTION — swap for the client's official holiday list if provided]. If the library
is unavailable, is_holiday=false / holiday_name=null (calendar still valid).
"""
from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame, functions as F


def _holiday_rows(start_year: int, end_year: int):
    try:
        import holidays as _h
    except Exception:
        return []
    us = _h.US(years=range(start_year, end_year + 1))
    return [(d.isoformat(), name) for d, name in sorted(us.items())]


def generate_calendar(spark: SparkSession, min_date: str, max_date: str) -> DataFrame:
    """Build a contiguous daily calendar for [min_date, max_date] (inclusive).

    min_date / max_date are 'YYYY-MM-DD' strings (derive from min/max order_date).
    """
    base = (spark.sql(
        f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), "
        f"interval 1 day)) AS date_key"))

    df = (base
          .withColumn("year", F.year("date_key"))
          .withColumn("month", F.month("date_key"))
          .withColumn("week", F.weekofyear("date_key"))
          .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
          .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7)))

    hol = _holiday_rows(int(min_date[:4]), int(max_date[:4]))
    if hol:
        hdf = (spark.createDataFrame(hol, ["h_date", "holiday_name"])
               .withColumn("h_date", F.to_date("h_date")))
        df = (df.join(hdf, df.date_key == hdf.h_date, "left")
                .withColumn("is_holiday", F.col("h_date").isNotNull())
                .drop("h_date"))
    else:
        df = (df.withColumn("is_holiday", F.lit(False))
                .withColumn("holiday_name", F.lit(None).cast("string")))

    return df.select("date_key", "year", "month", "week", "day_of_week",
                     "is_weekend", "is_holiday", "holiday_name").orderBy("date_key")
