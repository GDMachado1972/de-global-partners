"""Bronze -> Silver cleaning rules (SDD §4.3, SME-ratified).

Pure functions on DataFrames so they run identically in pytest, CI, and Glue.
Rules (order_items):
  1. drop 1 malformed record (embedded-newline artifact: null lineitem_id/item_name)
  2. exclude APP_NAME='Alltown Fresh - DEVELOPMENT' test rows (826)
  3. parse creation_time_utc as ISO-8601 (recovers 187 rows lacking fractional secs)
  4. flag null user_id as guest (kept for revenue totals; excluded from CLV/RFM downstream)
  5. cast numerics; derive gross_line = item_price * item_quantity
Rules (order_item_options):
  6. de-duplicate identical rows (2,299)
  7. cast numerics; derive option_line = option_price * option_quantity
     (option_price >= 0 in this data — additive, no discount signal; SDD §4.3 D2)
"""
from __future__ import annotations
from pyspark.sql import DataFrame, functions as F
from .schema import standardize_casing

DEV_APP_NAME = "Alltown Fresh - DEVELOPMENT"
# ISO-8601 with optional fractional seconds and a trailing 'Z' offset.
_TS_FMT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][.SSS]X"


def clean_order_items(df: DataFrame) -> DataFrame:
    df = standardize_casing(df)
    # 1 + 2: drop malformed and DEVELOPMENT test rows
    df = df.filter(F.col("lineitem_id").isNotNull() & F.col("item_name").isNotNull())
    df = df.filter(F.trim(F.col("app_name")) != F.lit(DEV_APP_NAME))
    # 3: robust ISO-8601 parse -> timestamp + date
    df = df.withColumn("order_ts", F.to_timestamp("creation_time_utc", _TS_FMT))
    df = df.withColumn("order_date", F.to_date("order_ts"))
    # 4: guest flag — normalize blank/whitespace user_id to NULL first. CSV sources
    # already parse empty fields as NULL, but a JDBC/RDS source can store a genuinely
    # empty string instead, which would otherwise slip through isNull() and collapse
    # every guest order onto one bogus "" customer.
    df = df.withColumn("user_id",
                       F.when(F.trim(F.col("user_id")) == "", None).otherwise(F.col("user_id")))
    df = df.withColumn("is_guest", F.col("user_id").isNull())
    # 5: numeric casts + line revenue
    df = (df
          .withColumn("item_price", F.col("item_price").cast("double"))
          .withColumn("item_quantity", F.col("item_quantity").cast("int"))
          .withColumn("is_loyalty",
                      F.lower(F.col("is_loyalty").cast("string")) == F.lit("true")))
    df = df.withColumn("gross_line",
                       F.round(F.col("item_price") * F.col("item_quantity"), 2))
    return df


def clean_order_item_options(df: DataFrame) -> DataFrame:
    df = standardize_casing(df)
    df = df.dropDuplicates()                                    # 6
    df = (df
          .withColumn("option_price", F.col("option_price").cast("double"))
          .withColumn("option_quantity", F.col("option_quantity").cast("int")))
    df = df.withColumn("option_line",
                       F.round(F.col("option_price") * F.col("option_quantity"), 2))  # 7
    return df
