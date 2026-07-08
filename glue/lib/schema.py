"""Column-casing contracts (SDD §4.2 gap #9).

order_items and order_item_options ship UPPERCASE; date_dim is lowercase.
Silver standardises everything to lowercase snake_case. Because the source headers
are already snake-shaped, a simple lowercase is sufficient and lossless.
"""
from __future__ import annotations
from pyspark.sql import DataFrame


def standardize_casing(df: DataFrame) -> DataFrame:
    """Lowercase every column name (UPPERCASE -> lowercase snake_case)."""
    for c in df.columns:
        if c != c.lower():
            df = df.withColumnRenamed(c, c.lower())
    return df


# Expected lowercase columns after standardisation (contract / documentation)
ORDER_ITEMS_COLS = [
    "app_name", "restaurant_id", "creation_time_utc", "order_id", "user_id",
    "printed_card_number", "is_loyalty", "currency", "lineitem_id",
    "item_category", "item_name", "item_price", "item_quantity",
]
ORDER_ITEM_OPTIONS_COLS = [
    "order_id", "lineitem_id", "option_group_name", "option_name",
    "option_price", "option_quantity",
]
DATE_DIM_COLS = [
    "date_key", "year", "month", "week", "day_of_week",
    "is_weekend", "is_holiday", "holiday_name",
]
