"""Dashboard data-access tests."""
import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dashboard")))
import data_access as da  # noqa: E402


def test_format_helpers():
    assert da.money(1234.5) == "$1,234.50"      # visual-QA: fixed 2dp, not rounded to int
    assert da.money(2.78) == "$2.78"
    assert da.pct(0.226) == "22.6%"
    assert da.money("bad") == "—"


def test_gold_tables_registry():
    assert len(da.GOLD_TABLES) == 7
    assert "g_fact_customer_clv_daily" in da.GOLD_TABLES


@pytest.mark.integration
def test_local_marts_build(spark, data_dir):
    if not data_dir:
        pytest.skip("DATA_DIR not set")
    marts = da.build_local_marts(spark, data_dir)
    assert set(marts.keys()) == set(da.GOLD_TABLES)
    clv = marts["g_fact_customer_clv_daily"]
    assert len(clv) == 20_059
    assert set(clv["clv_tier"].unique()) <= {"High", "Medium", "Low"}
    # every mart returns a non-empty pandas frame
    for name, df in marts.items():
        assert len(df) > 0, name
