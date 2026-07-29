from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
from fixtures import ORDERS_IN_EUR, PREDICTIONS
from main import daily_order_summary, daily_order_summary_table, daily_summary_quality_check


def test_analytics_pipeline_produces_summary_and_passes_quality_check():
    loaded = {}

    def fake_read_table(table_name: str) -> pd.DataFrame:
        return {
            "orders_in_eur": ORDERS_IN_EUR,
            "order_value_predictions": PREDICTIONS,
        }[table_name]

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    with patch.object(db, "read_table", side_effect=fake_read_table), patch.object(
        db, "load_table", side_effect=fake_load_table
    ):
        result = materialize(
            [daily_order_summary, daily_order_summary_table, daily_summary_quality_check]
        )

    assert result.success

    summary = loaded["daily_order_summary"]
    assert len(summary) == 2
    assert set(summary.columns) == {
        "order_date",
        "num_orders",
        "total_eur",
        "high_value_orders",
        "high_value_rate",
    }

    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 1
    assert evaluations[0].passed is True


def test_quality_check_fails_when_totals_are_negative():
    bad_orders = ORDERS_IN_EUR.copy()
    bad_orders.loc[0, "total_eur"] = -5.0

    def fake_read_table(table_name: str) -> pd.DataFrame:
        return {
            "orders_in_eur": bad_orders,
            "order_value_predictions": PREDICTIONS,
        }[table_name]

    with patch.object(db, "read_table", side_effect=fake_read_table), patch.object(
        db, "load_table", return_value=0
    ):
        result = materialize(
            [daily_order_summary, daily_order_summary_table, daily_summary_quality_check]
        )

    evaluations = result.get_asset_check_evaluations()
    assert evaluations[0].passed is False
