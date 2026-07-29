import pytest

from fixtures import ORDERS_IN_EUR, PREDICTIONS
from main import build_daily_summary, summary_metadata


def test_build_daily_summary_groups_by_day_and_computes_high_value_rate():
    summary = build_daily_summary(ORDERS_IN_EUR, PREDICTIONS)

    assert list(summary["order_date"]) == ["2026-01-01", "2026-01-02"]

    day_1 = summary.loc[summary["order_date"] == "2026-01-01"].iloc[0]
    assert day_1["num_orders"] == 2
    assert day_1["total_eur"] == pytest.approx(22.5)  # 18.0 + 4.5
    assert day_1["high_value_orders"] == 1
    assert day_1["high_value_rate"] == pytest.approx(0.5)

    day_2 = summary.loc[summary["order_date"] == "2026-01-02"].iloc[0]
    assert day_2["num_orders"] == 1
    assert day_2["total_eur"] == pytest.approx(9.0)
    assert day_2["high_value_rate"] == pytest.approx(1.0)


def test_build_daily_summary_drops_orders_without_a_matching_prediction():
    predictions_missing_order_3 = PREDICTIONS[PREDICTIONS["order_id"] != 3]

    summary = build_daily_summary(ORDERS_IN_EUR, predictions_missing_order_3)

    day_2 = summary.loc[summary["order_date"] == "2026-01-02"]
    assert day_2.empty  # order 3 was the only order on 2026-01-02


def test_summary_metadata_reports_days_total_and_average_rate():
    summary = build_daily_summary(ORDERS_IN_EUR, PREDICTIONS)

    meta = summary_metadata(summary)

    assert meta["num_days"] == 2
    assert meta["total_eur_sum"] == pytest.approx(31.5)  # 22.5 + 9.0
    assert meta["avg_high_value_rate"] == pytest.approx(0.75)  # (0.5 + 1.0) / 2


def test_summary_metadata_handles_empty_dataframe_without_error():
    empty_summary = build_daily_summary(ORDERS_IN_EUR.iloc[0:0], PREDICTIONS.iloc[0:0])

    meta = summary_metadata(empty_summary)

    assert meta["num_days"] == 0
    assert meta["total_eur_sum"] == 0.0
    assert meta["avg_high_value_rate"] == 0.0
