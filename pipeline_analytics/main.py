import pandas as pd
from dagster import (
    AssetCheckResult,
    Definitions,
    MetadataValue,
    Output,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

import db


def build_daily_summary(orders_in_eur: pd.DataFrame, predictions: pd.DataFrame) -> pd.DataFrame:
    """Join `orders_in_eur` (written by pipeline_fx) with
    `order_value_predictions` (written by pipeline_ml) on `order_id`, then
    aggregate into a per-day summary: number of orders, total EUR value,
    and the share of orders predicted as high-value.

    Kept as a pure function (no Dagster/db imports inside) so it's easy to
    unit test without a real database.
    """
    merged = orders_in_eur.merge(predictions, on="order_id", how="inner")

    grouped = (
        merged.groupby("order_date")
        .agg(
            num_orders=("order_id", "count"),
            total_eur=("total_eur", "sum"),
            high_value_orders=("predicted_label", "sum"),
        )
        .reset_index()
    )
    grouped["high_value_rate"] = grouped["high_value_orders"] / grouped["num_orders"]

    return grouped.sort_values("order_date").reset_index(drop=True)


def summary_metadata(summary: pd.DataFrame) -> dict:
    """Plain-dict summary of the daily summary table, used to populate
    Dagster's Output(metadata=...) for `daily_order_summary`."""
    return {
        "num_days": len(summary),
        "total_eur_sum": float(summary["total_eur"].sum()) if len(summary) else 0.0,
        "avg_high_value_rate": float(summary["high_value_rate"].mean()) if len(summary) else 0.0,
    }


@asset
def daily_order_summary() -> Output[pd.DataFrame]:
    orders_in_eur = db.read_table("orders_in_eur")
    predictions = db.read_table("order_value_predictions")
    summary = build_daily_summary(orders_in_eur, predictions)
    meta = summary_metadata(summary)
    return Output(
        value=summary,
        metadata={
            "num_days": MetadataValue.int(meta["num_days"]),
            "total_eur_sum": MetadataValue.float(meta["total_eur_sum"]),
            "avg_high_value_rate": MetadataValue.float(meta["avg_high_value_rate"]),
            "preview": MetadataValue.text(summary.head().to_string(index=False)),
        },
    )


@asset_check(asset=daily_order_summary)
def daily_summary_quality_check(daily_order_summary: pd.DataFrame) -> AssetCheckResult:
    """Fails if any day has a negative EUR total (would mean a bad join or
    bad upstream data) or a high_value_rate outside the valid 0-1 range."""
    has_negative_total = bool((daily_order_summary["total_eur"] < 0).any())
    rate_in_range = bool(daily_order_summary["high_value_rate"].between(0, 1).all())
    passed = (not has_negative_total) and rate_in_range
    return AssetCheckResult(
        passed=passed,
        metadata={
            "has_negative_total": has_negative_total,
            "high_value_rate_in_range": rate_in_range,
        },
    )


@asset
def daily_order_summary_table(daily_order_summary: pd.DataFrame) -> Output[int]:
    row_count = db.load_table(daily_order_summary, "daily_order_summary")
    return Output(
        value=row_count,
        metadata={
            "num_rows": MetadataValue.int(row_count),
            "table_name": MetadataValue.text("daily_order_summary"),
        },
    )


refresh_analytics_job = define_asset_job(name="refresh_analytics_job")

refresh_analytics_daily = ScheduleDefinition(
    name="refresh_analytics_daily",
    job=refresh_analytics_job,
    # Runs after refresh_fx_daily (06:00) and refresh_ml_weekly, so the
    # source tables it reads are already fresh for the day.
    cron_schedule="0 7 * * *",
)

defs = Definitions(
    assets=[daily_order_summary, daily_order_summary_table],
    asset_checks=[daily_summary_quality_check],
    jobs=[refresh_analytics_job],
    schedules=[refresh_analytics_daily],
)
