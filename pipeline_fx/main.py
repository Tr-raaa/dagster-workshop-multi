import pandas as pd
from dagster import Definitions, MetadataValue, Output, ScheduleDefinition, asset, define_asset_job

import db
import source


def build_orders_in_eur(
    orders: pd.DataFrame, products: pd.DataFrame, exchange_rates: pd.DataFrame
) -> pd.DataFrame:
    """Join orders (from pipeline_products) with products (from
    pipeline_products) and exchange_rates (from this pipeline) to compute
    each order's total value converted from USD to EUR.

    Kept as a pure function (no Dagster/db imports inside) so it's easy to
    unit test without a real database.
    """
    eur_rates = exchange_rates.loc[exchange_rates["quote_currency"] == "EUR", "rate"]
    if eur_rates.empty:
        raise ValueError("No USD -> EUR rate found in exchange_rates table")
    eur_rate = eur_rates.iloc[0]

    merged = orders.merge(products, on="product_id", how="inner")
    merged["total_usd"] = merged["quantity"] * merged["price"]
    merged["total_eur"] = merged["total_usd"] * eur_rate

    return merged[
        [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "total_usd",
            "total_eur",
        ]
    ]


@asset
def raw_exchange_rates() -> pd.DataFrame:
    payload = source.fetch_latest_rates(base="USD")
    rows = [
        {"base_currency": "USD", "quote_currency": currency, "rate": rate}
        for currency, rate in payload["rates"].items()
    ]
    return pd.DataFrame(rows)


@asset
def exchange_rates_table(raw_exchange_rates: pd.DataFrame) -> int:
    return db.load_table(raw_exchange_rates, "exchange_rates")


# TODO(exercise-2): DONE — see orders_in_eur / orders_in_eur_table below.
# This is a cross-container exercise: pipeline_fx and pipeline_products both
# write to the same warehouse Postgres, so these assets just read those
# tables directly with db.read_table(), with no direct code dependency
# between the two containers.


@asset
def orders_in_eur() -> Output[pd.DataFrame]:
    orders = db.read_table("orders")
    products = db.read_table("products")
    exchange_rates = db.read_table("exchange_rates")
    result = build_orders_in_eur(orders, products, exchange_rates)
    return Output(
        value=result,
        metadata={
            "num_rows": MetadataValue.int(len(result)),
            "total_eur_sum": MetadataValue.float(float(result["total_eur"].sum())),
            "preview": MetadataValue.text(result.head().to_string(index=False)),
        },
    )


@asset
def orders_in_eur_table(orders_in_eur: pd.DataFrame) -> Output[int]:
    row_count = db.load_table(orders_in_eur, "orders_in_eur")
    return Output(
        value=row_count,
        metadata={
            "num_rows": MetadataValue.int(row_count),
            "table_name": MetadataValue.text("orders_in_eur"),
        },
    )


refresh_fx_job = define_asset_job(name="refresh_fx_job")

refresh_fx_daily = ScheduleDefinition(
    name="refresh_fx_daily",
    job=refresh_fx_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[raw_exchange_rates, exchange_rates_table, orders_in_eur, orders_in_eur_table],
    jobs=[refresh_fx_job],
    schedules=[refresh_fx_daily],
)
