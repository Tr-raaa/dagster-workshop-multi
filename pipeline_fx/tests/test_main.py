from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
import source
from main import exchange_rates_table, orders_in_eur, orders_in_eur_table, raw_exchange_rates

FAKE_PAYLOAD = {"base": "USD", "rates": {"EUR": 0.9, "GBP": 0.8}}

FAKE_ORDERS = pd.DataFrame(
    [{"order_id": 1, "customer_id": 5, "product_id": 1, "quantity": 2, "order_date": "2026-01-01"}]
)
FAKE_PRODUCTS = pd.DataFrame([{"product_id": 1, "name": "Widget", "category": "tools", "price": 10.0}])
FAKE_EXCHANGE_RATES = pd.DataFrame(
    [{"base_currency": "USD", "quote_currency": "EUR", "rate": 0.9}]
)


def test_exchange_rates_pipeline_loads_expected_rows():
    loaded = {}

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    with patch.object(
        source, "fetch_latest_rates", return_value=FAKE_PAYLOAD
    ), patch.object(db, "load_table", side_effect=fake_load_table):
        result = materialize([raw_exchange_rates, exchange_rates_table])

    assert result.success
    table = loaded["exchange_rates"]
    assert set(table["quote_currency"]) == {"EUR", "GBP"}
    assert table.loc[table["quote_currency"] == "EUR", "rate"].iloc[0] == 0.9


def test_orders_in_eur_pipeline_reads_warehouse_tables_and_writes_conversion():
    loaded = {}

    def fake_read_table(table_name: str) -> pd.DataFrame:
        return {
            "orders": FAKE_ORDERS,
            "products": FAKE_PRODUCTS,
            "exchange_rates": FAKE_EXCHANGE_RATES,
        }[table_name]

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    with patch.object(db, "read_table", side_effect=fake_read_table), patch.object(
        db, "load_table", side_effect=fake_load_table
    ):
        result = materialize([orders_in_eur, orders_in_eur_table])

    assert result.success
    table = loaded["orders_in_eur"]
    assert len(table) == 1
    # 2 * $10 = $20 -> 20 * 0.9 = 18.0 EUR
    assert table.iloc[0]["total_eur"] == 18.0
