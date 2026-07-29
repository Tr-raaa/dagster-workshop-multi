from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
import source
from main import orders_table, products_table, raw_orders, raw_products

FAKE_PRODUCTS = [
    {"id": 1, "title": "Widget", "category": "tools", "price": 9.99},
    {"id": 2, "title": "Gadget", "category": "tools", "price": 4.99},
]

FAKE_CARTS = [
    {
        "id": 100,
        "userId": 5,
        "date": "2026-01-01",
        "products": [{"productId": 1, "quantity": 2}],
    }
]


def test_products_and_orders_pipeline_loads_expected_rows():
    loaded = {}

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    with patch.object(source, "fetch_products", return_value=FAKE_PRODUCTS), patch.object(
        source, "fetch_carts", return_value=FAKE_CARTS
    ), patch.object(db, "load_table", side_effect=fake_load_table):
        result = materialize([raw_products, raw_orders, products_table, orders_table])

    assert result.success
    assert loaded["products"].shape[0] == 2
    assert loaded["orders"].loc[0, "customer_id"] == 5
    assert loaded["orders"].loc[0, "quantity"] == 2
