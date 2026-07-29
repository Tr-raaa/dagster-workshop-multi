from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
from main import order_features, order_value_predictions, trained_model, model_quality_check

FAKE_ORDERS = pd.DataFrame(
    [
        {"order_id": i, "customer_id": 1, "product_id": 1, "quantity": 1, "order_date": "2026-01-01"}
        for i in range(1, 6)
    ]
    + [
        {"order_id": i, "customer_id": 2, "product_id": 2, "quantity": 10, "order_date": "2026-01-02"}
        for i in range(6, 11)
    ]
)
FAKE_PRODUCTS = pd.DataFrame(
    [
        {"product_id": 1, "name": "Cheap Widget", "category": "tools", "price": 5.0},
        {"product_id": 2, "name": "Pricey Gadget", "category": "electronics", "price": 50.0},
    ]
)


def test_ml_pipeline_produces_predictions_and_passes_quality_check():
    loaded = {}

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    def fake_read_table(table_name: str) -> pd.DataFrame:
        return {"orders": FAKE_ORDERS, "products": FAKE_PRODUCTS}[table_name]

    with patch.object(db, "read_table", side_effect=fake_read_table), patch.object(
        db, "load_table", side_effect=fake_load_table
    ):
        result = materialize(
            [order_features, trained_model, order_value_predictions, model_quality_check]
        )

    assert result.success

    predictions = loaded["order_value_predictions"]
    assert len(predictions) == 10
    assert set(predictions.columns) == {
        "order_id",
        "predicted_label",
        "probability",
        "actual_label",
    }

    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 1
    assert evaluations[0].passed is True
