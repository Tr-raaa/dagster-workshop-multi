import pandas as pd

from main import build_order_features, feature_metadata

ORDERS = pd.DataFrame(
    [
        {"order_id": 100, "customer_id": 5, "product_id": 1, "quantity": 2, "order_date": "2026-01-01"},
        {"order_id": 101, "customer_id": 6, "product_id": 2, "quantity": 1, "order_date": "2026-01-02"},
    ]
)
PRODUCTS = pd.DataFrame(
    [
        {"product_id": 1, "name": "Widget", "category": "tools", "price": 9.99},
        {"product_id": 2, "name": "Gadget", "category": "tools", "price": 4.99},
    ]
)


def test_build_order_features_joins_and_computes_total_and_label():
    result = build_order_features(ORDERS, PRODUCTS)

    assert set(result.columns) == {
        "order_id",
        "product_id",
        "customer_id",
        "quantity",
        "price",
        "category",
        "total",
        "is_high_value",
    }
    row_100 = result.loc[result["order_id"] == 100].iloc[0]
    assert row_100["total"] == 19.98
    # median(total) of [19.98, 4.99] is above 4.99, so only the pricier
    # order (100) is above the median and labeled high value.
    assert row_100["is_high_value"] == 1
    row_101 = result.loc[result["order_id"] == 101].iloc[0]
    assert row_101["is_high_value"] == 0


def test_feature_metadata_reports_row_count_columns_and_high_value_rate():
    features = build_order_features(ORDERS, PRODUCTS)

    meta = feature_metadata(features)

    assert meta["num_rows"] == 2
    assert set(meta["columns"]) == set(features.columns)
    # Exactly 1 of the 2 rows (order 100) is high value -> rate of 0.5
    assert meta["high_value_rate"] == 0.5


def test_feature_metadata_handles_empty_dataframe_without_error():
    empty = build_order_features(ORDERS.iloc[0:0], PRODUCTS)

    meta = feature_metadata(empty)

    assert meta["num_rows"] == 0
    assert meta["high_value_rate"] == 0.0
