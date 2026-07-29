import pandas as pd

# Two orders on 2026-01-01, one order on 2026-01-02 — mirrors the shape of
# the `orders_in_eur` table written by pipeline_fx (see exercise 2).
ORDERS_IN_EUR = pd.DataFrame(
    [
        {
            "order_id": 1,
            "customer_id": 1,
            "product_id": 1,
            "order_date": "2026-01-01",
            "quantity": 2,
            "total_usd": 20.0,
            "total_eur": 18.0,
        },
        {
            "order_id": 2,
            "customer_id": 2,
            "product_id": 2,
            "order_date": "2026-01-01",
            "quantity": 1,
            "total_usd": 5.0,
            "total_eur": 4.5,
        },
        {
            "order_id": 3,
            "customer_id": 3,
            "product_id": 1,
            "order_date": "2026-01-02",
            "quantity": 1,
            "total_usd": 10.0,
            "total_eur": 9.0,
        },
    ]
)

# Mirrors the shape of the `order_value_predictions` table written by
# pipeline_ml (order 1 and 3 predicted high-value, order 2 is not).
PREDICTIONS = pd.DataFrame(
    [
        {"order_id": 1, "predicted_label": 1, "probability": 0.9, "actual_label": 1},
        {"order_id": 2, "predicted_label": 0, "probability": 0.2, "actual_label": 0},
        {"order_id": 3, "predicted_label": 1, "probability": 0.8, "actual_label": 1},
    ]
)
