import pandas as pd
import pytest

from main import build_orders_in_eur

ORDERS = pd.DataFrame(
    [
        {"order_id": 100, "customer_id": 5, "product_id": 1, "quantity": 2, "order_date": "2026-01-01"},
        {"order_id": 101, "customer_id": 6, "product_id": 2, "quantity": 1, "order_date": "2026-01-02"},
    ]
)
PRODUCTS = pd.DataFrame(
    [
        {"product_id": 1, "name": "Widget", "category": "tools", "price": 10.0},
        {"product_id": 2, "name": "Gadget", "category": "tools", "price": 5.0},
    ]
)
EXCHANGE_RATES = pd.DataFrame(
    [
        {"base_currency": "USD", "quote_currency": "EUR", "rate": 0.9},
        {"base_currency": "USD", "quote_currency": "GBP", "rate": 0.8},
    ]
)


def test_build_orders_in_eur_joins_and_converts_totals():
    result = build_orders_in_eur(ORDERS, PRODUCTS, EXCHANGE_RATES)

    assert set(result.columns) == {
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "total_usd",
        "total_eur",
    }
    row_100 = result.loc[result["order_id"] == 100].iloc[0]
    # 2 units * $10 = $20 USD -> 20 * 0.9 = 18.0 EUR
    assert row_100["total_usd"] == 20.0
    assert row_100["total_eur"] == pytest.approx(18.0)

    row_101 = result.loc[result["order_id"] == 101].iloc[0]
    # 1 unit * $5 = $5 USD -> 5 * 0.9 = 4.5 EUR
    assert row_101["total_usd"] == 5.0
    assert row_101["total_eur"] == pytest.approx(4.5)


def test_build_orders_in_eur_raises_when_no_eur_rate_available():
    rates_without_eur = EXCHANGE_RATES[EXCHANGE_RATES["quote_currency"] != "EUR"]

    with pytest.raises(ValueError, match="No USD -> EUR rate"):
        build_orders_in_eur(ORDERS, PRODUCTS, rates_without_eur)
