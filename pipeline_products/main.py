import pandas as pd
from dagster import Definitions, ScheduleDefinition, asset, define_asset_job

import db
import source


@asset
def raw_products() -> pd.DataFrame:
    products = source.fetch_products()
    return pd.DataFrame(products)[["id", "title", "category", "price"]].rename(
        columns={"id": "product_id", "title": "name"}
    )


@asset
def raw_orders() -> pd.DataFrame:
    carts = source.fetch_carts()
    rows = []
    for cart in carts:
        for item in cart["products"]:
            rows.append(
                {
                    "order_id": cart["id"],
                    "customer_id": cart["userId"],
                    "product_id": item["productId"],
                    "quantity": item["quantity"],
                    "order_date": cart["date"],
                }
            )
    return pd.DataFrame(rows)


@asset
def products_table(raw_products: pd.DataFrame) -> int:
    return db.load_table(raw_products, "products")


@asset
def orders_table(raw_orders: pd.DataFrame) -> int:
    return db.load_table(raw_orders, "orders")


# TODO(exercise-1): add a `top_selling_products` asset downstream of
# raw_orders and raw_products (join on product_id, sum quantity) — see
# docs/exercises.md

# TODO(exercise-3): add an @asset_check on raw_orders that fails if any row
# has quantity <= 0 — see docs/exercises.md

refresh_products_job = define_asset_job(name="refresh_products_job")

refresh_products_daily = ScheduleDefinition(
    name="refresh_products_daily",
    job=refresh_products_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[raw_products, raw_orders, products_table, orders_table],
    jobs=[refresh_products_job],
    schedules=[refresh_products_daily],
)
