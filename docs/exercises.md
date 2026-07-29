# Exercises

Each exercise has a `# TODO(exercise-N)` comment in the source marking where
to add your code. Work through them in order.

## ① Add a `top_selling_products` asset

**File:** `pipeline_products/main.py`

Add a new `@asset` named `top_selling_products` that depends on both
`raw_orders` and `raw_products`, joins them on `product_id`, sums `quantity`
per product, and returns the top 5 by total quantity sold. Add it to the
`assets=[...]` list in `defs`.

## ② Add a cross-pipeline `orders_in_eur` asset

**File:** `pipeline_fx/main.py`

Add a new `@asset` named `orders_in_eur` in `pipeline_fx` that reads the
`orders` and `products` tables (written by `pipeline_products`) and the
`exchange_rates` table (written by `pipeline_fx` itself) directly from the
shared warehouse Postgres using `db.get_engine()` + `pd.read_sql`, joins
them, and computes each order's total value converted to EUR. This is the payoff of having two
independent pipelines land in one database — no direct dependency between the
two containers is needed, only the shared destination.

## ③ Add a data-quality asset check

**File:** `pipeline_products/main.py`

Add an `@asset_check` on `raw_orders` that fails if any row has
`quantity <= 0`. Register it via the `asset_checks=[...]` argument to
`Definitions` in `pipeline_products/main.py`.

Hint: see the [Dagster asset checks docs](https://docs.dagster.io/concepts/assets/asset-checks)
for the `@asset_check` decorator signature.

## Next: the capstone

Once these three are done, take a look at `pipeline_ml` — a third pipeline
in this repo showing how the same asset/asset-check pattern applies to
training and evaluating a model (see [mlops.md](mlops.md)). Then head to
[capstone.md](capstone.md) for a bigger assignment built on everything
above.
