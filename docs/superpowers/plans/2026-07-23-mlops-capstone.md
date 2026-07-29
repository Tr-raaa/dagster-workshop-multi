# MLOps Reference Pipeline + Portfolio Capstone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a third, fully-built reference pipeline (`pipeline_ml`) that demonstrates Dagster for MLOps, and write a capstone assignment that lets students turn their own fork into a portfolio piece.

**Architecture:** `pipeline_ml` is a new container, wired the same way as `pipeline_products`/`pipeline_fx` (own `Dockerfile`, `requirements.txt`, `db.py`, `main.py`, `tests/`), registered in `workspace.yaml` and `docker-compose.yml` on port 4002. It has no external API — it reads `products`/`orders` out of `warehouse_postgresql`, trains a classifier, gates on accuracy via an `@asset_check`, and writes predictions back to the warehouse. Docs (`docs/mlops.md`, `docs/capstone.md`, `docs/portfolio-readme-template.md`) explain the pattern and the assignment.

**Tech Stack:** Python 3.11, Dagster 1.9.x, pandas 2.2.x, SQLAlchemy 2.0.x, scikit-learn 1.9.x, pytest 8.3.x, Docker Compose.

## Global Constraints

- New pipeline container name: `pipeline_ml`, gRPC port `4002`, `workspace.yaml` `location_name: "pipeline_ml"`.
- `pipeline_ml` is a **fully built reference** — no `# TODO(exercise-N)` markers, unlike `pipeline_products`/`pipeline_fx`.
- New dependency `scikit-learn~=1.9.0` goes **only** in `pipeline_ml/requirements.txt` — do not touch `pipeline_products/requirements.txt` or `pipeline_fx/requirements.txt`.
- ML task: classify order lines as high/low value. Label rule: `is_high_value = total > median(total)` where `total = quantity * price`, computed in `pipeline_ml/main.py`.
- Model quality gate threshold: accuracy `>= 0.6`, enforced by `@asset_check` named `model_quality_check` on the `trained_model` asset.
- Retrain schedule: **weekly**, cron `"0 6 * * 1"` (Monday 06:00) — deliberately different cadence from the daily `refresh_products_daily`/`refresh_fx_daily` schedules.
- Predictions table name in the warehouse: `order_value_predictions`, columns `order_id, predicted_label, probability, actual_label`.
- No new Dagster concepts beyond what `pipeline_products`/`pipeline_fx` already use: `@asset`, `@asset_check`, `Definitions`, `define_asset_job`, `ScheduleDefinition`.
- Capstone deliverable model: students fork the repo to their own public GitHub account; no PR-back-to-shared-repo flow.
- Capstone grading: lightweight self-check checklist only, not a points-weighted rubric, not automated/CI-graded.

---

### Task 1: `pipeline_ml/db.py` — warehouse read/write helpers

**Files:**
- Create: `pipeline_ml/db.py`
- Create: `pipeline_ml/tests/conftest.py`
- Create: `pipeline_ml/tests/test_db.py`

**Interfaces:**
- Produces: `db.get_engine() -> sqlalchemy.Engine`, `db.load_table(df: pd.DataFrame, table_name: str) -> int`, `db.read_table(table_name: str) -> pd.DataFrame` — later tasks import `db` and call `db.read_table(...)` / `db.load_table(...)`.

- [ ] **Step 1: Create the `pipeline_ml` directory and test scaffolding**

Create `pipeline_ml/tests/conftest.py`:

```python
import sys
from pathlib import Path

# Add parent directory to Python path so 'db' and 'main' can be imported
sys.path.insert(0, str(Path(__file__).parent.parent))
```

- [ ] **Step 2: Write the failing tests for `db.py`**

Create `pipeline_ml/tests/test_db.py`:

```python
from unittest.mock import MagicMock, patch

import pandas as pd

import db


def test_get_engine_builds_expected_connection_string(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_USER", "u")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "p")
    monkeypatch.setenv("WAREHOUSE_HOST", "h")
    monkeypatch.setenv("WAREHOUSE_PORT", "5432")
    monkeypatch.setenv("WAREHOUSE_DB", "d")

    with patch("db.create_engine") as mock_create_engine:
        db.get_engine()

    mock_create_engine.assert_called_once_with("postgresql+psycopg2://u:p@h:5432/d")


def test_load_table_writes_dataframe_and_returns_row_count(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_USER", "u")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "p")
    monkeypatch.setenv("WAREHOUSE_HOST", "h")
    monkeypatch.setenv("WAREHOUSE_PORT", "5432")
    monkeypatch.setenv("WAREHOUSE_DB", "d")

    df = pd.DataFrame({"a": [1, 2, 3]})
    fake_engine = MagicMock()

    with patch("db.create_engine", return_value=fake_engine):
        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            row_count = db.load_table(df, "my_table")

    mock_to_sql.assert_called_once_with(
        "my_table", fake_engine, if_exists="replace", index=False
    )
    assert row_count == 3


def test_read_table_reads_dataframe_from_table(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_USER", "u")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "p")
    monkeypatch.setenv("WAREHOUSE_HOST", "h")
    monkeypatch.setenv("WAREHOUSE_PORT", "5432")
    monkeypatch.setenv("WAREHOUSE_DB", "d")

    fake_engine = MagicMock()
    fake_df = pd.DataFrame({"a": [1, 2]})

    with patch("db.create_engine", return_value=fake_engine):
        with patch("db.pd.read_sql", return_value=fake_df) as mock_read_sql:
            result = db.read_table("my_table")

    mock_read_sql.assert_called_once_with("SELECT * FROM my_table", fake_engine)
    pd.testing.assert_frame_equal(result, fake_df)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd pipeline_ml && python -m pytest tests/test_db.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'db'` (file doesn't exist yet)

- [ ] **Step 4: Write `db.py`**

Create `pipeline_ml/db.py`:

```python
import os

import pandas as pd
from sqlalchemy import create_engine


def get_engine():
    user = os.environ["WAREHOUSE_USER"]
    password = os.environ["WAREHOUSE_PASSWORD"]
    host = os.environ["WAREHOUSE_HOST"]
    port = os.environ["WAREHOUSE_PORT"]
    db_name = os.environ["WAREHOUSE_DB"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")


def load_table(df: pd.DataFrame, table_name: str) -> int:
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    return len(df)


def read_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd pipeline_ml && python -m pytest tests/test_db.py -v`
Expected: `3 passed`

- [ ] **Step 6: Commit**

```bash
git add pipeline_ml/db.py pipeline_ml/tests/conftest.py pipeline_ml/tests/test_db.py
git commit -m "feat(pipeline_ml): add warehouse read/write helpers"
```

---

### Task 2: `order_features` asset — join orders+products, compute the label

**Files:**
- Create: `pipeline_ml/main.py`
- Create: `pipeline_ml/tests/test_features.py`

**Interfaces:**
- Consumes: `db.read_table(table_name: str) -> pd.DataFrame` (Task 1)
- Produces: `build_order_features(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame` returning columns `order_id, product_id, customer_id, quantity, price, category, total, is_high_value`; asset `order_features`. Later tasks depend on this exact column set.

- [ ] **Step 1: Write the failing test**

Create `pipeline_ml/tests/test_features.py`:

```python
import pandas as pd

from main import build_order_features

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pipeline_ml && python -m pytest tests/test_features.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'main'`

- [ ] **Step 3: Write `main.py` with `build_order_features` and the `order_features` asset**

Create `pipeline_ml/main.py`:

```python
import pandas as pd
from dagster import asset

import db


def build_order_features(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    merged = orders.merge(products, on="product_id", how="inner")
    merged["total"] = merged["quantity"] * merged["price"]
    median_total = merged["total"].median()
    merged["is_high_value"] = (merged["total"] > median_total).astype(int)
    return merged[
        [
            "order_id",
            "product_id",
            "customer_id",
            "quantity",
            "price",
            "category",
            "total",
            "is_high_value",
        ]
    ]


@asset
def order_features() -> pd.DataFrame:
    orders = db.read_table("orders")
    products = db.read_table("products")
    return build_order_features(orders, products)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd pipeline_ml && python -m pytest tests/test_features.py -v`
Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add pipeline_ml/main.py pipeline_ml/tests/test_features.py
git commit -m "feat(pipeline_ml): add order_features asset"
```

---

### Task 3: `trained_model` asset + `model_quality_check` asset check

**Files:**
- Modify: `pipeline_ml/main.py`
- Create: `pipeline_ml/tests/test_training.py`

**Interfaces:**
- Consumes: `order_features` asset output shape from Task 2 (`quantity, price, category, is_high_value` columns).
- Produces: constants `ACCURACY_THRESHOLD = 0.6`, `FEATURE_INPUT_COLUMNS = ["quantity", "price", "category"]`; function `train_classifier(features: pd.DataFrame) -> dict` returning `{"model": ..., "accuracy": float, "feature_columns": list[str]}`; asset `trained_model`; asset check `model_quality_check`. Task 4 consumes the `trained_model` dict shape and `FEATURE_INPUT_COLUMNS`.

- [ ] **Step 1: Write the failing tests**

Create `pipeline_ml/tests/test_training.py`:

```python
import pandas as pd

from main import ACCURACY_THRESHOLD, train_classifier

# Perfectly separable on quantity/price so the classifier's accuracy is
# deterministic regardless of the train/test split.
SEPARABLE_FEATURES = pd.DataFrame(
    {
        "order_id": range(1, 11),
        "product_id": [1] * 10,
        "customer_id": [1] * 10,
        "quantity": [1, 1, 1, 1, 1, 10, 10, 10, 10, 10],
        "price": [5.0, 5.0, 5.0, 5.0, 5.0, 50.0, 50.0, 50.0, 50.0, 50.0],
        "category": ["a"] * 10,
        "total": [5.0] * 5 + [500.0] * 5,
        "is_high_value": [0] * 5 + [1] * 5,
    }
)


def test_train_classifier_meets_accuracy_threshold_on_separable_data():
    bundle = train_classifier(SEPARABLE_FEATURES)

    assert bundle["accuracy"] >= ACCURACY_THRESHOLD
    assert bundle["feature_columns"] == ["quantity", "price", "category_a"]
    assert hasattr(bundle["model"], "predict")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pipeline_ml && python -m pytest tests/test_training.py -v`
Expected: FAIL — `ImportError: cannot import name 'train_classifier' from 'main'`

- [ ] **Step 3: Add `train_classifier`, the `trained_model` asset, and `model_quality_check` to `main.py`**

Modify `pipeline_ml/main.py` — add these imports at the top:

```python
from dagster import AssetCheckResult, asset_check
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
```

Add these constants after the imports:

```python
ACCURACY_THRESHOLD = 0.6
FEATURE_INPUT_COLUMNS = ["quantity", "price", "category"]
```

Add after `build_order_features` (before the `order_features` asset):

```python
def train_classifier(features: pd.DataFrame) -> dict:
    encoded = pd.get_dummies(features[FEATURE_INPUT_COLUMNS], columns=["category"])
    feature_columns = list(encoded.columns)
    labels = features["is_high_value"]

    x_train, x_test, y_train, y_test = train_test_split(
        encoded, labels, test_size=0.3, random_state=42
    )
    model = LogisticRegression(max_iter=1000)
    model.fit(x_train, y_train)
    accuracy = accuracy_score(y_test, model.predict(x_test))

    return {"model": model, "accuracy": accuracy, "feature_columns": feature_columns}
```

Add after the `order_features` asset:

```python
@asset
def trained_model(order_features: pd.DataFrame) -> dict:
    return train_classifier(order_features)


@asset_check(asset=trained_model)
def model_quality_check(trained_model: dict) -> AssetCheckResult:
    accuracy = trained_model["accuracy"]
    return AssetCheckResult(
        passed=accuracy >= ACCURACY_THRESHOLD, metadata={"accuracy": accuracy}
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd pipeline_ml && python -m pytest tests/test_training.py -v`
Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add pipeline_ml/main.py pipeline_ml/tests/test_training.py
git commit -m "feat(pipeline_ml): add trained_model asset with accuracy quality gate"
```

---

### Task 4: `order_value_predictions` asset + `Definitions` wiring

**Files:**
- Modify: `pipeline_ml/main.py`
- Create: `pipeline_ml/tests/test_main.py`

**Interfaces:**
- Consumes: `order_features` (Task 2), `trained_model`/`FEATURE_INPUT_COLUMNS` (Task 3), `db.load_table` (Task 1).
- Produces: `score_orders(features: pd.DataFrame, model_bundle: dict) -> pd.DataFrame` with columns `order_id, predicted_label, probability, actual_label`; asset `order_value_predictions`; `defs` (`Definitions`) exposing all three assets, `model_quality_check`, `refresh_ml_job`, `refresh_ml_weekly`.

- [ ] **Step 1: Write the failing end-to-end test**

Create `pipeline_ml/tests/test_main.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pipeline_ml && python -m pytest tests/test_main.py -v`
Expected: FAIL — `ImportError: cannot import name 'order_value_predictions' from 'main'`

- [ ] **Step 3: Add `score_orders`, the `order_value_predictions` asset, and `Definitions` wiring**

Modify `pipeline_ml/main.py` — add this import at the top alongside the others:

```python
from dagster import Definitions, ScheduleDefinition, define_asset_job
```

Add after `train_classifier`:

```python
def score_orders(features: pd.DataFrame, model_bundle: dict) -> pd.DataFrame:
    encoded = pd.get_dummies(features[FEATURE_INPUT_COLUMNS], columns=["category"])
    encoded = encoded.reindex(columns=model_bundle["feature_columns"], fill_value=0)
    model = model_bundle["model"]
    predicted = model.predict(encoded)
    probability = model.predict_proba(encoded)[:, 1]
    return pd.DataFrame(
        {
            "order_id": features["order_id"].values,
            "predicted_label": predicted,
            "probability": probability,
            "actual_label": features["is_high_value"].values,
        }
    )
```

Add at the end of `pipeline_ml/main.py`:

```python
@asset
def order_value_predictions(order_features: pd.DataFrame, trained_model: dict) -> int:
    predictions = score_orders(order_features, trained_model)
    return db.load_table(predictions, "order_value_predictions")


refresh_ml_job = define_asset_job(name="refresh_ml_job")

refresh_ml_weekly = ScheduleDefinition(
    name="refresh_ml_weekly",
    job=refresh_ml_job,
    cron_schedule="0 6 * * 1",
)

defs = Definitions(
    assets=[order_features, trained_model, order_value_predictions],
    asset_checks=[model_quality_check],
    jobs=[refresh_ml_job],
    schedules=[refresh_ml_weekly],
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd pipeline_ml && python -m pytest tests/test_main.py -v`
Expected: `1 passed`

- [ ] **Step 5: Run the full `pipeline_ml` test suite**

Run: `cd pipeline_ml && python -m pytest -v`
Expected: `6 passed` (2 from `test_db.py` and `test_read_table_reads_dataframe_from_table` = 3, 1 from `test_features.py`, 1 from `test_training.py`, 1 from `test_main.py`)

- [ ] **Step 6: Commit**

```bash
git add pipeline_ml/main.py pipeline_ml/tests/test_main.py
git commit -m "feat(pipeline_ml): add order_value_predictions asset and Definitions wiring"
```

---

### Task 5: Containerize `pipeline_ml`

**Files:**
- Create: `pipeline_ml/requirements.txt`
- Create: `pipeline_ml/Dockerfile`
- Create: `pipeline_ml/.dockerignore`

**Interfaces:**
- Consumes: `pipeline_ml/main.py` (Task 4) as the module the gRPC server loads via `-f main.py`.
- Produces: a buildable image exposing port `4002`, consumed by Task 6's `docker-compose.yml` entry.

- [ ] **Step 1: Create `pipeline_ml/requirements.txt`**

```
dagster~=1.9.0
dagster-postgres~=0.25.13
pandas~=2.2.0
SQLAlchemy~=2.0.0
psycopg2-binary~=2.9.0
scikit-learn~=1.9.0
pytest~=8.3.0
```

- [ ] **Step 2: Create `pipeline_ml/.dockerignore`**

```
.venv/
__pycache__/
.pytest_cache/
tests/
```

- [ ] **Step 3: Create `pipeline_ml/Dockerfile`**

```dockerfile
FROM python:3.11-slim

WORKDIR /opt/dagster/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 4002

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4002", "-f", "main.py"]
```

- [ ] **Step 4: Verify the image builds**

Run: `docker build -t pipeline_ml_verify ./pipeline_ml`
Expected: build completes with exit code 0 (no `ERROR` lines)

- [ ] **Step 5: Remove the verification image**

Run: `docker rmi pipeline_ml_verify`
Expected: image removed

- [ ] **Step 6: Commit**

```bash
git add pipeline_ml/requirements.txt pipeline_ml/Dockerfile pipeline_ml/.dockerignore
git commit -m "feat(pipeline_ml): containerize the ML pipeline"
```

---

### Task 6: Wire `pipeline_ml` into `workspace.yaml` and `docker-compose.yml`

**Files:**
- Modify: `workspace.yaml`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: `pipeline_ml` image from Task 5 (port 4002).
- Produces: a `pipeline_ml` code location the webserver/daemon can load, and a `pipeline_ml` compose service other tasks/docs reference.

- [ ] **Step 1: Add `pipeline_ml` to `workspace.yaml`**

Modify `workspace.yaml` — current content:

```yaml
load_from:
  - grpc_server:
      host: pipeline_products
      port: 4000
      location_name: "pipeline_products"
  - grpc_server:
      host: pipeline_fx
      port: 4001
      location_name: "pipeline_fx"
```

New content:

```yaml
load_from:
  - grpc_server:
      host: pipeline_products
      port: 4000
      location_name: "pipeline_products"
  - grpc_server:
      host: pipeline_fx
      port: 4001
      location_name: "pipeline_fx"
  - grpc_server:
      host: pipeline_ml
      port: 4002
      location_name: "pipeline_ml"
```

- [ ] **Step 2: Add the `pipeline_ml` service to `docker-compose.yml`**

Modify `docker-compose.yml` — insert a new service after the `pipeline_fx` service block (after its closing `depends_on: - warehouse_postgresql` and before the `dagster_webserver` service):

```yaml
  pipeline_ml:
    build: ./pipeline_ml
    container_name: pipeline_ml
    environment:
      WAREHOUSE_HOST: warehouse_postgresql
      WAREHOUSE_PORT: "5432"
      WAREHOUSE_USER: warehouse_user
      WAREHOUSE_PASSWORD: warehouse_password
      WAREHOUSE_DB: warehouse
      DAGSTER_POSTGRES_USER: postgres_user
      DAGSTER_POSTGRES_PASSWORD: postgres_password
      DAGSTER_POSTGRES_DB: postgres_db
    networks:
      - dagster_network
    depends_on:
      - warehouse_postgresql
```

Also add `pipeline_ml` to the `depends_on` lists of the `dagster_webserver` and `dagster_daemon` services, so each becomes:

```yaml
    depends_on:
      - dagster_postgresql
      - pipeline_products
      - pipeline_fx
      - pipeline_ml
```

- [ ] **Step 3: Validate the compose file**

Run: `docker compose config --quiet`
Expected: exits 0 with no output (invalid YAML or references print an error and exit non-zero)

- [ ] **Step 4: Manually verify the full stack (developer verification, not automated)**

Run: `docker compose up --build`, open `http://localhost:3000`, confirm `pipeline_ml` appears under Deployment > Code Locations alongside `pipeline_products` and `pipeline_fx`, then run `docker compose down`.

- [ ] **Step 5: Commit**

```bash
git add workspace.yaml docker-compose.yml
git commit -m "feat: register pipeline_ml as a third code location"
```

---

### Task 7: `docs/mlops.md` — the MLOps pattern explainer

**Files:**
- Create: `docs/mlops.md`

**Interfaces:**
- Consumes: nothing (documentation only, describes `pipeline_ml` from Tasks 1-6).
- Produces: a doc linked from `docs/capstone.md` (Task 8) and `README.md` (Task 9).

- [ ] **Step 1: Write `docs/mlops.md`**

```markdown
# Dagster for MLOps

`pipeline_ml` is a third reference pipeline showing how the same Dagster
primitives used for ingestion in `pipeline_products` and `pipeline_fx` — assets,
asset checks, jobs, schedules — apply just as well to a training/prediction
workflow.

## The pattern

```
order_features  ---->  trained_model  ---->  order_value_predictions
  (join + label)      (train + evaluate)      (score + write to warehouse)
                            |
                            v
                    model_quality_check
                 (@asset_check, blocks a
                  bad model from shipping)
```

- **`order_features`** reads `products`/`orders` from the shared warehouse
  (the same cross-container read pattern from exercise ②) and computes a
  `is_high_value` label.
- **`trained_model`** fits a classifier and reports its accuracy.
- **`model_quality_check`** is an `@asset_check` on `trained_model` — if
  accuracy drops below a threshold, the check fails and the run is flagged,
  the same way exercise ③'s check blocks bad *data* from propagating. Here
  it blocks a bad *model*.
- **`order_value_predictions`** scores the features with the trained model
  and writes predictions back to the warehouse, queryable like any other
  table.

## Why this fits Dagster

- **Lineage**: the asset graph shows exactly which data trained which
  model, and which model produced which predictions — no separate
  experiment-tracking tool needed to answer "what produced this row?"
- **Quality gates as first-class citizens**: `@asset_check` is the same
  mechanism for "is this data good?" and "is this model good enough to
  ship?" — one system, one UI, one alerting path.
- **Independent scheduling**: `refresh_ml_weekly` retrains on a different
  cadence (weekly) than the daily ingestion schedules — because a model
  usually doesn't need retraining as often as raw data needs refreshing.

## What this example deliberately leaves out

No model registry, no experiment tracking, no real-time serving — the
quality-gate asset check is the full extent of the "Ops" here, intentionally,
to keep the pattern legible for a first pass. See `docs/capstone.md` Track C
if you want to build your own version of this with a different dataset or
prediction task.
```

- [ ] **Step 2: Commit**

```bash
git add docs/mlops.md
git commit -m "docs: explain the pipeline_ml MLOps pattern"
```

---

### Task 8: `docs/capstone.md` + `docs/portfolio-readme-template.md`

**Files:**
- Create: `docs/capstone.md`
- Create: `docs/portfolio-readme-template.md`

**Interfaces:**
- Consumes: `docs/mlops.md` (Task 7, linked from Track C), `docs/exercises.md` (existing, linked as prerequisite).
- Produces: the assignment doc and template, linked from `README.md` (Task 9).

- [ ] **Step 1: Write `docs/portfolio-readme-template.md`**

```markdown
# Portfolio README template

Copy this into your fork's root `README.md` after finishing your capstone
track, and fill in every `<...>` placeholder. This is the part a recruiter
or hiring manager actually reads.

---

# <your-pipeline-name>

<One or two sentences: what does this pipeline do, and why did you pick
this data source or prediction task?>

Built on top of [dagster-workshop-multi](https://github.com/<original-org>/dagster-workshop-multi),
a multi-container Dagster workshop — see that repo's README for the base
architecture (`pipeline_products`, `pipeline_fx`, `pipeline_ml`).

## What I built

- **Track:** <A: new source pipeline / B: cross-pipeline analytics / C: MLOps pipeline>
- **Data source:** <API/dataset name and link>
- **Key assets:** <list your asset names and one line each on what they do>
- **Quality gate:** <what your `@asset_check` verifies, and why you chose that threshold>

## Architecture

<Redraw the ASCII/diagram from the root README, extended with your new
pipeline, the same way `pipeline_ml` extended the two-pipeline diagram.>

## Running it

```bash
docker compose up --build
```

Open http://localhost:3000, find `<your_pipeline_name>` under Deployment >
Code Locations, and materialize its assets.

## Demo

<A screenshot or short GIF of the Dagster UI with your pipeline's assets
materialized — the asset graph view or the run log both work well.>

## What I'd do differently in production

<2-4 sentences: what's simplified here for the workshop (truncate-and-load,
no retries, no secrets manager, etc.) and what you'd change with real
infrastructure and stakes behind it.>
```

- [ ] **Step 2: Write `docs/capstone.md`**

```markdown
# Capstone: build your own pipeline

You've finished the three exercises in [exercises.md](exercises.md) and
studied `pipeline_ml`, the MLOps reference pipeline (see
[mlops.md](mlops.md)). The capstone is a bigger, open-ended pipeline you
design and build yourself — and it doubles as a portfolio piece, since you
do it in your own public fork.

## 1. Fork the repo

Fork `dagster-workshop-multi` to your own public GitHub account. Do all
capstone work there — commit as you go. Your fork *is* the deliverable;
there's no PR back to a shared repo.

## 2. Pick a track

Each track produces one new pipeline container in your fork, wired in the
same way `pipeline_products`, `pipeline_fx`, and `pipeline_ml` are.

### Track A — New source pipeline

Pick a free public API or dataset and build `pipeline_<name>/` from
scratch: `Dockerfile`, `requirements.txt`, `source.py`, `db.py`, `main.py`,
`tests/`. Mirror `pipeline_products`/`pipeline_fx` — at least one raw
ingestion asset, at least one table-load asset, at least one `@asset_check`.

### Track B — Cross-pipeline analytics

Build a downstream pipeline/asset that combines data across the *existing*
pipelines (`pipeline_products`, `pipeline_fx`, `pipeline_ml`) into a new
reporting table — a deeper version of exercise ②'s cross-container read.
Example: a daily summary table joining predicted high-value orders with
their EUR-converted totals.

### Track C — MLOps pipeline

Build your own `pipeline_ml`-style pipeline with a different prediction
task on a dataset of your choosing (it doesn't have to be the workshop's
`products`/`orders` data). Follow the pattern in [mlops.md](mlops.md):
a feature-engineering asset, a training asset, a `@asset_check` quality
gate, and a predictions asset.

## 3. Self-check before you call it done

- [ ] Your new pipeline builds and appears as its own code location under
      Deployment > Code Locations
- [ ] "Materialize all" runs it end-to-end with no errors
- [ ] It defines at least one `@asset_check` that passes
- [ ] `pytest` passes for your new pipeline's `tests/`
- [ ] It's wired into `docker-compose.yml` and `workspace.yaml`
- [ ] Your fork's README documents it — copy in
      [portfolio-readme-template.md](portfolio-readme-template.md) and fill
      it in

## 4. Make it a portfolio piece

Once the checklist passes, fill in
[portfolio-readme-template.md](portfolio-readme-template.md) in your fork's
root README: what you built, why, your architecture diagram, how to run it,
a screenshot or GIF of it running, and a short reflection on what you'd do
differently with real production infrastructure behind it. That's the part
that makes this more than an assignment — it's evidence of you designing
and shipping a working data pipeline end to end.
```

- [ ] **Step 3: Commit**

```bash
git add docs/capstone.md docs/portfolio-readme-template.md
git commit -m "docs: add capstone assignment and portfolio README template"
```

---

### Task 9: Update `README.md` and `docs/exercises.md` to reference `pipeline_ml` and the capstone

**Files:**
- Modify: `README.md`
- Modify: `docs/exercises.md`

**Interfaces:**
- Consumes: `pipeline_ml` (Tasks 1-6), `docs/mlops.md` and `docs/capstone.md` (Tasks 7-8).
- Produces: none (terminal documentation task).

- [ ] **Step 1: Update the Prerequisites and Quickstart sections in `README.md`**

Change:

```markdown
- Docker Desktop (or Docker Engine + Docker Compose)
- Internet access (both pipelines call free public APIs)
```

To:

```markdown
- Docker Desktop (or Docker Engine + Docker Compose)
- Internet access (`pipeline_products` and `pipeline_fx` call free public APIs)
```

Change:

```markdown
Then open http://localhost:3000. Under Deployment > Code Locations you should
see `pipeline_products` and `pipeline_fx`, each its own container. Select all
assets and click "Materialize all" to run both pipelines end to end.
```

To:

```markdown
Then open http://localhost:3000. Under Deployment > Code Locations you should
see `pipeline_products`, `pipeline_fx`, and `pipeline_ml`, each its own
container. Select all assets and click "Materialize all" to run all three
pipelines end to end — `pipeline_ml` trains on the data the other two just
loaded, so it needs to run after them at least once.
```

- [ ] **Step 2: Update the architecture diagram in `README.md`**

Change:

```markdown
```
                     dagster_webserver (:3000)  <-- workspace.yaml -->  dagster_daemon
                              |                                              |
                              +---------------------+-----------------------+
                                                     |
                             dagster_postgresql  (Dagster's own run/schedule/event storage)

  pipeline_products (:4000)                    pipeline_fx (:4001)
  fakestoreapi.com -> raw_products/raw_orders   api.frankfurter.app -> raw_exchange_rates
        |                                              |
        v                                              v
  products, orders tables  ------------------->  warehouse_postgresql  <-------  exchange_rates table
```
```

To:

```markdown
```
                     dagster_webserver (:3000)  <-- workspace.yaml -->  dagster_daemon
                              |                                              |
                              +---------------------+-----------------------+
                                                     |
                             dagster_postgresql  (Dagster's own run/schedule/event storage)

  pipeline_products (:4000)          pipeline_fx (:4001)          pipeline_ml (:4002)
  fakestoreapi.com ->                api.frankfurter.app ->       trains a classifier on
  raw_products/raw_orders            raw_exchange_rates           products+orders, writes
        |                                  |                      predictions back
        v                                  v                            |
  products, orders  ------------->  warehouse_postgresql  <-------------+
  tables                            (also: exchange_rates,
                                      order_value_predictions)
```
```

- [ ] **Step 3: Add a mention of `pipeline_ml` to the "Each pipeline is..." paragraph**

Change:

```markdown
Each pipeline is a fully independent container: its own `Dockerfile`, its own
`requirements.txt`, its own source/db modules. They only share the
`warehouse_postgresql` database as a landing zone — exactly like production's
21 pipeline containers, each pulling from its own source system into one
destination database.
```

To:

```markdown
Each pipeline is a fully independent container: its own `Dockerfile`, its own
`requirements.txt`, its own source/db modules. They only share the
`warehouse_postgresql` database as a landing zone — exactly like production's
21 pipeline containers, each pulling from its own source system into one
destination database. `pipeline_ml` is the odd one out: instead of pulling
from an external API, it reads `pipeline_products`' tables straight out of
the warehouse, trains a classifier, and writes predictions back — see
[docs/mlops.md](docs/mlops.md) for why Dagster's asset/asset-check model
fits that pattern too.
```

- [ ] **Step 4: Add a Capstone section to `README.md`**

Change:

```markdown
## Exercises

See [docs/exercises.md](docs/exercises.md) for three hands-on TODOs, in
increasing difficulty. Each one has a `# TODO(exercise-N)` comment marking
where to add your code.

## How this maps to the production pipeline
```

To:

```markdown
## Exercises

See [docs/exercises.md](docs/exercises.md) for three hands-on TODOs, in
increasing difficulty. Each one has a `# TODO(exercise-N)` comment marking
where to add your code.

## Capstone

Once you've finished the three exercises, see
[docs/capstone.md](docs/capstone.md) for a bigger, open-ended assignment:
build and wire in your own pipeline, in your own fork, and turn it into a
portfolio piece.

## How this maps to the production pipeline
```

- [ ] **Step 5: Add a pointer to the capstone at the end of `docs/exercises.md`**

Change the end of `docs/exercises.md`:

```markdown
## ③ Add a data-quality asset check

**File:** `pipeline_products/main.py`

Add an `@asset_check` on `raw_orders` that fails if any row has
`quantity <= 0`. Register it via the `asset_checks=[...]` argument to
`Definitions` in `pipeline_products/main.py`.

Hint: see the [Dagster asset checks docs](https://docs.dagster.io/concepts/assets/asset-checks)
for the `@asset_check` decorator signature.
```

To:

```markdown
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
```

- [ ] **Step 6: Verify the docs render sensibly**

Run: `python -c "import pathlib; [print(p, len(pathlib.Path(p).read_text())) for p in ['README.md', 'docs/exercises.md', 'docs/mlops.md', 'docs/capstone.md', 'docs/portfolio-readme-template.md']]"`
Expected: prints all five paths with nonzero character counts (confirms no file was left empty by the edits)

- [ ] **Step 7: Commit**

```bash
git add README.md docs/exercises.md
git commit -m "docs: reference pipeline_ml and the capstone assignment from README and exercises"
```
