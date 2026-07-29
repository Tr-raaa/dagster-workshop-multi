# Design: MLOps reference pipeline + portfolio capstone assignment

## Context

`dagster-workshop-multi` currently has two guided pipelines (`pipeline_products`,
`pipeline_fx`) and three small TODO-driven exercises in `docs/exercises.md`.
The audience is students who have never used Dagster or Docker before.

Goal: give students a capstone assignment, after the three exercises, that
(a) is substantial enough to serve as a portfolio piece on their own public
GitHub, and (b) introduces Dagster for MLOps, since the workshop currently
only covers ingestion/ETL-style pipelines.

## 1. Architecture: new reference pipeline `pipeline_ml`

A third container, `pipeline_ml/`, is added to the repo as a **fully built
reference implementation** (no TODOs — unlike `pipeline_products`/
`pipeline_fx`, students study this one rather than complete it). It follows
the exact same per-container pattern already established: own `Dockerfile`,
`requirements.txt`, `db.py`, `main.py`, `tests/`.

Unlike the other two pipelines, `pipeline_ml` has no external API source —
it reads `products` and `orders` directly out of `warehouse_postgresql`
(the same cross-container read pattern exercise ② teaches), trains a small
classifier, and writes predictions back to the warehouse.

```
pipeline_products (:4000)      pipeline_fx (:4001)      pipeline_ml (:4002)
fakestoreapi -> products,           frankfurter.app             reads products+orders
orders tables                       -> exchange_rates           from warehouse
        \_______________________________|______________________/
                                         v
                              warehouse_postgresql
                     (products, orders, exchange_rates,
                      order_value_predictions)
```

Root `README.md`'s architecture diagram and "What just happened" section
are updated to show all three pipelines.

## 2. `pipeline_ml` components

- **`order_features`** (asset): reads `products` + `orders` via
  `db.get_engine()` + `pd.read_sql`, joins on `product_id`, computes
  `total = quantity * price`, and labels each order line
  `is_high_value = total > median(total)`.
- **`trained_model`** (asset): splits train/test, fits a `scikit-learn`
  classifier (`LogisticRegression` or `DecisionTreeClassifier`), returns a
  bundle of `{model, accuracy, feature_columns}`. Uses Dagster's default
  (pickle) IO manager — no new infra needed.
- **`model_quality_check`** (`@asset_check` on `trained_model`): fails if
  accuracy is below a lenient threshold (e.g. 0.6). This is the MLOps
  quality gate — a bad model blocks downstream materialization, the same
  way exercise ③'s check blocks bad data.
- **`order_value_predictions`** (asset): scores `order_features` with
  `trained_model`, writes `(order_id, predicted_label, probability,
  actual_label)` to the warehouse via `db.load_table`.
- `refresh_ml_job` + a **weekly** `ScheduleDefinition` (retrain cadence
  intentionally differs from the daily raw-ingestion schedules — called out
  in docs as a real MLOps consideration).

New dependency: `scikit-learn` added only to `pipeline_ml/requirements.txt`
(containers remain independent, per existing convention).

Docker/workspace wiring: new `pipeline_ml` service in `docker-compose.yml`
(port 4002, same `WAREHOUSE_*`/`DAGSTER_POSTGRES_*` env vars as the other
two pipelines, `depends_on: warehouse_postgresql`), new `grpc_server` entry
in `workspace.yaml` (`location_name: "pipeline_ml"`).

Operational note (documented, not solved): like exercise ②'s
`orders_in_eur`, `pipeline_ml` assumes `products`/`orders` have already been
materialized by `pipeline_products` at least once before it can run
meaningfully. No automatic dependency/ordering is added between containers
— this mirrors the existing exercise ②'s documented assumption.

## 3. `docs/mlops.md`

A short conceptual doc explaining the pattern used in `pipeline_ml`:
feature-engineering asset → train asset → quality-gate check → predictions
asset. Explains why Dagster's asset/asset-check model fits MLOps: lineage
across train→predict, quality gates as first-class citizens (not an
afterthought), independent retrain scheduling from ingestion. Ties back to
the root README's "how this maps to production" framing.

## 4. Capstone assignment: `docs/capstone.md`

Positioned **after** the three exercises in `docs/exercises.md`. Student
picks **one of three tracks**, each producing a new pipeline container of
their own, added to their fork:

- **Track A — New source pipeline**: pick a free public API/dataset, build
  `pipeline_<name>/` from scratch, mirroring `pipeline_products`/
  `pipeline_fx`.
- **Track B — Cross-pipeline analytics**: a downstream pipeline/asset
  combining data across all existing pipelines into a reporting table (a
  deeper version of exercise ②'s cross-container read pattern).
- **Track C — MLOps pipeline**: build their own `pipeline_ml`-style
  pipeline with their own prediction task, modeled on the reference
  implementation from section 2.

All three tracks are graded against the same **lightweight self-check
checklist** (not a points-weighted rubric):

- [ ] Pipeline container builds and appears as its own code location under
      Deployment > Code Locations
- [ ] "Materialize all" runs the new pipeline end-to-end with no errors
- [ ] At least one `@asset_check` is defined and passes
- [ ] `pytest` passes for the new pipeline's `tests/`
- [ ] New service is wired into `docker-compose.yml` and `workspace.yaml`
- [ ] Fork's README documents the new pipeline (see section 5)

## 5. Portfolio packaging

`docs/capstone.md` includes:

- **Fork instructions**: fork `dagster-workshop-multi` to the student's own
  public GitHub account. The fork itself is the deliverable — own commit
  history, own README, a shareable link with their name on it. No PR back
  to a shared/classroom repo.
- **`docs/portfolio-readme-template.md`**: a fill-in-the-blank template
  students copy into their fork's root README, covering:
  - What they built and why (their chosen track + dataset/task)
  - Updated architecture diagram (placeholder + instructions to redraw it)
  - Setup/run instructions (`docker compose up --build`, what to click)
  - A screenshot or GIF of the Dagster UI showing their pipeline
    materialized
  - A short "what I'd do differently in production" reflection — the part
    that reads well to a recruiter skimming GitHub

## 6. Testing

`pipeline_ml/tests/` mirrors the existing test structure in
`pipeline_products/tests/` and `pipeline_fx/tests/` (`test_db.py`,
`test_main.py`): feature-engineering and labeling logic (the join, the
`total` computation, the `is_high_value` label) is extracted into pure
functions and tested against small synthetic DataFrames — no real
database or network access required, consistent with how the existing two
pipelines' tests are written.

## Out of scope

- No new Dagster concepts beyond what the 3 existing exercises already
  cover (assets, asset checks, jobs, schedules) — `pipeline_ml` uses the
  same primitives, just applied to a training/prediction workflow instead
  of ingestion.
- No model registry, experiment tracking, or real-time serving — the
  quality-gate asset check is the full extent of the "Ops" in this MLOps
  example, intentionally, for a beginner audience.
- No automated grading/CI for the capstone; the checklist in section 4 is
  self-check only.
