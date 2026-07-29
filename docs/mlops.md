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
