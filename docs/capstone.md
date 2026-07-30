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

- [√] Your new pipeline builds and appears as its own code location under
      Deployment > Code Locations
- [√] "Materialize all" runs it end-to-end with no errors
- [√] It defines at least one `@asset_check` that passes
- [√] `pytest` passes for your new pipeline's `tests/`
- [√] It's wired into `docker-compose.yml` and `workspace.yaml`
- [√] Your fork's README documents it — copy in
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
