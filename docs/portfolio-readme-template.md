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
