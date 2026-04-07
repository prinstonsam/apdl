# Appodeal Data Engineering Challenge

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

```bash
uv sync
```

## Run

```bash
uv run python -m src.solution config.yaml
```

All parameters (input files, output directory, Spark settings) are defined in `config.yaml`.

Results will be written to the `output_data/` directory:

- `metrics.json` — impressions, clicks, revenue by app_id and country_code
- `recommendations.json` — top 5 advertiser_ids per app and country
- `median_spend.json` — median user spend per country

## Tests

```bash
.venv/bin/pytest tests/ -v
```

## Lint

```bash
.venv/bin/ruff check src/
```

---

## How This Was Built

### 1. Technology Choice

**PySpark** on `local[*]` — utilizes all 8 cores via Spark's built-in parallelism.
Pydantic v2 + YAML for configuration, `uv` for dependency management, `ruff` for linting.

### 2. Architecture

Functional style with strict I/O separation from business logic:

```
config.py     — Pydantic models + YAML loader
spark.py      — SparkSession factory
io.py         — read/write JSON
cleaning.py   — data quality fixes
transforms.py — business logic (metrics, top advertisers, median spend)
solution.py   — orchestration
```

### 3. Data Quality Analysis

Exploratory analysis revealed the following issues:

| Issue | Count | Decision |
|---|---|---|
| Duplicate impression IDs | 15 | Deduplicate by `id`, keep first occurrence |
| Orphan clicks (no matching impression) | 12 | Dropped by LEFT JOIN from impressions |
| Null `country_code` | 5 | Keep as-is, appear in output as `null` |
| Invalid `country_code` (`??`, `XX`, `ZZZ`) | 11 | Keep as-is — no standard enforced per spec |
| Null `user_id` | 4 | Filter out before median spend calculation |
| Null `revenue` | 1 | Fill with 0 |
| Negative `revenue` | 7 | Keep as-is — likely refunds/corrections |
| Multiple clicks per impression | 2 | Keep all — each click generates revenue |

### 4. Data Skew Mitigation

Observed skews:
- **User skew**: 1 user = 16% of all impressions (167 out of 1038)
- **app_id skew**: `app_id=1` = 44% of impressions

Mitigations applied:
- **Broadcast join**: clicks DataFrame (705 rows) is small enough to broadcast, avoiding shuffle skew on the impressions side
- **Repartition before heavy aggregations**: redistribute data evenly across partitions by grouping keys to prevent hot partitions during `groupBy`

### 5. Caching Strategy

`join_impressions_clicks` is reused by all three transforms. Without caching, Spark would recompute the same broadcast join three times.

Solution: call `join_impressions_clicks` once in `solution.py`, `.cache()` the result, pass it to each transform, and `.unpersist()` before shutdown.
