# GH Archive Lakehouse

*A production-shaped data pipeline that ingests [GH Archive](https://www.gharchive.org/)'s
hourly GitHub event stream into a tiered-retention lakehouse — bronze/silver/gold on
DuckDB + dbt, orchestrated by GitHub Actions, served through a Streamlit dashboard.*

**Live dashboard:** _not yet deployed (Phase 9)_
**Status:** Phase 0 — Foundations

---

## Architecture

```
                    ┌──────────────────────────────────────┐
                    │   GH Archive (data.gharchive.org)    │
                    │   hourly .json.gz, public, no auth   │
                    └──────────────────┬───────────────────┘
                                       │
                          ┌────────────▼────────────┐
                          │  INGEST (Python)        │
                          │  • download hour file   │
                          │  • filter event types   │
                          │  • flatten + project    │
                          │  • write Parquet        │
                          └────────────┬────────────┘
                                       │
    ╔══════════════════════════════════▼══════════════════════════════════╗
    ║                  OBJECT STORE (Cloudflare R2)                       ║
    ║                                                                     ║
    ║  BRONZE  raw/events/event_date=YYYY-MM-DD/hour=HH/part.parquet      ║
    ║          flattened, typed, unaggregated     [7-day retention]       ║
    ║                              │                                      ║
    ║                       dbt (DuckDB engine)                           ║
    ║                              │                                      ║
    ║  SILVER  stg_events, stg_repos, stg_actors                          ║
    ║          deduplicated, cleaned, conformed   [7-day retention]       ║
    ║                              │                                      ║
    ║  GOLD    fact_events, dim_repo (SCD2), dim_actor, dim_date,         ║
    ║          mart_daily_repo_activity, mart_pr_lifecycle,               ║
    ║          mart_hourly_volume                 [retained forever]      ║
    ╚══════════════════════════════════╤══════════════════════════════════╝
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
     ┌────────▼────────┐     ┌─────────▼─────────┐   ┌──────────▼────────┐
     │  dbt tests      │     │  Streamlit app    │   │  Run log +        │
     │  + freshness    │     │  analytics +      │   │  alerting         │
     │  SLA checks     │     │  status page      │   │  (email on fail)  │
     └─────────────────┘     └───────────────────┘   └───────────────────┘

        ORCHESTRATION: GitHub Actions — hourly cron + manual backfill
```

This section will be replaced with the full README (problem statement, tech stack
rationale, data model, quality approach, local setup, and scale discussion) in
**Phase 10 — Documentation**, once there's a working pipeline to document.

## Local setup (Phase 0)

```bash
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -e ".[notebook]"
cp .env.example .env   # defaults to STORAGE_BACKEND=local, no cloud account needed
python -c "from pipeline.config import Settings; print(Settings.load())"

python -m pipeline.cli ingest --hour 2024-01-15T12   # writes data/bronze/...

# WHY this mkdir is needed: DuckDB's COPY TO (which dbt-duckdb's "external"
# materialization uses under the hood) does not create missing parent
# directories — confirmed directly in Phase 4, not assumed. `dbt run` will
# fail with an IO Error until this exists at least once.
mkdir -p data/silver
cd transform
dbt run --select staging --profiles-dir .
dbt test --select staging --profiles-dir .
```

## Project status

Built phase by phase per `gh-archive-lakehouse-BUILD-SPEC.md`. See that document for
the full build plan, or `docs/decisions/` (from Phase 1 onward) for the reasoning
behind individual design choices.
