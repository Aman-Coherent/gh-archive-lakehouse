# GH Archive Lakehouse

An hourly-updating, tiered-retention data lakehouse built entirely on free
infrastructure — ingesting [GH Archive](https://www.gharchive.org/)'s live
GitHub event stream into a bronze/silver/gold pipeline on DuckDB + dbt,
orchestrated by GitHub Actions, served through a Streamlit dashboard.

**Live dashboard:** _not yet deployed — pending a Cloudflare R2 account and
Streamlit Community Cloud deployment; see [Scale](#what-i-would-change-at-100x)
and [What I'd do differently](#what-id-do-differently)._
**Repo:** [github.com/Aman-Coherent/gh-archive-lakehouse](https://github.com/Aman-Coherent/gh-archive-lakehouse)

## Architecture

```
                    ┌──────────────────────────────────────┐
                    │   GH Archive (data.gharchive.org)    │
                    │   hourly .json.gz, public, no auth   │
                    └──────────────────┬───────────────────┘
                                       │
                          ┌────────────▼────────────┐
                          │  INGEST (Python)        │
                          │  • stream-download hour │
                          │  • filter event types   │
                          │  • flatten + project    │
                          │  • atomic Parquet write │
                          └────────────┬────────────┘
                                       │
    ╔══════════════════════════════════▼══════════════════════════════════╗
    ║                  OBJECT STORE (local disk / Cloudflare R2)          ║
    ║                                                                     ║
    ║  BRONZE  events/event_date=YYYY-MM-DD/hour=HH/part-0.parquet        ║
    ║          flattened, typed, unaggregated     [7-day retention]       ║
    ║                              │                                      ║
    ║                       dbt (DuckDB engine)                           ║
    ║                              │                                      ║
    ║  SILVER  stg_events, stg_repos, stg_actors                          ║
    ║          deduplicated, cleaned, conformed   [7-day retention]       ║
    ║                              │                                      ║
    ║  GOLD    fact_events (incremental), dim_repo (SCD2), dim_actor,     ║
    ║          dim_date, dim_event_type, mart_daily_repo_activity,        ║
    ║          mart_pr_lifecycle, mart_hourly_volume,                     ║
    ║          mart_pipeline_health               [retained forever]      ║
    ╚══════════════════════════════════╤══════════════════════════════════╝
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
     ┌────────▼────────┐     ┌─────────▼─────────┐   ┌──────────▼────────┐
     │  dbt tests      │     │  Streamlit app    │   │  Run log +        │
     │  + freshness    │     │  analytics +      │   │  alerting         │
     │  + anomaly      │     │  status page      │   │  (email on fail)  │
     │  detection      │     │                   │   │                   │
     └─────────────────┘     └───────────────────┘   └───────────────────┘

        ORCHESTRATION: GitHub Actions — hourly cron (T-2h lag) + manual backfill
```

## The problem, and why this design solves it

Almost every junior data portfolio project loads a static CSV once. That
can't demonstrate what data engineering interviews actually probe, because
the source never changes: no incremental loading, no idempotency question,
no backfill, no freshness SLA, no reason for retention to matter. An
hourly-updating public source forces real answers to all of those.

Two decisions carry the most weight here, and they're related:

**Tiered retention.** Full-fidelity event history is too large to keep
forever on any free tier. Bronze and silver hold complete, row-level detail
but are deleted after 7 days — they exist to be transformed, not archived.
Gold holds the aggregates and dimensions the marts and dashboard actually
need, and is kept forever. Measured directly (ADR-001): bronze+silver at
7-day retention uses roughly **5% of Cloudflare R2's 10 GiB free tier** —
comfortable headroom, verified against real data, not assumed from a rough
estimate.

**Idempotency.** Every ingest writes to a temp path and atomically moves it
into place — a partial write is never visible to a reader, and re-running
the same hour twice (a retry, a scheduler hiccup, a deliberate `force=true`
re-ingest) either finishes cleanly or is a true no-op. This is also *why*
automating this with a plain hourly cron (see the orchestration section
below) is safe at all: an unattended retry is only safe if running the same
thing twice can't corrupt anything.

## Tech stack, and what was rejected

| Layer | Choice | Rejected alternative, and why |
|---|---|---|
| Compute | **DuckDB** | Spark — real strength is distributing work across machines when a single machine genuinely can't hold the data; a few million rows in a few GB of Parquet is nowhere near that line. Every real `dbt build` in this project finishes in single-digit seconds. See [ADR-002](docs/decisions/ADR-002-duckdb-over-spark.md). |
| Storage format | **Parquet + zstd** | CSV — no types, no compression, no predicate pushdown. Measured (ADR-001): projecting to the ~12 columns actually needed, not just compressing, is what delivers a **63x** size reduction over raw JSON — compression codec choice alone barely moves the needle. |
| Object store | **Cloudflare R2** | AWS S3 — R2 has zero egress fees; S3-compatible API means the exact same `boto3`/`fsspec` code works against either. |
| Transformation | **dbt Core** | Raw SQL scripts — no lineage graph, no built-in tests, no docs site. dbt compiles SQL and manages dependency order; DuckDB still does all the real execution. |
| Orchestration | **GitHub Actions** (`schedule` trigger) | Airflow — real strength is coordinating many interdependent tasks with complex retry/backoff; this project has one job on one schedule. Verified for real: 23+ consecutive unattended successful hourly runs with zero infrastructure to provision. See [ADR-003](docs/decisions/ADR-003-github-actions-over-airflow.md). |
| Dashboard | **Streamlit Community Cloud** | A React frontend — this project's value is the pipeline, not the UI; Streamlit gets a working, shareable dashboard in pure Python. |

## Data model

**Star schema.** `fact_events` (the measurements — one row per event,
millions of rows) references four dimensions (the context — orders of
magnitude fewer rows each): `dim_repo`, `dim_actor`, `dim_date`,
`dim_event_type`.

```
                dim_date            dim_event_type
                    │                     │
                    │                     │
     dim_repo ───── fact_events ───── dim_actor
    (SCD Type 2)   (one row per         (SCD Type 1)
                     GitHub event)
```

**Grain:** `fact_events` is **one row per GitHub event.** Stated once,
explicitly, in the model's own SQL comment — an undefined grain is the root
cause of most dashboards where two people's numbers quietly disagree.

**SCD Type 2 on `dim_repo` only** (`dim_actor` is Type 1). A repo renamed
from `old/name` to `new/name` gets two rows: one valid until the rename,
one valid after. `fact_events` joins to `dim_repo` on a genuine
**point-in-time join** — `repo_id` *and* the event's timestamp falling
inside that version's `valid_from`/`valid_to` — not a plain lookup, so a
query about last month correctly returns the name that repo had *then*.
Demonstrated with a constructed fixture (no natural rename existed in the
sampled data): a February event resolves to the old name, an August event
to the new one, using the exact join predicate `fact_events` uses in
production. Why the split between dimensions, and a real performance
lesson learned writing that join: [ADR-004](docs/decisions/ADR-004-scd2-on-repo-only.md).

Full column-by-column reference: [docs/data_dictionary.md](docs/data_dictionary.md).
Deep system design: [docs/architecture.md](docs/architecture.md).

## Data quality

Three different, deliberately separate claims, not one:

- **Tests** (dbt, `transform/tests/` + schema tests) — is the data
  internally consistent? Uniqueness, referential integrity, accepted
  values, plus singular tests like "no PR merged before it opened."
  Severity tiers matter here: a duplicate primary key is `error` (pages
  someone); a new event type nobody's seen before is `warn` (worth a look,
  not a 3am wakeup) — a real distinction, not a stylistic one, after a
  hard-`error` test on a legitimately-nullable field broke the very first
  live unattended run.
- **Freshness** (`pipeline/quality.py::check_freshness()`) — is the data
  *recent* enough to trust, independent of whether it's correct? A job can
  succeed and still leave stale data if it simply hasn't run in a while.
- **Anomaly detection** (`assert_hourly_volume_not_anomalous.sql`) — flags
  any hour more than 3 standard deviations from the trailing 7-day mean
  *for that same hour of day*, not a flat threshold — GitHub's traffic is
  heavily diurnal, so comparing 3am against 3am (not 3am against a flat
  number tuned for 2pm) is the only comparison that doesn't either miss
  real problems or false-alarm every single night.

## How to run it locally

Tested from a genuinely fresh clone, not assumed to work:

```bash
git clone https://github.com/Aman-Coherent/gh-archive-lakehouse.git
cd gh-archive-lakehouse
py -3.12 -m venv .venv
.venv\Scripts\activate            # macOS/Linux: source .venv/bin/activate
pip install -e ".[notebook]"
cp .env.example .env              # defaults to STORAGE_BACKEND=local, no cloud account needed

python -m pipeline.cli ingest --hour 2024-01-15T12   # writes data/bronze/...

mkdir -p data/silver data/gold    # DuckDB's COPY TO doesn't create missing
                                   # parent directories — confirmed directly
cd transform
dbt deps --profiles-dir .
dbt build --profiles-dir . --target dev
cd ..

streamlit run dashboard/app.py    # http://localhost:8501
```

## What I would change at 100x

Named honestly, not hand-waved:

- **Orchestration:** Airflow or Dagster, once there's more than one job or
  any inter-job dependency to coordinate — GitHub Actions' `schedule`
  trigger has no answer for "run B only after A succeeds," retries with
  real backoff, or a UI to inspect a DAG. See ADR-003 for exactly where
  that line is.
- **Compute:** Spark, or a cloud warehouse (Snowflake/BigQuery), once gold
  genuinely can't fit or compute on one machine — not before. DuckDB
  handled this project's real 4.6M-row fact table in single-digit seconds;
  billions of rows is a different conversation. See ADR-002.
  - **Warehouse if the audience needs to self-serve SQL without touching
    Python/dbt tooling; Spark if the bottleneck is genuinely compute-bound
    (not I/O-bound, which DuckDB already handles well via vectorized
    execution).**
- **Table format:** Iceberg or Delta Lake instead of plain partitioned
  Parquet, once concurrent writers or true ACID transactions across
  partitions matter — this project's single-writer, one-partition-per-hour
  design sidesteps that need entirely today, but doesn't scale to multiple
  concurrent ingestion sources.
- **Lineage:** partition-level lineage (which specific bronze partitions
  fed which specific gold rows) instead of dbt's model-level lineage graph
  — useful once debugging "why is this number wrong" needs to trace back to
  one specific hour's ingest, not just one model.

## What I'd do differently

- **I'd set up Cloudflare R2 before Phase 7, not after.** Building the
  orchestration layer against `STORAGE_BACKEND=local` proved the mechanics
  work (23+ real unattended successful runs), but real cross-run
  persistence — and the actual point of a lakehouse — needed R2 the whole
  time. The abstraction (`pipeline.config.Settings` / `pipeline.storage.Storage`)
  was built to support both from Phase 0, so the switch is one env var and
  four secrets, not a redesign — but doing the account setup earlier would
  have meant demonstrating real accumulation sooner.
- **I'd write the point-in-time join's `coalesce()` form the first time,**
  not the more readable `OR` form. It cost 34 minutes of real wall-clock
  time to discover DuckDB's planner couldn't extract a hash join from it —
  a good reminder that "reads more clearly" and "compiles to the same
  query plan" aren't the same claim, and worth checking with `EXPLAIN`
  before trusting either.
- **I'd add the `int_pipeline_runs`/`mart_pipeline_health` observability
  layer in Phase 2, not Phase 8.** Every real bug found during this build
  (the network retry gap, the `fact_events` export bug, the timezone bug)
  was caught by actually running the pipeline and reading its output
  carefully — having a structured, queryable run history from the start
  would have made several of those diagnoses faster.

---

Built phase by phase per `gh-archive-lakehouse-BUILD-SPEC.md`. See
[docs/decisions/](docs/decisions/) for the full reasoning behind individual
design choices, most of them driven by something measured during the build,
not decided upfront.
