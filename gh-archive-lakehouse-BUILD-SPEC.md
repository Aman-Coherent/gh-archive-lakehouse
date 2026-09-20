# GH Archive Lakehouse — Full Build Specification

**A production-shaped data engineering project, built entirely on free tiers.**

This document is both a *build plan* and an *agent prompt*. Hand it to your coding agent, and it will build the project phase by phase, teaching you each concept before it writes any code.

---

## PART 0 — HOW TO USE THIS DOCUMENT

### If you are the human

1. Open a fresh agent session in an empty repository folder.
2. Paste this entire document as your first message, followed by:
   > "Read this spec fully. Confirm you understand the Agent Operating Protocol, then begin Phase 0. Do not skip ahead."
3. The agent will teach, then build, then stop at a checkpoint and wait for you.
4. You reply `approved` (or ask questions) before it moves to the next phase.
5. **Do not let it run multiple phases at once.** The whole point is that you understand what you shipped. If a recruiter asks "walk me through your pipeline" and you can't, the project is worthless.

**Expected time:** 3–4 weeks at a few hours per week. Phases 2, 5, and 7 are the heavy ones.

### If you are the agent

Read Part 1 (Operating Protocol) before doing anything. It is binding for the entire build.

---

## PART 1 — AGENT OPERATING PROTOCOL

You are acting as a **senior data engineer pairing with a junior engineer who is learning**. Your job is not just to produce working code — it is to make sure the human can defend every line of it in a technical interview.

### The phase loop (mandatory for every phase)

For each phase, you must follow these five steps **in order**, and you must not merge them:

**Step 1 — TEACH (before writing any code)**

Deliver a short brief covering:
- **What** we are building in this phase, in plain language.
- **Why** it matters — specifically, what a data engineering interviewer would ask about it.
- **How** it works — the underlying concept, explained from first principles. Use a concrete example with real numbers.
- **What could go wrong** — the failure mode this design protects against.
- **The alternatives I rejected** — name at least one other approach and why we aren't using it.

Keep this to roughly 300–500 words. Plain prose, no wall of bullets.

**Step 2 — PLAN**

List the exact files you will create or modify, one line each, with a one-clause purpose. Wait for nothing here — go straight to Step 3 unless the plan contradicts the spec.

**Step 3 — BUILD**

Write the code. Rules:
- Every module gets a docstring explaining its role in the pipeline.
- Every non-obvious decision gets an inline comment starting with `# WHY:`.
- No placeholder code, no `TODO`, no stubbed functions. If a phase is specified, it ships complete and working.
- Type hints on all public functions.
- No secrets in code, ever. Read from environment variables.

**Step 4 — VERIFY**

Run the acceptance checks listed in the phase. Show the actual command output. If something fails, fix it and show the passing run. **Never claim a phase works without executing it.**

**Step 5 — CHECKPOINT**

Stop. Output exactly this block and then end your turn:

```
=== PHASE <N> COMPLETE ===
Built:        <one line>
Verified by:  <the command(s) you ran>
You now understand: <the 2-3 concepts from Step 1>
Interview question this answers: "<a real question>"
Next phase: <N+1> — <name>

Reply "approved" to continue, or ask me anything about what we just built.
```

Then **wait**. Do not begin the next phase until the human replies.

### Standing rules

- **Measure, don't assume.** Where this spec gives an estimate (file sizes, row counts, costs), treat it as a hypothesis to verify, not a fact. Several are flagged explicitly.
- **Free tier limits change.** Before relying on any provider's free allowance, check their current pricing page and report what you find. If a limit has changed, say so and propose an adjustment rather than silently proceeding.
- **Commit at every phase boundary** with a conventional commit message (`feat:`, `chore:`, `docs:`).
- **If the human's environment blocks something** (no Docker, no network, wrong Python version), say so plainly and propose the workaround. Don't fake success.
- **Explain jargon on first use.** "Idempotent", "partition pruning", "SCD2", "surrogate key" — define each the first time it appears.

---

## PART 2 — PROJECT OVERVIEW

### The problem this project solves

GitHub publishes every public event — pushes, pull requests, issues, stars, forks — as a continuous stream. [GH Archive](https://www.gharchive.org/) mirrors this as hourly gzipped JSON files, free, with no authentication and no rate limits.

We will build a pipeline that ingests this stream continuously, models it into a dimensional warehouse, tests it, monitors it, and serves it through a dashboard — and keeps the whole thing inside free-tier limits through a tiered retention policy.

### Why this source (and not a Kaggle CSV)

Almost every junior portfolio project loads a static historical file once. That design **cannot demonstrate** the skills data engineering interviews actually probe, because the source never changes.

An hourly-updating source forces you to solve real problems:

| Problem | Why a static CSV can't show it |
|---|---|
| Incremental loading | Nothing new ever arrives |
| Idempotency | You only ever run it once |
| Backfill | There's no gap to fill |
| Late-arriving data | Data never arrives late |
| Freshness SLAs | Freshness is undefined |
| Retention / tiered storage | Volume never grows |
| Slowly changing dimensions | Attributes never change |

This project demonstrates all seven.

### Architecture

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

### The tiered retention idea (this is the clever bit)

Full-volume GitHub event data is far too large for any free storage tier if you keep it forever. Real lakehouses solve this the same way we will:

- **Bronze and Silver** hold full-detail rows but are **deleted after 7 days**. They exist to be transformed, not archived.
- **Gold** holds aggregates and dimensions — tiny by comparison — and is **kept forever**.

This means your storage footprint stabilises instead of growing without bound, and it gives you a genuinely senior talking point: *"I implemented a tiered retention policy because full-fidelity history wasn't worth the storage cost for the questions the marts needed to answer."*

### Tech stack and the reasoning

| Layer | Choice | Why this and not the obvious alternative |
|---|---|---|
| Compute | **DuckDB** | Handles multi-GB Parquet on a laptop with streaming execution. Alternative rejected: pandas — loads everything into RAM and would die on an hour of this data. |
| Storage format | **Parquet + zstd** | Columnar, compresses ~10x vs JSON, supports partition pruning. Alternative rejected: CSV — no types, no compression, no predicate pushdown. |
| Object store | **Cloudflare R2** | Free tier with zero egress fees, S3-compatible API. Alternative rejected: S3 — egress charges can surprise you. |
| Transformation | **dbt Core** | Free, gives you tests, lineage, docs, and SCD2 snapshots out of the box. Alternative rejected: raw SQL scripts — no lineage, no tests, no docs. |
| Orchestration | **GitHub Actions** | Free and unlimited for public repos, cron built in. Alternative rejected: Airflow — needs a server you'd have to pay for. |
| Dashboard | **Streamlit Community Cloud** | Free public hosting, pure Python. Alternative rejected: a React frontend — this project's value is the pipeline, not the UI. |

> **Agent: verify before relying on these.** Cloudflare R2, GitHub Actions, and Streamlit Community Cloud free tiers are accurate as of this spec's writing but change regularly. Check current limits in Phase 0 and report them.

### Free-tier budget (to be verified, not assumed)

| Resource | Expected usage | Free allowance | Headroom |
|---|---|---|---|
| R2 storage | Measure in Phase 1 | Check current | Retention policy tuned to fit |
| R2 operations | ~24 writes + reads/day | Check current | Large |
| Actions minutes | ~5 min/hour ≈ 120 min/day | Unlimited on **public** repos | Repo must be public |
| Streamlit | 1 app | Check current | Fine |

**Critical:** the repository **must be public** for the Actions allowance. This is also good for you — the repo *is* the portfolio piece.

---

## PART 3 — REPOSITORY STRUCTURE

```
gh-archive-lakehouse/
├── .github/
│   └── workflows/
│       ├── ingest_hourly.yml      # cron: hourly ingest + transform + test
│       ├── backfill.yml           # manual: reprocess a date range
│       └── ci.yml                 # on PR: lint, unit tests, dbt parse
├── pipeline/
│   ├── __init__.py
│   ├── config.py                  # env-driven settings, no hardcoded values
│   ├── storage.py                 # local/R2 abstraction over fsspec
│   ├── ingest.py                  # download → filter → flatten → write
│   ├── retention.py               # enforce bronze/silver TTL
│   ├── runlog.py                  # write pipeline run metadata
│   └── cli.py                     # typer CLI: ingest, backfill, retention
├── transform/                     # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/               # silver: clean + dedupe
│   │   ├── intermediate/          # reusable joins
│   │   └── marts/                 # gold: facts, dims, aggregates
│   ├── snapshots/                 # SCD2 on dim_repo
│   ├── seeds/                     # static lookup: event type descriptions
│   ├── macros/
│   └── tests/                     # custom singular tests
├── dashboard/
│   └── app.py                     # Streamlit: analytics + status page
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── decisions/                 # ADR-001.md, ADR-002.md, ...
├── tests/                         # pytest for pipeline/
├── notebooks/
│   └── 01_explore_source.ipynb    # Phase 1 exploration
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

---

# PART 4 — THE PHASES

---

## PHASE 0 — Foundations

**Goal:** a clean, reproducible skeleton. No data yet.

### Teach first
Cover: why configuration lives in environment variables and never in code; what "reproducible environment" means and why `pyproject.toml` with pinned versions beats a loose `requirements.txt`; what the `.env` / `.env.example` split is for.

### Build
- `pyproject.toml` with dependencies: `duckdb`, `dbt-core`, `dbt-duckdb`, `boto3`, `fsspec`, `s3fs`, `requests`, `typer`, `pytest`, `ruff`, `python-dotenv`, `streamlit`, `plotly`.
- `.gitignore` covering `.env`, `*.duckdb`, `data/`, `target/`, `__pycache__/`, `.venv/`.
- `.env.example` with every variable named and a comment, **no real values**.
- `pipeline/config.py` — a `Settings` dataclass loaded from env, with sane local defaults so the project runs with zero cloud setup.
- `README.md` skeleton with the architecture diagram from Part 2.
- Initialise git, first commit.

### Config contract
```
STORAGE_BACKEND=local|r2        # local for dev, r2 for CI
LOCAL_DATA_ROOT=./data
R2_ACCOUNT_ID=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET=
BRONZE_RETENTION_DAYS=7
EVENT_TYPES=PushEvent,PullRequestEvent,IssuesEvent,IssueCommentEvent,WatchEvent,ForkEvent
FRESHNESS_SLA_HOURS=3
ALERT_EMAIL=
```

### Acceptance
- `python -c "from pipeline.config import Settings; print(Settings.load())"` prints resolved settings.
- `ruff check .` passes.
- `git log` shows one commit.
- Agent reports current free-tier limits for R2, Actions, and Streamlit as actually found today.

---

## PHASE 1 — Understand the source

**Goal:** know your data before you build for it. Produce real measurements that drive later design decisions.

### Teach first
Cover: why a data engineer profiles the source before writing the pipeline; what nested JSON means for a columnar store; what "projection" (selecting only needed columns) and "predicate pushdown" mean and why they matter for cost.

### Build
`notebooks/01_explore_source.ipynb` that downloads **one** hour file (e.g. `https://data.gharchive.org/2024-01-15-12.json.gz`) and answers:

1. Compressed size, uncompressed size, event count.
2. Distribution of `type` — which event types dominate?
3. Full schema tree, including how deeply `payload` nests and how it **varies by event type**.
4. Which fields are consistently present vs sparse.
5. **The key measurement:** write the same data three ways and compare sizes — raw JSON, Parquet all-columns, Parquet with only our projected columns. Report the compression ratio.
6. Extrapolate: at this rate, what is daily and monthly volume for bronze? Does a 7-day retention window fit the free tier?

### Output
A written summary in the notebook, and `docs/decisions/ADR-001-retention-and-projection.md` recording the measured numbers and the resulting retention/projection decision.

### Acceptance
- Notebook runs top to bottom without error.
- ADR-001 exists and contains **real measured numbers**, not estimates copied from this spec.
- Agent explicitly states whether the 7-day default needs adjusting based on what it measured.

---

## PHASE 2 — Ingestion (Bronze)

**Goal:** a reliable, idempotent, resumable loader. This is the heart of the project.

### Teach first
Cover these four concepts properly, with examples:
- **Idempotency** — running the same hour twice must not produce duplicate rows. Explain *atomic partition overwrite* as the mechanism.
- **Partitioning** — why `event_date=.../hour=...` directory layout lets a query engine skip files it doesn't need.
- **Atomic writes** — write to a temp path, then move. Explain what a half-written partition does to downstream consumers.
- **Graceful failure** — a missing or corrupt upstream file should mark the run failed and move on, not crash the scheduler.

### Build

`pipeline/storage.py`
```python
class Storage:
    """Filesystem abstraction over local disk and S3-compatible object stores."""
    def write_parquet_atomic(self, df, path: str) -> None: ...
    def exists(self, path: str) -> bool: ...
    def list_partitions(self, prefix: str) -> list[str]: ...
    def delete_prefix(self, prefix: str) -> int: ...
```
Backed by `fsspec` so `local` and `r2` share one code path. **WHY: the same code must run on a laptop and in CI without branching.**

`pipeline/ingest.py`
```python
def ingest_hour(dt: datetime, *, force: bool = False) -> IngestResult:
    """Download one GH Archive hour, project + flatten, write one Parquet partition."""
```
Logic:
1. Build source URL from the timestamp.
2. If partition exists and `force=False` → skip, return `status="skipped"`.
3. Stream-download the `.gz`. **Do not load it all into memory.**
4. Parse NDJSON line by line; skip malformed lines but **count them**.
5. Filter to `EVENT_TYPES` from config.
6. Flatten to the bronze schema below.
7. Write atomically to `bronze/events/event_date=.../hour=.../part-0.parquet` with zstd.
8. Return counts: read, kept, malformed, bytes written.

### Bronze schema
| Column | Type | Source |
|---|---|---|
| `event_id` | BIGINT | `id` |
| `event_type` | VARCHAR | `type` |
| `created_at` | TIMESTAMP | `created_at` |
| `actor_id` | BIGINT | `actor.id` |
| `actor_login` | VARCHAR | `actor.login` |
| `repo_id` | BIGINT | `repo.id` |
| `repo_name` | VARCHAR | `repo.name` |
| `org_id` | BIGINT | `org.id` (nullable) |
| `action` | VARCHAR | `payload.action` (nullable) |
| `pr_number` | INT | `payload.pull_request.number` |
| `pr_merged` | BOOLEAN | `payload.pull_request.merged` |
| `pr_created_at` | TIMESTAMP | `payload.pull_request.created_at` |
| `pr_merged_at` | TIMESTAMP | `payload.pull_request.merged_at` |
| `issue_number` | INT | `payload.issue.number` |
| `commit_count` | INT | `len(payload.commits)` |
| `ingested_at` | TIMESTAMP | now, UTC |
| `source_file` | VARCHAR | the URL |

Last two are **lineage columns** — teach why every row should know where it came from and when it arrived.

`pipeline/cli.py` — Typer commands: `ingest --hour`, `backfill --start --end`, `retention`.

`tests/test_ingest.py` — pytest covering: idempotent re-run produces identical output; malformed lines are counted not fatal; event type filter works.

### Acceptance
- `python -m pipeline.cli ingest --hour 2024-01-15T12` writes a partition.
- Running it **again** returns `skipped` and the file's modification time is unchanged.
- `duckdb -c "SELECT count(*), count(DISTINCT event_id) FROM 'data/bronze/**/*.parquet'"` — the two numbers are equal (no duplicates).
- `pytest tests/ -v` passes.

---

## PHASE 3 — Backfill and resumability

**Goal:** fill a date range, survive interruption, run in parallel.

### Teach first
Cover: what backfill means and why every pipeline needs one; why backfill must reuse the *exact same code path* as the scheduled run (divergence is a classic production bug); the risk of hammering a public free service and why you rate-limit yourself.

### Build
- `backfill(start, end, *, workers: int, force: bool)` — iterate hours, skip existing partitions, process with a small thread pool.
- Bounded concurrency (default 4) and a politeness delay. **WHY: GH Archive is free infrastructure; don't abuse it.**
- Per-hour failures are logged and collected, not fatal. Print a summary table at the end.
- Re-running a partially failed backfill picks up only what's missing.

### Acceptance
- Backfill 24 hours; confirm 24 partitions.
- Kill it halfway, restart, confirm it completes without reprocessing what it already did.
- Confirm `count(*) == count(DISTINCT event_id)` across the full range.

---

## PHASE 4 — Staging models (Silver)

**Goal:** dbt is now driving. Clean, deduplicate, conform.

### Teach first
Cover: what dbt actually does (it compiles SQL and manages dependency order — it is not an execution engine); what a staging layer is for and why you never let marts read raw data directly; **deduplication using a window function**, with the pattern written out and explained.

### Build
dbt project configured with `dbt-duckdb`, reading bronze Parquet via `read_parquet` with a glob, writing silver as external Parquet.

`models/staging/stg_events.sql`
- Read bronze.
- Deduplicate: `row_number() over (partition by event_id order by ingested_at desc) = 1`. **Teach why duplicates can occur at all** — a re-run with `force=true`, or an upstream republish.
- Cast types, trim strings, normalise `repo_name` casing.
- Split `repo_name` into `repo_owner` and `repo_short_name`.
- Add `event_date`, `event_hour` derived columns.

`models/staging/stg_repos.sql` — one row per `repo_id` per day, with the name observed that day.
`models/staging/stg_actors.sql` — one row per `actor_id`, latest login observed.

`models/staging/_staging.yml` — descriptions for every column, plus tests: `unique` and `not_null` on `event_id`, `not_null` on `repo_id` and `actor_id`, `accepted_values` on `event_type`.

### Acceptance
- `dbt run --select staging` succeeds.
- `dbt test --select staging` — all pass.
- `dbt docs generate` produces a lineage graph.

---

## PHASE 5 — Dimensional model (Gold)

**Goal:** a proper star schema, including the SCD2 that most candidates never attempt.

### Teach first
This is the most interview-relevant phase. Cover thoroughly:
- **Star schema** — facts (measurements, many rows) vs dimensions (context, fewer rows). Why denormalising into a star beats a fully normalised model for analytics.
- **Grain** — state the grain of the fact table in one sentence and explain why an undefined grain is the root cause of most broken dashboards.
- **Surrogate keys** — why you don't use the source system's ID as your primary key.
- **SCD Type 2** — what it is, with a worked example: a repo is renamed from `old/name` to `new/name`. Type 1 overwrites and loses history; Type 2 closes the old row and opens a new one, so a query about last month still returns the name it had *then*.

### Build

**`snapshots/dim_repo_snapshot.sql`** — dbt snapshot, `strategy='check'`, `check_cols=['repo_name', 'repo_owner']`. This is what produces `valid_from` / `valid_to` history automatically.

**`models/marts/dim_repo.sql`** — reads the snapshot; exposes `repo_key` (surrogate), `repo_id`, `repo_name`, `repo_owner`, `valid_from`, `valid_to`, `is_current`.

**`models/marts/dim_actor.sql`** — SCD Type 1 (latest login wins). **Teach why this dimension doesn't need Type 2** — login changes here aren't analytically interesting.

**`models/marts/dim_date.sql`** — generated calendar with `date_key`, `day_of_week`, `is_weekend`, `iso_week`, `month`, `quarter`, `year`.

**`seeds/event_types.csv`** → `dim_event_type` with a human-readable description and a category (`code`, `collaboration`, `social`).

**`models/marts/fact_events.sql`**
> **Grain: one row per GitHub event.**
Foreign keys to `dim_repo` (joined on `repo_id` **and** timestamp between `valid_from` and `valid_to` — teach this point-in-time join carefully, it's the whole payoff of SCD2), `dim_actor`, `dim_date`, `dim_event_type`. Plus measures: `commit_count`, `is_pr_merged`, `hours_to_merge`.

**Aggregate marts:**
- `mart_daily_repo_activity` — per repo per day: event counts by category, distinct contributors, commits.
- `mart_pr_lifecycle` — per merged PR: time from open to merge, with percentiles by repo.
- `mart_hourly_volume` — events per hour, for the ops/status page.

Full `schema.yml` with descriptions, `relationships` tests on every FK, and `dbt_utils` uniqueness tests on composite mart keys.

### Acceptance
- `dbt build --select marts` runs and all tests pass.
- Point-in-time join demonstrated: a query showing a repo that was renamed, returning the correct historical name for an old event. **If no renamed repo exists in the sample, construct a test fixture that proves the logic.**
- Lineage graph in `dbt docs` shows bronze → staging → marts cleanly.

---

## PHASE 6 — Data quality and freshness

**Goal:** the pipeline knows when it is wrong.

### Teach first
Cover: the difference between a *test* (is this data correct?) and a *freshness check* (is this data recent?); why "the job succeeded" and "the data is good" are completely different claims; what an SLA is; why anomaly detection on row counts catches problems that schema tests never will.

### Build
- **Schema tests** — ensure every model has `not_null` and `unique` where applicable, `accepted_values` on all enums, `relationships` on all FKs.
- **Custom singular tests** in `transform/tests/`:
  - `assert_no_future_events.sql` — no `created_at` beyond now.
  - `assert_hourly_volume_not_anomalous.sql` — flag any hour whose count is more than 3 standard deviations from the trailing 7-day mean for that hour-of-day. **Teach why hour-of-day matters** — GitHub traffic is heavily diurnal, so a flat threshold produces constant false alarms.
  - `assert_pr_merge_after_open.sql` — `merged_at >= created_at`.
- **Freshness check** — `pipeline/quality.py::check_freshness()` compares max `event_date`/`hour` in gold against now; fails if older than `FRESHNESS_SLA_HOURS`.
- **Severity tiers** — configure some tests as `severity: warn` and others as `error`. Teach the judgement: what should page you at 3am vs what should just be noted.

### Acceptance
- `dbt test` passes on good data.
- Deliberately corrupt a partition (inject a future timestamp), confirm the test **fails**, then clean up. Show both runs.
- `python -m pipeline.cli check-freshness` returns correct exit codes (0 fresh, 1 stale).

---

## PHASE 7 — Orchestration

**Goal:** it runs without you.

### Teach first
Cover: what an orchestrator does; why cron-on-CI is a legitimate choice at this scale and where it stops being one (say this honestly — it's a *strength* in an interview to know your design's limits); why the schedule must lag the source; idempotency as the thing that makes retries safe.

**The lag point matters:** GH Archive publishes an hour's file *after* that hour ends, with some delay. If you schedule at `:05` to fetch the hour that just ended, you will intermittently 404. Schedule to fetch **the hour before last**. Teach this as an example of *late-arriving data*.

### Build

**`.github/workflows/ingest_hourly.yml`**
```yaml
on:
  schedule:
    - cron: '15 * * * *'
  workflow_dispatch:
```
Steps: checkout → setup Python with cache → install → `ingest --hour <T-2h>` → `dbt build` → `check-freshness` → write run log → on failure, send alert.

Secrets via `${{ secrets.* }}`. **Never echo them.**

**`.github/workflows/backfill.yml`** — `workflow_dispatch` with `start_date`, `end_date`, `force` inputs.

**`.github/workflows/ci.yml`** — on pull request: `ruff`, `pytest`, `dbt parse`, `dbt compile`. **Teach why compiling dbt in CI catches broken SQL before it reaches production.**

**Retention enforcement** — `pipeline/retention.py` deletes bronze/silver partitions older than `BRONZE_RETENTION_DAYS`, runs daily, logs bytes reclaimed. Gold is never touched.

**Concurrency guard** — `concurrency: group: ingest` so two runs can't overlap.

### Acceptance
- Manually trigger the workflow; it goes green.
- Wait for one natural cron fire; confirm it ran unattended.
- Trigger a backfill of 3 days; confirm partitions land.
- Run retention with a short TTL on test data; confirm old partitions go and gold survives.

---

## PHASE 8 — Observability

**Goal:** you can answer "is the pipeline healthy?" without reading logs.

### Teach first
Cover: the three pillars (metrics, logs, traces) and which ones matter here; why a run log is itself a dataset that belongs in the warehouse; what makes an alert actionable versus noise.

### Build
- `pipeline/runlog.py` — append one row per run to `gold/ops/pipeline_runs/`: `run_id`, `started_at`, `finished_at`, `duration_s`, `status`, `hour_processed`, `rows_read`, `rows_written`, `malformed_rows`, `bytes_written`, `error_message`.
- `mart_pipeline_health` — dbt model over the run log: success rate over 24h/7d, p50/p95 duration, consecutive failures, current data age.
- **Alerting** — on failure, send an email via SMTP to `ALERT_EMAIL`. Content must be actionable: which hour, which step, the error, and the link to the run. **Teach: an alert that says "job failed" and nothing else is worse than no alert.**
- Structured JSON logging throughout, with `run_id` on every line.

### Acceptance
- Run log accumulates rows across several runs.
- `mart_pipeline_health` returns sensible numbers.
- Force a failure; confirm the email arrives and contains all four actionable fields.

---

## PHASE 9 — Dashboard

**Goal:** a public URL that proves it all works.

### Teach first
Cover: why the dashboard reads from gold and never from bronze; why pre-aggregated marts make the UI fast; caching, and why a dashboard hitting the warehouse on every keystroke is a cost problem.

### Build
`dashboard/app.py` — Streamlit, two tabs.

**Tab 1 — Analytics**
- Date range and event category filters.
- Hourly event volume over time (shows the diurnal pattern clearly).
- Top repos by activity.
- PR merge-time distribution.
- Weekday vs weekend activity split.

**Tab 2 — Pipeline Status** *(this is the differentiator — build it properly)*
- Big freshness indicator: green/amber/red against the SLA, showing current data age.
- Last 48 runs as a success/failure strip.
- Rows ingested per hour.
- dbt test pass rate.
- Storage footprint by zone, showing retention working.

Use `@st.cache_data(ttl=...)` on every query. Deploy to Streamlit Community Cloud. Put the live URL in the README.

### Acceptance
- Runs locally.
- Deployed and publicly reachable.
- Status tab reflects reality — verified by forcing a failure and watching the strip change.

---

## PHASE 10 — Documentation

**Goal:** the part that actually gets you the interview.

### Teach first
Explain plainly: most reviewers spend 60–90 seconds on a repo and **never run the code**. The README is therefore not documentation *about* the project — for most readers it *is* the project. Treat it as the primary deliverable it is.

### Build

**`README.md`**, in this order:
1. One-sentence description and the live dashboard link.
2. Architecture diagram.
3. **The problem and why the design solves it** — lead with tiered retention and idempotency. This is your differentiation; put it above the fold.
4. Tech stack table *with the rejected alternatives* — this is the section that reads as senior.
5. Data model: star schema diagram, grain statement, SCD2 explanation.
6. Data quality approach: tests, freshness SLA, anomaly detection.
7. How to run it locally — commands that actually work from a clean clone.
8. **Scale section:** "What I would change at 100x" — Airflow or Dagster for orchestration, Spark or a warehouse for compute, Iceberg or Delta for ACID table format, partition-level lineage. Be specific about *why* each swap becomes worth its cost at that scale.
9. What you'd do differently, honestly.

**`docs/architecture.md`** — the deep version.
**`docs/data_dictionary.md`** — every gold column, generated from dbt docs where possible.
**`docs/decisions/`** — ADRs for: retention and projection (from Phase 1), DuckDB over Spark, GitHub Actions over Airflow, SCD2 on repo only.

### Acceptance
- A clean clone plus the documented steps gets a working local run. **Agent must actually test this in a fresh directory.**
- `dbt docs` site builds.
- Every ADR states the decision, the context, the alternatives, and the consequences.

---

## PART 5 — RESUME BULLETS

Once built, these replace or supplement your project section. Adjust the numbers to what you actually measured — **do not ship a number you can't explain.**

```latex
\begin{tabular*}{\textwidth}[t]{l@{\extracolsep{\fill}}r}
\textbf{\large GitHub Events Lakehouse | End-to-End Data Pipeline} \,\, \href{DASHBOARD_URL}{(\textcolor{blue}{Link})} & \href{REPO_URL}{(\textcolor{blue}{GitHub})}
\end{tabular*}\\
\vspace{1mm}
\textit{Python, DuckDB, dbt, Parquet, Cloudflare R2, GitHub Actions, Streamlit}
\begin{itemize}[leftmargin=3ex, rightmargin=2ex, noitemsep,labelsep=1.2mm,itemsep=0.5mm]\normalsize
\item Built an idempotent hourly \textbf{ETL pipeline} ingesting \textbf{XM+ GitHub events/day} into a partitioned \textbf{Parquet} lakehouse with atomic writes, backfill, and tiered retention.
\item Modeled a \textbf{star schema} in \textbf{dbt} with \textbf{SCD Type 2} repository history and \textbf{XX data quality tests}, including anomaly detection on hourly volume.
\item Orchestrated via \textbf{GitHub Actions} with freshness \textbf{SLA} monitoring, failure alerting, and a live pipeline health dashboard.
\end{itemize}
```

### Interview questions this project now lets you answer

- *"How do you make a pipeline idempotent?"* → atomic partition overwrite, Phase 2.
- *"What happens if a job runs twice?"* → demonstrated, with the dedupe window function as a second line of defence.
- *"Explain slowly changing dimensions."* → Phase 5, with a real renamed repo.
- *"How do you handle late-arriving data?"* → Phase 7, the two-hour scheduling lag.
- *"How do you know your data is correct?"* → Phase 6, tests vs freshness vs anomaly detection.
- *"Why DuckDB and not Spark?"* → ADR, with the honest scale boundary.
- *"How would this change at 100x volume?"* → README scale section.

---

## PART 6 — GLOSSARY

The agent must define each of these the first time it comes up, and may refer back here.

**Idempotent** — running an operation N times leaves the same result as running it once.
**Partition** — a directory-level split of a dataset (here by date and hour) letting engines skip irrelevant files.
**Partition pruning** — the engine reading only the partitions a query needs.
**Predicate pushdown** — filtering inside the file reader rather than after loading.
**Projection** — selecting only the columns you need, before you read.
**Grain** — precisely what one row of a fact table represents.
**Surrogate key** — a warehouse-generated key, independent of source system IDs.
**SCD Type 1** — overwrite the attribute; history is lost.
**SCD Type 2** — close the old row, insert a new one with validity dates; history is preserved.
**Point-in-time join** — joining a fact to the dimension row that was valid *at the moment the fact occurred*.
**Backfill** — reprocessing a historical range through the current pipeline.
**Late-arriving data** — data that shows up after the window it belongs to has closed.
**Freshness SLA** — a stated maximum acceptable age for the data.
**Bronze / Silver / Gold** — raw, cleaned, and business-ready layers of a lakehouse.

---

## APPENDIX — AGENT STARTING INSTRUCTION

> Read this document in full. Confirm in your own words that you understand the Agent Operating Protocol in Part 1, especially the five-step phase loop and the requirement to stop at every checkpoint.
>
> Then begin **Phase 0**, starting with the TEACH step.
>
> Do not write code before teaching. Do not proceed past a checkpoint without explicit approval. Do not assume any number in this spec is true — measure and report.
