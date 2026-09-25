# ADR-002: DuckDB over Spark

**Status:** Accepted
**Date:** 2026-09-20 (Phase 0)

## Context

This project needed a compute engine to read Parquet, transform it, and
write it back out — the workhorse behind every dbt model in this pipeline.
Spark is the default answer most people reach for on a "data engineering
portfolio project," largely because it's the name most associated with the
field, not because the workload demands it.

## Decision

Use **DuckDB** as the compute engine (via `dbt-duckdb`), not Spark.

## Real evidence from this build, not just theory

- Every real dbt run in this project processed a few million rows in
  **single-digit seconds** (the full `dbt build`, staging through gold,
  regularly finishes in 3-30 seconds against ~4.6M real events). Spark's
  startup and scheduling overhead alone is often several seconds *before*
  any actual work happens — for data this size, Spark would be slower
  end-to-end than DuckDB, not faster.
- Phase 5 needed a point-in-time SCD2 join between 4.6M events and 725K
  repo-history rows. Done naively it took **34 minutes**; the fix was one
  SQL rewrite (`coalesce()` instead of `OR` in the join condition) that
  dropped it to **8.6 seconds** — a single-process, single-machine engine
  with a good query planner, once the query itself was written correctly,
  handled this without needing to scale out at all.
- DuckDB runs embedded, in-process, with zero cluster to provision,
  configure, or pay for. GitHub Actions' free runners (2 CPU, 7GB RAM) are
  plenty.

## Alternatives considered

- **Spark.** Rejected for this project's actual scale. Spark's real
  strength is distributing work across many machines when a single
  machine's RAM or CPU genuinely can't hold the data or finish the job in
  reasonable time — several million rows on a few GB of Parquet is nowhere
  near that line. Choosing Spark here would mean paying its real costs
  (cluster provisioning, JVM startup, distributed-systems failure modes)
  for a scaling problem this project doesn't have.
- **pandas.** Rejected even earlier (Phase 0) — pandas loads entire
  datasets into memory with no streaming execution; an hour of raw GH
  Archive data (~100MB uncompressed, per Phase 1's measurement) is
  manageable, but pandas has no answer for when that grows, while DuckDB's
  vectorized, out-of-core execution does.

## Consequences

- This project's honest scale ceiling is real and worth naming, not
  glossing over: DuckDB is a single-machine engine. If gold's fact table
  grew into the billions of rows, or if this needed genuine multi-machine
  parallelism, that's the point Spark (or a cloud warehouse like Snowflake
  or BigQuery) would earn its cost. See the README's "What I would change
  at 100x" section for specifics.
- Every dbt model in this project is plain SQL DuckDB can execute directly —
  there's no Spark-specific code (no RDDs, no `.repartition()` tuning) to
  port if that day comes; the migration cost of outgrowing DuckDB is mostly
  "point dbt at a different adapter," not a rewrite.
