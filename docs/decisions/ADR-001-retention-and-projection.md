# ADR-001: Bronze retention window and column projection

**Status:** Accepted
**Date:** 2026-09-20
**Phase:** 1 — Understand the source

## Context

The build spec assumed (but did not measure) two things: that a 7-day retention window
for bronze/silver would fit inside Cloudflare R2's free storage tier, and that keeping
only a projected set of columns (rather than every field GitHub sends) would meaningfully
reduce storage cost. Both are testable, so `notebooks/01_explore_source.ipynb` downloaded
one real hour of GH Archive data — **2026-09-17, 15:00 UTC, an ordinary weekday, 61,147
events** — and measured them directly instead of trusting the estimate.

## Measurement

| Form | Size | vs raw JSON |
|---|---|---|
| Raw JSON (uncompressed) | 107,955,076 bytes (103 MiB) | 1x |
| Raw JSON, gzip (what GH Archive actually serves) | 23,594,269 bytes (22.5 MiB) | 4.6x smaller |
| Parquet, all columns, zstd | 24,470,837 bytes (23.3 MiB) | 4.4x smaller |
| Parquet, projected columns (~12 cols), zstd | 1,703,444 bytes (1.6 MiB) | **63.4x smaller** |

Projection — keeping only the ~12 columns the pipeline actually needs, instead of every
field GitHub sends — accounts for almost all of the size reduction: going from
all-columns Parquet to projected Parquet is a further 14.4x drop on its own. Switching
from JSON to Parquet with compression, without projecting, only buys 4.4x. **Which
columns you keep matters far more than which compression codec you use.**

Event type mix in the sample hour: PushEvent 66.5%, PullRequestEvent 9.0%, CreateEvent
6.6%, IssueCommentEvent 4.7%, IssuesEvent 3.3%, and eleven other types making up the
remaining ~10%.

### Retention math

- Bronze, projected, at 7-day retention: 1,703,444 bytes/hour × 24 × 7 ≈ **0.267 GiB**
- Silver doesn't exist yet (Phase 4). Assumed ≤ bronze size (it's a deduplicated,
  conformed copy) as a conservative upper bound: another ~0.267 GiB
- Combined bronze + silver footprint at steady state: **~0.53 GiB**
- Cloudflare R2 free tier (Standard storage, verified in Phase 0): **10 GiB**
- Headroom: **~9.47 GiB free, using ~5.3% of the free tier**

## Decision

**Keep `BRONZE_RETENTION_DAYS=7` as the default.** The measured footprint uses only
~5% of the free tier, so 7 days is not a tight constraint — it was chosen for
freshness/debugging convenience (a week of raw history to investigate late-arriving or
malformed data), not because storage forced a shorter window. There is enormous headroom
to extend retention later (30 days would still be well under 25% of the free tier) if a
future phase needs more raw history for backfill or debugging; no reason to do so today.

**Keep column projection as designed.** The Bronze schema in Phase 2 already lists a
specific, narrow set of columns rather than "everything GitHub sends" — this measurement
confirms that choice is what actually controls cost, and validates it before any ingest
code is written.

## Two schema findings this measurement surfaced

Profiling real, current data (rather than trusting the spec's schema table) surfaced two
mismatches between the spec's assumed Bronze schema and what GitHub's live feed actually
contains today. Both are genuine, dated changes to GitHub's Events API — confirmed by
comparing against a 2025-06-10 sample, which still had the full objects.

1. **`payload.pull_request` is truncated.** It used to carry ~44 fields (including
   `merged`, `created_at`, `merged_at`); it now carries only `base`, `head`, `id`,
   `number`, `url`. The `pr_merged`, `pr_created_at`, and `pr_merged_at` columns in the
   spec's Bronze schema table cannot be populated from `payload.pull_request` anymore.
   **There is a substitute:** `payload.action` now includes a distinct `"merged"` value
   (separate from `"closed"`), and because every PR lifecycle step is its own timestamped
   event, PR lifecycle duration can be derived by matching a PR's `opened` and `merged`
   action events (by `repo_id` + `pr_number`) and diffing their `created_at` timestamps,
   instead of reading embedded fields. This changes the bronze column mapping and the
   join logic behind `mart_pr_lifecycle` (Phase 5) — to be decided explicitly in those
   phases, not patched around silently.

2. **`payload.commits` is gone from PushEvent entirely.** A real PushEvent payload today
   contains only `repository_id`, `push_id`, `ref`, `head`, `before` — no commit list, and
   no substitute count field (`size`/`distinct_size`) either. **There is no substitute
   this time**: the `commit_count` column in the spec's Bronze schema cannot be populated
   from this source at all. Recovering it would require calling GitHub's REST API per
   push, which needs authentication and has rate limits — defeating the reason this
   project uses GH Archive (free, public, no auth). Phase 2 needs to decide explicitly
   whether to drop `commit_count` from the Bronze schema or ship it as an always-NULL
   column with that limitation documented, rather than silently returning nulls with no
   explanation.

## Alternatives considered

- **Trust the spec's estimates and skip measuring.** Rejected — the spec explicitly
  requires verification, and this measurement caught two real schema mismatches that
  would otherwise have surfaced as silent null columns partway through Phase 2 or Phase 5.
- **Keep all columns in bronze "just in case."** Rejected — the 14.4x size difference
  between all-columns and projected Parquet is the single biggest lever on storage cost
  in this whole pipeline; discarding it for "just in case" flexibility isn't worth 14x
  the storage for data that isn't used downstream.

## Consequences

- Phase 2's bronze schema needs an explicit decision on `commit_count` (drop vs.
  always-NULL) and should source PR-merge information from `payload.action`, not
  `payload.pull_request.merged`.
- Phase 5's `mart_pr_lifecycle` needs to compute merge duration via a self-join on
  matching `opened`/`merged` action events rather than reading embedded timestamps.
- No change needed to `BRONZE_RETENTION_DAYS` or the general bronze/silver/gold retention
  architecture.
