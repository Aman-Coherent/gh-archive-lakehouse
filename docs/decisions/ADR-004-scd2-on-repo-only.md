# ADR-004: SCD Type 2 on `dim_repo` only

**Status:** Accepted
**Date:** 2026-09-20 (Phase 5)

## Context

The gold layer has two "real" dimensions that could plausibly change over
time: `dim_repo` (a repository can be renamed) and `dim_actor` (a GitHub
user can change their login). The build spec asks specifically for SCD Type
2 (full history preservation, `valid_from`/`valid_to`) — the question is
*where* it's actually worth the complexity.

## Decision

**`dim_repo` is SCD Type 2** (dbt snapshot, `strategy='check'` on
`repo_name`/`repo_owner`, point-in-time joined from `fact_events`).
**`dim_actor` is SCD Type 1** (plain overwrite — latest login wins, no
history kept).

## Why the split, not the same treatment for both

A repo rename changes what a *historical* report should call something. If
`old/name` was renamed to `new/name` in March, a report about February's
activity that shows `new/name` is showing something that didn't exist yet
at the time — it's not just imprecise, it's actively misleading to anyone
trying to understand what actually happened. That's exactly the failure
mode SCD2 exists to prevent, and it's the worked example this project's
Phase 5 checkpoint demonstrated with a constructed fixture (no natural
rename existed in the sampled data): an event from before a rename
correctly resolves to the old name, an event after correctly resolves to
the new one.

A username change doesn't carry the same weight. Nobody analyzing this
data is asking "what did this contributor's account used to be called last
quarter" the way they're asking "what was this repository called when this
happened." Adding SCD2's real cost — a surrogate key that's per-version
instead of per-entity, a point-in-time join everywhere `dim_actor` is used
instead of a plain lookup, a dbt snapshot to maintain — for a dimension
where nobody needs the history is complexity with no payoff.

## Alternatives considered

- **SCD2 on both dimensions**, for consistency. Rejected — "be consistent"
  isn't a strong enough reason on its own to pay a real, ongoing complexity
  cost (every downstream query touching `dim_actor` would need to reason
  about point-in-time validity) for history nobody analyzing this data
  actually needs.
- **SCD1 on both** (simpler, one pattern everywhere). Rejected — this is
  the design that would have actually broken the point-in-time-join
  demonstration this project treats as a core differentiator; overwriting
  `dim_repo` on rename means historical reports silently show the wrong
  name for events that already happened, which is a real correctness bug,
  not just a missed opportunity.

## Consequences

- `fact_events`' join to `dim_repo` has to be a genuine point-in-time join
  (`repo_id` **and** the event's timestamp falling inside `valid_from`/
  `valid_to`) — not a plain equality lookup. This was also the source of a
  real, measured bug (Phase 5): writing that join carelessly (`created_at <
  valid_to OR valid_to IS NULL`) made DuckDB fall back to a full nested-loop
  scan, 34 minutes instead of 8.6 seconds, over this project's real data
  volume. SCD2 isn't just a modeling choice — the join style it requires
  has real performance consequences that have to be gotten right.
- If a future requirement ever needs actor login history, `dim_actor`
  converting to SCD2 is a contained, well-understood change (the same
  snapshot pattern `dim_repo` already uses) — not a redesign of the star
  schema.
