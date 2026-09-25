# ADR-003: GitHub Actions over Airflow

**Status:** Accepted
**Date:** 2026-09-20 (Phase 7)

## Context

This pipeline needs something to trigger it on a schedule, retry sensibly,
and give visibility into run history. Airflow (or a similar orchestrator
like Dagster/Prefect) is the standard answer in production data
engineering — but "standard answer" and "right tool for this specific job"
aren't automatically the same thing.

## Decision

Use **GitHub Actions' built-in `schedule` trigger** as the orchestrator, not
a dedicated tool like Airflow.

## What this project's actual orchestration needs are

One job. One schedule (hourly). No inter-job dependencies to coordinate, no
DAG of dozens of tasks, no need for a task to conditionally trigger a
different task based on runtime branching logic. Airflow exists to solve
problems in that shape — this project doesn't have that shape.

## Real evidence from this build

- The orchestration this project actually needed — a cron trigger, a
  concurrency guard so overlapping runs can't corrupt data, and a
  documented scheduling lag (fetch `T-2h`, not the hour that just ended, to
  dodge GH Archive's publish delay) — took a single ~100-line YAML file to
  express completely.
- Verified for real, not just configured and hoped: the schedule fired
  **23 times fully unattended** over roughly 4.5 days of continuous
  operation (2026-09-20 through 2026-09-25), every single run successful,
  with zero infrastructure to provision, patch, or pay for.
- GitHub Actions is free and unmetered for this project specifically
  *because* the repo is public — a real, load-bearing constraint that
  shaped the whole project (see the "must be public" note in the README),
  not a coincidence.

## Alternatives considered

- **Airflow (self-hosted or managed).** Rejected — running Airflow means
  running and maintaining a scheduler, a metadata database, and (for
  anything beyond a toy setup) a worker fleet, none of which this project
  needs to coordinate one hourly job. That's real, ongoing operational
  cost — patching, monitoring the orchestrator itself, paying for the
  compute it runs on — being paid to solve a coordination problem this
  project doesn't have.
- **A cron job on a personal VPS.** Rejected because it depends on one
  specific machine being powered on, network-connected, and *noticed* when
  it silently stops working — with no history, no logs beyond whatever you
  remembered to set up yourself, and a bill either way (a VPS isn't free
  the way GitHub Actions is for a public repo).

## Consequences

- The honest limit, named plainly: the moment this project needs one job's
  completion to *conditionally* trigger a different job, or needs
  retries with real backoff policies and dependency-aware scheduling
  across many tasks, or needs to coordinate more than a small handful of
  scheduled things — that's exactly the point a dedicated orchestrator like
  Airflow or Dagster stops being overkill and starts earning its
  operational cost. Recognizing *where that line is*, rather than reaching
  for the heavier tool by default, is the actual point of this decision.
- Because this choice was made explicitly rather than by default, moving
  to a real orchestrator later is a scoped, well-understood change (wrap
  the existing CLI commands as tasks in a DAG) rather than a redesign.
