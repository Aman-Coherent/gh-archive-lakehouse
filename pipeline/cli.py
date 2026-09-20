"""Command-line entry points for the pipeline."""

from __future__ import annotations

from datetime import datetime

import typer

from pipeline.ingest import backfill as run_backfill
from pipeline.ingest import ingest_hour
from pipeline.quality import check_freshness
from pipeline.retention import enforce_retention

app = typer.Typer()


@app.callback()
def _main() -> None:
    """gh-archive-lakehouse pipeline CLI."""
    # WHY this empty callback exists: Typer collapses an app with exactly one
    # registered command into a bare top-level command (no subcommand name
    # needed) — but `backfill` and `retention` join `ingest` as siblings in
    # later phases, and the spec's acceptance command names `ingest`
    # explicitly. A callback forces proper subcommand mode from the start.


def _parse_hour(value: str) -> datetime:
    """Parse an --hour value like '2024-01-15T12' into an exact-hour datetime."""
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H")
    except ValueError as exc:
        raise typer.BadParameter(
            f"expected format YYYY-MM-DDTHH (e.g. 2024-01-15T12), got {value!r}"
        ) from exc


@app.command()
def ingest(
    hour: str = typer.Option(..., help="Hour to ingest, e.g. 2024-01-15T12 (UTC)."),
    force: bool = typer.Option(False, help="Re-ingest even if the partition already exists."),
) -> None:
    """Ingest one hour of GH Archive data into the bronze layer."""
    dt = _parse_hour(hour)
    result = ingest_hour(dt, force=force)
    typer.echo(result)
    if result.status == "failed":
        raise typer.Exit(code=1)


@app.command()
def backfill(
    start: str = typer.Option(..., help="First hour to backfill, e.g. 2024-01-15T00 (UTC)."),
    end: str = typer.Option(..., help="Last hour to backfill, inclusive (UTC)."),
    workers: int = typer.Option(4, help="Max concurrent ingest workers."),
    force: bool = typer.Option(False, help="Re-ingest even if partitions already exist."),
) -> None:
    """Backfill a range of hours into the bronze layer."""
    start_dt = _parse_hour(start)
    end_dt = _parse_hour(end)
    results = run_backfill(start_dt, end_dt, workers=workers, force=force)

    header = f"{'hour':<18} {'status':<9} {'read':>8} {'kept':>8} {'malformed':>10} {'bytes':>10}"
    typer.echo(header)
    for r in results:
        typer.echo(
            f"{r.hour.isoformat():<18} {r.status:<9} {r.rows_read:>8} {r.rows_kept:>8} "
            f"{r.malformed_rows:>10} {r.bytes_written:>10}"
        )
        if r.error_message:
            typer.echo(f"  -> {r.error_message}")

    succeeded = sum(1 for r in results if r.status == "success")
    skipped = sum(1 for r in results if r.status == "skipped")
    failed = [r for r in results if r.status == "failed"]
    typer.echo(
        f"\n{len(results)} hours: {succeeded} succeeded, {skipped} skipped, {len(failed)} failed"
    )

    if failed:
        raise typer.Exit(code=1)


@app.command(name="check-freshness")
def check_freshness_command() -> None:
    """Check whether gold data is fresh enough per FRESHNESS_SLA_HOURS.

    Exit code 0 = fresh, 1 = stale. This is a separate claim from "did the
    last job succeed" — a job can succeed and still leave stale data if it
    simply hasn't run recently enough.
    """
    result = check_freshness()
    typer.echo(result)
    if not result.is_fresh:
        raise typer.Exit(code=1)


@app.command()
def retention() -> None:
    """Delete bronze partitions older than BRONZE_RETENTION_DAYS. Gold is never touched."""
    result = enforce_retention()
    typer.echo(result)


if __name__ == "__main__":
    app()
