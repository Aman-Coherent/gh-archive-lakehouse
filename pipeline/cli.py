"""Command-line entry points for the pipeline.

WHY only `ingest` exists yet: the build spec's eventual CLI also has
`backfill` and `retention` commands, but those wrap functions that don't
exist until Phase 3 and Phase 7 respectively — adding stub commands now
would violate "no stubbed functions, ships complete and working."
"""

from __future__ import annotations

from datetime import datetime

import typer

from pipeline.ingest import ingest_hour

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


if __name__ == "__main__":
    app()
