"""Streamlit dashboard: Analytics + Pipeline Status, reading only from gold.

WHY only gold, never bronze: bronze is enormous (millions of raw rows),
ephemeral (deleted after BRONZE_RETENTION_DAYS), and shaped for ingestion,
not for charts. Gold is the pre-aggregated, retained-forever layer built
specifically so a dashboard's job is "read a small table and draw it" — none
of the heavy lifting happens at click time (Phase 9's teach step covers why
that matters for a shared, hosted app, not just for this one machine).

WHY every query below is wrapped in @st.cache_data: gold only changes once
an hour at most, but Streamlit re-runs this whole script on nearly every
click — without caching, moving a filter slider would re-read gold from disk
every time, for every viewer, for no reason the data could have changed that
fast.
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from pipeline.config import Settings
from pipeline.gold import connect, gold_path
from pipeline.storage import Storage

st.set_page_config(page_title="GH Archive Lakehouse", layout="wide")

# WHY this bridge: pipeline.config.Settings reads os.environ (the same
# contract used locally and in GitHub Actions) — but Streamlit Community
# Cloud's secrets manager exposes values via st.secrets, not as real
# environment variables. Copying them across once, at startup, means the
# exact same Settings/Storage code runs unmodified in both places, instead
# of the dashboard needing its own separate st.secrets-reading config path.
#
# WHY the try/except: st.secrets raises StreamlitSecretNotFoundError when no
# secrets.toml exists at all (confirmed directly, not just an empty dict) —
# true for every local run, since this project only ever uses .env locally.
try:
    for _key, _value in st.secrets.items():
        os.environ.setdefault(_key, str(_value))
except st.errors.StreamlitSecretNotFoundError:
    pass

# WHY 5 minutes: gold is refreshed at most once an hour by the pipeline —
# re-querying more often than every few minutes can't possibly see new data,
# it would just be spending disk I/O to re-confirm nothing changed yet.
_CACHE_TTL_SECONDS = 300


def _query(sql: str) -> pd.DataFrame:
    con = connect()
    return con.execute(sql).fetchdf()


@st.cache_data(ttl=_CACHE_TTL_SECONDS)
def load_daily_repo_activity() -> pd.DataFrame:
    activity = gold_path("mart_daily_repo_activity.parquet")
    dim_repo = gold_path("dim_repo.parquet")
    dim_date = gold_path("dim_date.parquet")
    df = _query(
        f"""
        select
            a.repo_id, a.date_key, a.category, a.event_count, a.distinct_contributors,
            d.date_day, d.is_weekend,
            r.repo_name
        from read_parquet('{activity}') a
        join read_parquet('{dim_date}') d on a.date_key = d.date_key
        left join read_parquet('{dim_repo}') r on a.repo_id = r.repo_id and r.is_current
        """
    )
    # WHY .dt.date: DuckDB's DATE type comes back through fetchdf() as a
    # pandas datetime64 column, not a plain Python date — comparing it
    # directly against st.date_input's date objects raises a TypeError
    # (confirmed by actually running this, not assumed).
    df["date_day"] = df["date_day"].dt.date
    return df


@st.cache_data(ttl=_CACHE_TTL_SECONDS)
def load_hourly_volume() -> pd.DataFrame:
    volume = gold_path("mart_hourly_volume.parquet")
    dim_date = gold_path("dim_date.parquet")
    df = _query(
        f"""
        select h.date_key, h.event_hour, h.event_count, d.date_day
        from read_parquet('{volume}') h
        join read_parquet('{dim_date}') d on h.date_key = d.date_key
        """
    )
    df["date_day"] = df["date_day"].dt.date
    return df


@st.cache_data(ttl=_CACHE_TTL_SECONDS)
def load_pr_lifecycle() -> pd.DataFrame:
    path = gold_path("mart_pr_lifecycle.parquet")
    return _query(f"select * from read_parquet('{path}')")


@st.cache_data(ttl=_CACHE_TTL_SECONDS)
def load_pipeline_health() -> pd.DataFrame:
    path = gold_path("mart_pipeline_health.parquet")
    return _query(f"select * from read_parquet('{path}')")


@st.cache_data(ttl=_CACHE_TTL_SECONDS)
def load_recent_runs(limit: int = 48) -> pd.DataFrame:
    path = gold_path("ops/pipeline_runs/*.parquet")
    return _query(f"select * from read_parquet('{path}') order by started_at desc limit {limit}")


@st.cache_data(ttl=_CACHE_TTL_SECONDS)
def load_latest_dbt_results() -> pd.DataFrame:
    path = gold_path("ops/dbt_test_results/*.parquet")
    return _query(f"select * from read_parquet('{path}') order by recorded_at desc limit 1")


@st.cache_data(ttl=_CACHE_TTL_SECONDS)
def load_storage_footprint() -> pd.DataFrame:
    # WHY pipeline.storage.Storage here instead of a SQL query: this needs
    # actual file sizes on disk/R2, not the contents of any Parquet file —
    # the same abstraction retention.py already uses to measure what it
    # reclaims, reused here to show the same footprint retention protects.
    settings = Settings.load()
    storage = Storage(settings)
    rows = []
    for zone in ("bronze", "silver", "gold"):
        files = storage.list_partitions(zone)
        rows.append(
            {
                "zone": zone,
                "file_count": len(files),
                "bytes": sum(storage.size(f) for f in files),
            }
        )
    return pd.DataFrame(rows)


def _empty_state(message: str) -> None:
    st.info(message)


def render_analytics_tab() -> None:
    daily_activity = load_daily_repo_activity()
    hourly_volume = load_hourly_volume()
    pr_lifecycle = load_pr_lifecycle()

    if daily_activity.empty:
        _empty_state("No gold data yet — run the pipeline at least once to see analytics here.")
        return

    min_date = daily_activity["date_day"].min()
    max_date = daily_activity["date_day"].max()

    col1, col2 = st.columns([2, 3])
    with col1:
        date_range = st.date_input(
            "Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date
        )
    with col2:
        categories = sorted(daily_activity["category"].unique())
        selected_categories = st.multiselect("Event categories", categories, default=categories)

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date

    filtered = daily_activity[
        (daily_activity["date_day"] >= start_date)
        & (daily_activity["date_day"] <= end_date)
        & (daily_activity["category"].isin(selected_categories))
    ]

    st.subheader("Hourly event volume")
    hv = hourly_volume[
        (hourly_volume["date_day"] >= start_date) & (hourly_volume["date_day"] <= end_date)
    ].copy()
    if hv.empty:
        _empty_state("No hourly volume in this date range.")
    else:
        hv["timestamp"] = pd.to_datetime(hv["date_day"]) + pd.to_timedelta(
            hv["event_hour"], unit="h"
        )
        fig = px.line(
            hv.sort_values("timestamp"), x="timestamp", y="event_count", title="Events per hour"
        )
        st.plotly_chart(fig, use_container_width=True)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Top repos by activity")
        if filtered.empty:
            _empty_state("No activity in this selection.")
        else:
            top_repos = (
                filtered.groupby(["repo_id", "repo_name"], as_index=False)["event_count"]
                .sum()
                .sort_values("event_count", ascending=False)
                .head(20)
            )
            fig = px.bar(
                top_repos, x="event_count", y="repo_name", orientation="h", title="Top 20 repos"
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("PR merge-time distribution")
        pr_filtered = pr_lifecycle[
            (pd.to_datetime(pr_lifecycle["merged_at"]).dt.date >= start_date)
            & (pd.to_datetime(pr_lifecycle["merged_at"]).dt.date <= end_date)
        ] if not pr_lifecycle.empty else pr_lifecycle
        if pr_filtered.empty:
            _empty_state("No merged PRs in this date range yet.")
        else:
            fig = px.histogram(pr_filtered, x="hours_to_merge", nbins=30, title="Hours to merge")
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Weekday vs weekend activity")
    if filtered.empty:
        _empty_state("No activity in this selection.")
    else:
        wd = filtered.copy()
        wd["day_type"] = wd["is_weekend"].map({True: "Weekend", False: "Weekday"})
        wd_summary = wd.groupby("day_type", as_index=False)["event_count"].sum()
        fig = px.pie(wd_summary, names="day_type", values="event_count", title="Weekday vs weekend")
        st.plotly_chart(fig, use_container_width=True)


def render_status_tab() -> None:
    settings = Settings.load()
    health = load_pipeline_health()

    if health.empty:
        _empty_state("No pipeline health data yet — the pipeline needs to run at least once.")
        return

    row = health.iloc[0]
    age_hours = float(row["data_age_hours"])
    sla_hours = settings.freshness_sla_hours

    # WHY three tiers, not just fresh/stale: an amber "getting old but not
    # broken yet" state is genuinely useful information a binary indicator
    # throws away — it's the difference between "keep an eye on this" and
    # "go look right now."
    if age_hours <= sla_hours:
        indicator, label = "🟢", "FRESH"
    elif age_hours <= sla_hours * 2:
        indicator, label = "🟡", "AGING"
    else:
        indicator, label = "🔴", "STALE"

    st.markdown(f"## {indicator} {label} — data is {age_hours:.1f}h old (SLA: {sla_hours}h)")

    col1, col2, col3 = st.columns(3)
    success_24h = row["success_rate_24h"]
    success_7d = row["success_rate_7d"]
    col1.metric(
        "Success rate (24h)", f"{success_24h * 100:.0f}%" if pd.notna(success_24h) else "N/A"
    )
    col2.metric("Success rate (7d)", f"{success_7d * 100:.0f}%" if pd.notna(success_7d) else "N/A")
    col3.metric("Consecutive failures", int(row["consecutive_failures"]))

    st.subheader("Last 48 runs")
    runs = load_recent_runs()
    if runs.empty:
        _empty_state("No runs logged yet.")
    else:
        strip = runs.sort_values("started_at").tail(48).copy()
        strip["bar_height"] = 1
        fig = px.bar(
            strip,
            x="started_at",
            y="bar_height",
            color="status",
            color_discrete_map={"success": "#2ecc71", "failed": "#e74c3c"},
            hover_data={"started_at": True, "status": True, "bar_height": False},
            title=f"Last {len(strip)} runs (green = success, red = failed)",
        )
        fig.update_yaxes(visible=False)
        fig.update_layout(showlegend=True, bargap=0.1)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Rows ingested per hour")
        fig = px.bar(strip, x="started_at", y="rows_written", title="Rows kept per run")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("dbt test pass rate")
    dbt_results = load_latest_dbt_results()
    if dbt_results.empty:
        _empty_state("No dbt test results recorded yet.")
    else:
        r = dbt_results.iloc[0]
        total = int(r["total_tests"])
        pass_rate = r["passed_tests"] / total if total else None
        st.metric(
            "Most recent dbt build",
            f"{pass_rate * 100:.0f}% pass" if pass_rate is not None else "N/A",
            help=(
                f"{int(r['passed_tests'])} passed, {int(r['warned_tests'])} warned, "
                f"{int(r['failed_tests'])} failed, out of {total} tests"
            ),
        )

    st.subheader("Storage footprint by zone")
    footprint = load_storage_footprint()
    if footprint.empty or footprint["bytes"].sum() == 0:
        _empty_state("No data on disk yet.")
    else:
        footprint = footprint.copy()
        footprint["MB"] = footprint["bytes"] / (1024**2)
        fig = px.bar(
            footprint,
            x="zone",
            y="MB",
            title="Storage by zone (bronze/silver retained 7 days, gold forever)",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(footprint[["zone", "file_count", "MB"]], hide_index=True)


def main() -> None:
    st.title("GH Archive Lakehouse")
    tab_analytics, tab_status = st.tabs(["Analytics", "Pipeline Status"])
    with tab_analytics:
        render_analytics_tab()
    with tab_status:
        render_status_tab()


main()
