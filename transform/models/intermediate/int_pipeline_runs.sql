-- WHY no defensive handling for "zero run-log files exist yet": DuckDB's
-- read_parquet() raises on a glob matching zero files (confirmed directly),
-- so this depends on at least one run already being logged. That's already
-- guaranteed by the real workflow order — `ingest` always runs before
-- `dbt build` (ingest_hourly.yml, backfill.yml), and ingest_hour() logs
-- every non-skipped outcome — exactly the same ordering dependency
-- stg_events already has on bronze existing first.

select *
from read_parquet('{{ var("gold_root") }}/ops/pipeline_runs/*.parquet')
