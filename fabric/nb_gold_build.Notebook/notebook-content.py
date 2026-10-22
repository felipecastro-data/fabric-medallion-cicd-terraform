# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Gold · Star schema & aggregates
# - Dimensions (full rebuild when their Silver input changed): `dim_date` (calendar + US holidays), `dim_station` (+ dockless member, key -1), `dim_weather` (by `date_key`)
# - Facts per period: `fact_trips` from `lh_silver.dbo.trips`, then `agg_daily_kpis` (day x member type, with weather) and `agg_station_monthly` (starts, ends, net flow)
# - Surrogate keys are deterministic (`yyyyMMdd`, hash of `station_id`): facts never need re-keying when a dimension is rebuilt
# - **Incremental:** a period is rebuilt only when Silver trips *or* weather has a newer batch for it (watermark in `lh_gold.dbo.processing_log`)
# - **Breaking schema change:** run once with `rebuild=True` and `periods` covering every loaded month (drops and reloads the fact and aggregate tables)

# PARAMETERS CELL ********************

periods = "last:12"  # YYYYMM | YYYYMM-YYYYMM | last:N | comma-separated
force = False        # True = rebuild periods even if their latest Silver batches were already processed
rebuild = False      # True = DROP fact_trips + aggregates first (breaking schema change); implies force

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import date

spark.conf.set("spark.sql.session.timeZone", "UTC")  # Divvy timestamps are Chicago wall-clock; keep them as-is

try:
    import divvy_medallion  # noqa: F401
except ModuleNotFoundError as exc:  # fail fast with an actionable message
    raise ModuleNotFoundError(
        "divvy_medallion is not installed in this Spark session: set env_divvy as the workspace default "
        "environment (Workspace settings > Spark settings > Environment) or attach it to this notebook"
    ) from exc

from divvy_medallion import lakehouse
from divvy_medallion.bronze import PERIOD_COLUMN
from divvy_medallion.control import (
    IngestionRun,
    append_runs,
    combine_watermarks,
    last_success,
    last_success_by_period,
    latest_succeeded_batches,
    needs_rebuild,
    periods_to_process,
    processed_batches,
)
from divvy_medallion.delta import overwrite_partition, overwrite_table
from divvy_medallion.gold import aggregates, dimensions, facts
from divvy_medallion.periods import parse_periods
from divvy_medallion.silver import stations, trips
from divvy_medallion.silver import weather as silver_weather

SOURCE = "gold.fact_trips"
SILVER_TRIPS, SILVER_WEATHER, SILVER_STATIONS = "silver.trips", "silver.weather_daily", "silver.stations"
DIM_DATE_START = date(2020, 1, 1)
DIM_DATE_END = date(date.today().year + 1, 12, 31)

silver = lakehouse.resolve("lh_silver")
gold = lakehouse.resolve("lh_gold")
silver_log, log_path = silver.table("processing_log"), gold.table("processing_log")

force = str(force).strip().lower() in ("true", "1", "yes")
rebuild = str(rebuild).strip().lower() in ("true", "1", "yes")
if rebuild:  # explicit, logged migration path; partial overwrites never change column types silently
    import notebookutils

    for table in (facts.FACT_TRIPS_TABLE, aggregates.DAILY_KPIS_TABLE, aggregates.STATION_MONTHLY_TABLE):
        if notebookutils.fs.exists(gold.table(table)):
            notebookutils.fs.rm(gold.table(table), True)
            print(f"rebuild: dropped {table}")
    force = True
# a period depends on its Silver trips AND its weather (agg_daily_kpis joins the day's weather)
upstream = combine_watermarks(
    latest_succeeded_batches(spark, silver_log, SILVER_TRIPS),
    latest_succeeded_batches(spark, silver_log, SILVER_WEATHER),
)
requested = parse_periods(periods)
todo = periods_to_process(upstream, processed_batches(spark, log_path, SOURCE), requested, force)
missing_upstream = [p for p in requested if p not in upstream]
print(f"to_process={todo} up_to_date={sorted(set(requested) & set(upstream) - set(todo))} no_silver={missing_upstream}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read(path):
    return spark.read.format("delta").load(path)


def build_dimension(source, batch_id, build, table):
    run = IngestionRun(source, "ALL", batch_id)
    try:
        overwrite_table(build(), gold.table(table))
        run.succeed(read(gold.table(table)).count())
    except Exception as exc:
        run.fail(exc)
    append_runs(spark, log_path, [run])
    return run


runs = []
# dim_date is generated: rebuilt only when its date range changes (once a year)
date_range = f"{DIM_DATE_START}..{DIM_DATE_END}"
if ("ALL", date_range) not in processed_batches(spark, log_path, "gold.dim_date"):
    runs.append(build_dimension(
        "gold.dim_date", date_range,
        lambda: dimensions.build_dim_date(spark, DIM_DATE_START, DIM_DATE_END), dimensions.DIM_DATE_TABLE,
    ))
# station and weather dimensions follow their Silver input (own watermark, so a failed build is retried)
for source, upstream_source, build, table in (
    ("gold.dim_station", SILVER_STATIONS,
     lambda: dimensions.build_dim_station(read(silver.table(stations.STATIONS_TABLE))), dimensions.DIM_STATION_TABLE),
    ("gold.dim_weather", SILVER_WEATHER,
     lambda: dimensions.build_dim_weather(read(silver.table(silver_weather.SILVER_TABLE))), dimensions.DIM_WEATHER_TABLE),
):
    changed_at = last_success(spark, silver_log, upstream_source)
    if needs_rebuild(changed_at, last_success(spark, log_path, source)):
        runs.append(build_dimension(source, f"{upstream_source}@{changed_at.isoformat()}", build, table))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils

silver_trips = read(silver.table(trips.SILVER_TABLE))
fact_path = gold.table(facts.FACT_TRIPS_TABLE)
for period in todo:
    run = IngestionRun(SOURCE, period, upstream[period])  # batch_id = the Silver batches this run consumed
    try:
        overwrite_partition(facts.build_fact_trips(silver_trips.filter(silver_trips[PERIOD_COLUMN] == period)),
                            fact_path, PERIOD_COLUMN, period)
        run.succeed(read(fact_path).filter(f"{PERIOD_COLUMN} = '{period}'").count(), silver.table(trips.SILVER_TABLE))
    except Exception as exc:
        run.fail(exc)
    append_runs(spark, log_path, [run])
    runs.append(run)

# Aggregates are keyed by the trip's own month: month M is rebuilt from the M and M+1 source files whenever
# either was (re)loaded after M's last successful aggregation (own watermark -> a failed build is retried).
AGG_SOURCE = "gold.aggregates"
fact_loaded_at = last_success_by_period(spark, log_path, SOURCE)
months = aggregates.months_to_refresh(fact_loaded_at, last_success_by_period(spark, log_path, AGG_SOURCE))
if months:
    fact_all, dim_weather = read(fact_path), read(gold.table(dimensions.DIM_WEATHER_TABLE))
for month in months:
    fed_at = max(fact_loaded_at[p] for p in aggregates.source_periods(month) if p in fact_loaded_at)
    run = IngestionRun(AGG_SOURCE, month, f"facts@{fed_at.isoformat()}")
    try:
        month_fact = aggregates.month_facts(fact_all, month)
        overwrite_partition(aggregates.build_daily_kpis(month_fact, dim_weather),
                            gold.table(aggregates.DAILY_KPIS_TABLE), aggregates.TRIP_MONTH_COLUMN, month)
        overwrite_partition(aggregates.build_station_monthly(month_fact),
                            gold.table(aggregates.STATION_MONTHLY_TABLE), aggregates.TRIP_MONTH_COLUMN, month)
        run.succeed(month_fact.count())
    except Exception as exc:
        run.fail(exc)
    append_runs(spark, log_path, [run])
    runs.append(run)

failed = [r for r in runs if r.status == "FAILED"]
for r in runs:
    print(f"{r.source:<17} {r.period}: {r.status:<9} rows={r.rows_written} {r.error or ''}")
summary = {
    "source": SOURCE,
    "processed": [r.period for r in runs if r.status == "SUCCEEDED" and r.source == SOURCE],
    "aggregated_months": [r.period for r in runs if r.status == "SUCCEEDED" and r.source == AGG_SOURCE],
    "dimensions": [r.source for r in runs if r.status == "SUCCEEDED" and r.source.startswith("gold.dim_")],
    "failed": [f"{r.source}:{r.period}" for r in failed],
    "no_silver": missing_upstream,
}
print(json.dumps(summary, indent=2))
if failed:
    raise RuntimeError(f"{len(failed)} run(s) failed: {summary['failed']} (see lh_gold.dbo.processing_log)")
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
