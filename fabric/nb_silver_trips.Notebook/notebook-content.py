# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Silver · Trips
# - Types and standardises `lh_bronze.dbo.divvy_trips`, adds duration, calendar, distance and round-trip/dockless flags
# - Applies the quality rules in `divvy_medallion.silver.trips`; failing rows go to `dbo.trips_quarantine` with `dq_reasons` (nothing is dropped)
# - Deduplicates on `ride_id` (latest ingestion wins) into `dbo.trips`, partitioned by `_period`
# - Rebuilds the `dbo.stations` dimension from all Silver trips
# - **Incremental:** a period is rebuilt only when Bronze has a newer successful batch for it (watermark in `dbo.processing_log`)
# - **Breaking schema change:** run once with `rebuild=True` and `periods` covering every loaded month (drops and reloads `trips` + `trips_quarantine`)

# PARAMETERS CELL ********************

periods = "last:12"  # YYYYMM | YYYYMM-YYYYMM | last:N | comma-separated
force = False        # True = rebuild periods even if their latest Bronze batch was already processed
rebuild = False      # True = DROP trips + trips_quarantine first (breaking schema change); implies force

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

spark.conf.set("spark.sql.session.timeZone", "UTC")  # Divvy timestamps are Chicago wall-clock; keep them as-is

try:
    import divvy_medallion  # noqa: F401
except ModuleNotFoundError as exc:  # fail fast with an actionable message
    raise ModuleNotFoundError(
        "divvy_medallion is not installed in this Spark session: set env_divvy as the workspace default "
        "environment (Workspace settings > Spark settings > Environment) or attach it to this notebook"
    ) from exc

from divvy_medallion import lakehouse
from divvy_medallion.bronze import PERIOD_COLUMN, divvy
from divvy_medallion.control import (
    IngestionRun,
    append_runs,
    last_success,
    latest_succeeded_batches,
    needs_rebuild,
    periods_to_process,
    processed_batches,
    rebuilt_at,
    record_rebuild,
)
from divvy_medallion.delta import overwrite_partition
from divvy_medallion.periods import parse_periods
from divvy_medallion.silver import stations, trips

SOURCE = "silver.trips"
bronze = lakehouse.resolve("lh_bronze")
silver = lakehouse.resolve("lh_silver")
log_path = silver.table("processing_log")

force = str(force).strip().lower() in ("true", "1", "yes")
rebuild = str(rebuild).strip().lower() in ("true", "1", "yes")
if rebuild:  # explicit, logged migration path; partial overwrites never change column types silently
    import notebookutils

    record_rebuild(spark, log_path, SOURCE)  # marker first: pre-rebuild checkpoints stop counting
    for table in (trips.SILVER_TABLE, trips.QUARANTINE_TABLE):
        if notebookutils.fs.exists(silver.table(table)):
            notebookutils.fs.rm(silver.table(table), True)
            print(f"rebuild: dropped {table}")
    force = True
upstream = latest_succeeded_batches(spark, bronze.table("ingestion_log"), divvy.SOURCE)  # watermark per period
requested = parse_periods(periods)
since = rebuilt_at(spark, log_path, SOURCE)  # checkpoints older than the last rebuild describe dropped data
todo = periods_to_process(upstream, processed_batches(spark, log_path, SOURCE, since), requested, force)
missing_upstream = [p for p in requested if p not in upstream]
print(f"to_process={todo} up_to_date={sorted(set(requested) & set(upstream) - set(todo))} no_bronze={missing_upstream}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils

bronze_trips = spark.read.format("delta").load(bronze.table(divvy.BRONZE_TABLE))
runs = []
for period in todo:
    run = IngestionRun(SOURCE, period, upstream[period])  # batch_id = the Bronze batch this run consumed
    try:
        valid, quarantine = trips.transform(bronze_trips.filter(bronze_trips[PERIOD_COLUMN] == period))
        overwrite_partition(valid, silver.table(trips.SILVER_TABLE), PERIOD_COLUMN, period)
        overwrite_partition(quarantine, silver.table(trips.QUARANTINE_TABLE), PERIOD_COLUMN, period)
        written = spark.read.format("delta").load(silver.table(trips.SILVER_TABLE)).filter(f"{PERIOD_COLUMN} = '{period}'")
        rejected = spark.read.format("delta").load(silver.table(trips.QUARANTINE_TABLE)).filter(f"{PERIOD_COLUMN} = '{period}'")
        run.succeed(written.count(), bronze.table(divvy.BRONZE_TABLE), rows_rejected=rejected.count())
    except Exception as exc:
        run.fail(exc)
    append_runs(spark, log_path, [run])
    runs.append(run)

# Stations is rebuilt whenever trips changed after its own last successful build (own watermark):
# a failed rebuild is repaired by a normal retry even when every period is already processed.
STATIONS_SOURCE = "silver.stations"
trips_changed_at = last_success(spark, log_path, SOURCE)
if needs_rebuild(trips_changed_at, last_success(spark, log_path, STATIONS_SOURCE)):
    run = IngestionRun(STATIONS_SOURCE, "ALL", f"trips@{trips_changed_at.isoformat()}")
    try:
        all_trips = spark.read.format("delta").load(silver.table(trips.SILVER_TABLE))
        stations.build_stations(all_trips).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(silver.table(stations.STATIONS_TABLE))
        run.succeed(spark.read.format("delta").load(silver.table(stations.STATIONS_TABLE)).count())
    except Exception as exc:
        run.fail(exc)
    append_runs(spark, log_path, [run])
    runs.append(run)

failed = [r for r in runs if r.status == "FAILED"]
for r in runs:
    print(f"{r.period}: {r.status:<9} valid={r.rows_written} rejected={r.rows_rejected} {r.error or ''}")
summary = {
    "source": SOURCE,
    "processed": [r.period for r in runs if r.status == "SUCCEEDED"],
    "failed": [r.period for r in failed],
    "no_bronze": missing_upstream,
}
print(json.dumps(summary, indent=2))
if failed:
    raise RuntimeError(f"{len(failed)} period(s) failed: {summary['failed']} (see lh_silver.dbo.processing_log)")
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
