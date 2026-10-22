# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Silver · Daily weather
# - Types `lh_bronze.dbo.weather_daily` (date), deduplicates by date and maps WMO codes to readable conditions
# - Writes `lh_silver.dbo.weather_daily`, partitioned by `_period`; same watermark rules as `nb_silver_trips`

# PARAMETERS CELL ********************

periods = "last:12"  # YYYYMM | YYYYMM-YYYYMM | last:N | comma-separated
force = False        # True = rebuild periods even if their latest Bronze batch was already processed

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
from divvy_medallion.bronze import PERIOD_COLUMN, weather
from divvy_medallion.control import (
    IngestionRun,
    append_runs,
    latest_succeeded_batches,
    periods_to_process,
    processed_batches,
)
from divvy_medallion.delta import overwrite_partition
from divvy_medallion.periods import parse_periods
from divvy_medallion.silver import weather as silver_weather

SOURCE = "silver.weather_daily"
bronze = lakehouse.resolve("lh_bronze")
silver = lakehouse.resolve("lh_silver")
log_path = silver.table("processing_log")

force = str(force).strip().lower() in ("true", "1", "yes")
upstream = latest_succeeded_batches(spark, bronze.table("ingestion_log"), weather.SOURCE)  # watermark per period
requested = parse_periods(periods)
todo = periods_to_process(upstream, processed_batches(spark, log_path, SOURCE), requested, force)
missing_upstream = [p for p in requested if p not in upstream]
print(f"to_process={todo} up_to_date={sorted(set(requested) & set(upstream) - set(todo))} no_bronze={missing_upstream}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils

bronze_weather = spark.read.format("delta").load(bronze.table(weather.BRONZE_TABLE))
runs = []
for period in todo:
    run = IngestionRun(SOURCE, period, upstream[period])
    try:
        df = silver_weather.transform(bronze_weather.filter(bronze_weather[PERIOD_COLUMN] == period))
        overwrite_partition(df, silver.table(silver_weather.SILVER_TABLE), PERIOD_COLUMN, period)
        run.succeed(df.count(), bronze.table(weather.BRONZE_TABLE), rows_rejected=0)
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
