# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Bronze · Daily weather (Open-Meteo)
#
# Lands daily Chicago weather from the Open-Meteo archive API in `lh_bronze`:
#
# * raw API response under `Files/raw/weather/<YYYYMM>.json` (replayable)
# * `dbo.weather_daily` — one row per day, partitioned by `_period`
# * `dbo.ingestion_log` — one row per period and run
#
# Same idempotency and skip rules as `nb_bronze_divvy_trips`.

# PARAMETERS CELL ********************

periods = "last:12"  # YYYYMM | YYYYMM-YYYYMM | last:N | comma-separated
force = False        # True = reload periods that already succeeded
batch_id = ""        # set by the orchestration pipeline; generated when empty

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import uuid

import notebookutils

from divvy_medallion import lakehouse
from divvy_medallion.bronze import PERIOD_COLUMN, add_ingestion_metadata, weather
from divvy_medallion.control import IngestionRun, append_runs, succeeded_periods
from divvy_medallion.delta import overwrite_partition
from divvy_medallion.periods import parse_periods

SOURCE = weather.SOURCE
bronze = lakehouse.resolve("lh_bronze")
target_path = bronze.table(weather.BRONZE_TABLE)
log_path = bronze.table("ingestion_log")

batch_id = batch_id or str(uuid.uuid4())
force = str(force).strip().lower() in ("true", "1", "yes")
requested = parse_periods(periods)
already_loaded = set() if force else succeeded_periods(spark, log_path, SOURCE)
todo = [p for p in requested if p not in already_loaded]
print(f"batch={batch_id} requested={len(requested)} to_load={todo}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

runs = []
for period in todo:
    run = IngestionRun(SOURCE, period, batch_id)
    try:
        payload = weather.fetch(period)
        raw_file = bronze.files("raw", "weather", f"{period}.json")
        notebookutils.fs.put(raw_file, json.dumps(payload), True)

        df = add_ingestion_metadata(
            weather.to_dataframe(spark, payload), period=period, batch_id=batch_id, source_file=weather.build_url(period)
        )
        overwrite_partition(df, target_path, PERIOD_COLUMN, period)
        run.succeed(df.count(), raw_file)
    except Exception as exc:
        run.fail(exc)
    append_runs(spark, log_path, [run])
    runs.append(run)

failed = [r for r in runs if r.status == "FAILED"]
for r in runs:
    print(f"{r.period}: {r.status:<9} rows={r.rows_written} {r.error or ''}")

summary = {
    "source": SOURCE,
    "batch_id": batch_id,
    "requested": requested,
    "skipped": sorted(already_loaded & set(requested)),
    "succeeded": [r.period for r in runs if r.status == "SUCCEEDED"],
    "failed": [r.period for r in failed],
}
print(json.dumps(summary, indent=2))
if failed:
    raise RuntimeError(f"{len(failed)} period(s) failed: {summary['failed']} (see lh_bronze.dbo.ingestion_log)")
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
