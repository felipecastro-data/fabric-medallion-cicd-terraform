# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Bronze · Divvy trips
# - Lands monthly Divvy trip files in `lh_bronze`; raw `.zip` and `.csv` are archived under `Files/raw/divvy/<YYYYMM>/` (replayable)
# - `dbo.divvy_trips`: all columns as strings + lineage columns, partitioned by `_period`
# - `dbo.ingestion_log`: one row per period and run (status, rows, error)
# - **Idempotent:** a period is replaced, never appended twice; periods already loaded are skipped unless `force=True`
# - Logic lives in the tested `divvy_medallion` package, shipped in the workspace environment `env_divvy`

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
import tempfile
import uuid
from pathlib import Path

import notebookutils

from divvy_medallion import lakehouse
from divvy_medallion.bronze import PERIOD_COLUMN, add_ingestion_metadata, divvy
from divvy_medallion.control import IngestionRun, append_runs, succeeded_periods
from divvy_medallion.delta import overwrite_partition
from divvy_medallion.periods import parse_periods

SOURCE = divvy.SOURCE
bronze = lakehouse.resolve("lh_bronze")
target_path = bronze.table(divvy.BRONZE_TABLE)
log_path = bronze.table("ingestion_log")

batch_id = batch_id or str(uuid.uuid4())
force = str(force).strip().lower() in ("true", "1", "yes")  # pipeline parameters arrive as strings
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
        raw_dir = bronze.files("raw", "divvy", period)
        with tempfile.TemporaryDirectory() as tmp:
            zip_path, csv_path = divvy.download_and_extract(period, Path(tmp))
            notebookutils.fs.cp(f"file:{zip_path}", f"{raw_dir}/{zip_path.name}")
            notebookutils.fs.cp(f"file:{csv_path}", f"{raw_dir}/{csv_path.name}")

        df = add_ingestion_metadata(
            divvy.read_raw(spark, f"{raw_dir}/{csv_path.name}"),
            period=period,
            batch_id=batch_id,
            source_file=divvy.source_url(period),
        )
        overwrite_partition(df, target_path, PERIOD_COLUMN, period)
        written = spark.read.format("delta").load(target_path).filter(f"{PERIOD_COLUMN} = '{period}'").count()
        run.succeed(written, divvy.source_url(period))
    except Exception as exc:  # record the failure and continue with the next period
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
