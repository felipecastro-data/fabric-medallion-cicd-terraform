# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Data quality · Bronze → Silver → Gold
# - Runs the suite in `divvy_medallion.dq.suite`: keys, accepted values, ranges, referential integrity, row-count reconciliation across layers, quarantine rate and freshness
# - Per-period checks cover each requested period loaded in `fact_trips`; table-wide checks run once (period `ALL`)
# - Appends one row per check to `lh_gold.dbo.dq_results` (`outcome` = passed / warning / failed), so DQ history can be trended
# - **Gate:** any failed `error` check fails this notebook (and the pipeline run); `warn` checks only report

# PARAMETERS CELL ********************

periods = "last:12"  # YYYYMM | YYYYMM-YYYYMM | last:N | comma-separated

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import uuid
from datetime import UTC, datetime

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
from divvy_medallion.control import last_success, latest_succeeded_batches
from divvy_medallion.dq.rules import blocking_failures, run_checks
from divvy_medallion.dq.suite import DQ_RESULTS_TABLE, Tables, period_checks, results_frame, table_checks
from divvy_medallion.gold import aggregates, dimensions, facts
from divvy_medallion.periods import parse_periods
from divvy_medallion.silver import trips

bronze, silver, gold = (lakehouse.resolve(n) for n in ("lh_bronze", "lh_silver", "lh_gold"))


def read(path):
    return spark.read.format("delta").load(path)


tables = Tables(
    bronze_trips=read(bronze.table(divvy.BRONZE_TABLE)),
    silver_trips=read(silver.table(trips.SILVER_TABLE)),
    quarantine=read(silver.table(trips.QUARANTINE_TABLE)),
    fact_trips=read(gold.table(facts.FACT_TRIPS_TABLE)),
    dim_date=read(gold.table(dimensions.DIM_DATE_TABLE)),
    dim_station=read(gold.table(dimensions.DIM_STATION_TABLE)),
    dim_weather=read(gold.table(dimensions.DIM_WEATHER_TABLE)),
    daily_kpis=read(gold.table(aggregates.DAILY_KPIS_TABLE)),
    station_monthly=read(gold.table(aggregates.STATION_MONTHLY_TABLE)),
)
loaded = {r[0] for r in tables.fact_trips.select(PERIOD_COLUMN).distinct().collect()}
in_scope = [p for p in parse_periods(periods) if p in loaded]
now = datetime.now(UTC).replace(tzinfo=None)
latest_bronze = max(latest_succeeded_batches(spark, bronze.table("ingestion_log"), divvy.SOURCE), default=None)
checks = table_checks(
    tables,
    latest_bronze_period=latest_bronze,
    gold_built_at=last_success(spark, gold.table("processing_log"), "gold.fact_trips"),
    today=now.date(),
    now=now,
)
for period in in_scope:
    checks += period_checks(tables, period)
print(f"periods_in_scope={in_scope} checks={len(checks)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils

run_id = str(uuid.uuid4())
results = run_checks(checks)
results_frame(spark, run_id, now, results).write.format("delta").mode("append").save(gold.table(DQ_RESULTS_TABLE))

for r in sorted(results, key=lambda r: (r.passed, r.severity, r.check, r.period)):
    print(f"{r.outcome:<8} {r.severity:<5} {r.period:<6} {r.check:<38} observed={r.observed} expected={r.expected}"
          f"{' ' + r.details if r.details and not r.passed else ''}")
outcomes = {o: sum(r.outcome == o for r in results) for o in ("passed", "warning", "failed")}
blocking = blocking_failures(results)
summary = {
    "run_id": run_id,
    "periods": in_scope,
    **outcomes,
    "blocking": sorted({f"{r.check}@{r.period}" for r in blocking}),
}
print(json.dumps(summary, indent=2))
if blocking:
    raise RuntimeError(f"{len(blocking)} blocking DQ check(s) failed: {summary['blocking']} (see lh_gold.dbo.dq_results)")
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
