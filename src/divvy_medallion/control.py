"""Ingestion control table: one row per (source, period, batch) run, used for auditing and skipping
periods that were already loaded successfully."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

INGESTION_LOG_SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("period", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("rows_written", LongType(), True),
        StructField("source_file", StringType(), True),
        StructField("error", StringType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
    ]
)


@dataclass
class IngestionRun:
    source: str
    period: str
    batch_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC).replace(tzinfo=None))
    status: str = "RUNNING"
    rows_written: int | None = None
    source_file: str | None = None
    error: str | None = None
    finished_at: datetime | None = None

    def succeed(self, rows_written: int, source_file: str | None = None) -> IngestionRun:
        self.status, self.rows_written, self.source_file = "SUCCEEDED", rows_written, source_file
        self.finished_at = datetime.now(UTC).replace(tzinfo=None)
        return self

    def fail(self, error: BaseException) -> IngestionRun:
        self.status, self.error = "FAILED", f"{type(error).__name__}: {error}"[:2000]
        self.finished_at = datetime.now(UTC).replace(tzinfo=None)
        return self


def append_runs(spark: SparkSession, path: str, runs: list[IngestionRun]) -> None:
    if not runs:
        return
    rows = [{k: v for k, v in asdict(r).items()} for r in runs]
    spark.createDataFrame(rows, schema=INGESTION_LOG_SCHEMA).write.format("delta").mode("append").save(path)


def succeeded_periods(spark: SparkSession, path: str, source: str) -> set[str]:
    """Periods with at least one successful run for ``source`` (empty if the log does not exist yet)."""
    try:
        log: DataFrame = spark.read.format("delta").load(path)
    except Exception:  # noqa: BLE001 - table not created yet
        return set()
    rows = log.filter((F.col("source") == source) & (F.col("status") == "SUCCEEDED")).select("period").distinct()
    return {r.period for r in rows.collect()}
