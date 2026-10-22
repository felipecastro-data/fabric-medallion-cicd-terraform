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
        StructField("rows_rejected", LongType(), True),
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
    rows_rejected: int | None = None
    source_file: str | None = None
    error: str | None = None
    finished_at: datetime | None = None

    def succeed(
        self, rows_written: int, source_file: str | None = None, rows_rejected: int | None = None
    ) -> IngestionRun:
        self.status, self.rows_written, self.source_file = "SUCCEEDED", rows_written, source_file
        self.rows_rejected = rows_rejected
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
    (
        spark.createDataFrame(rows, schema=INGESTION_LOG_SCHEMA)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")  # older logs predate rows_rejected
        .save(path)
    )


def succeeded_periods(spark: SparkSession, path: str, source: str) -> set[str]:
    """Periods with at least one successful run for ``source`` (empty if the log does not exist yet)."""
    try:
        log: DataFrame = spark.read.format("delta").load(path)
    except Exception:  # noqa: BLE001 - table not created yet
        return set()
    rows = log.filter((F.col("source") == source) & (F.col("status") == "SUCCEEDED")).select("period").distinct()
    return {r.period for r in rows.collect()}


def _succeeded(spark: SparkSession, path: str, source: str) -> DataFrame | None:
    try:
        log = spark.read.format("delta").load(path)
    except Exception:  # noqa: BLE001 - table not created yet
        return None
    return log.filter((F.col("source") == source) & (F.col("status") == "SUCCEEDED"))


def latest_succeeded_batches(spark: SparkSession, path: str, source: str) -> dict[str, str]:
    """Period -> batch_id of the most recent successful run (the upstream 'watermark' per period)."""
    runs = _succeeded(spark, path, source)
    if runs is None:
        return {}
    latest = runs.groupBy("period").agg(F.max_by("batch_id", "finished_at").alias("batch_id"))
    return {r.period: r.batch_id for r in latest.collect()}


def processed_batches(spark: SparkSession, path: str, source: str) -> set[tuple[str, str]]:
    """``(period, upstream batch_id)`` pairs a downstream step already processed successfully."""
    runs = _succeeded(spark, path, source)
    if runs is None:
        return set()
    return {(r.period, r.batch_id) for r in runs.select("period", "batch_id").distinct().collect()}


def periods_to_process(
    upstream: dict[str, str], processed: set[tuple[str, str]], requested: list[str], force: bool = False
) -> list[str]:
    """Requested periods whose latest upstream batch has not been processed yet (all available if ``force``)."""
    available = [p for p in requested if p in upstream]
    return available if force else [p for p in available if (p, upstream[p]) not in processed]
