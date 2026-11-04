"""Divvy (Chicago bike share) monthly trip files -> Bronze.

Source: https://divvy-tripdata.s3.amazonaws.com/<YYYYMM>-divvy-tripdata.zip (public, Lyft Bikes and
Scooters data license). All columns are kept as strings in Bronze; typing happens in Silver.
"""

from __future__ import annotations

import shutil
import urllib.error
import urllib.request
import zipfile
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from divvy_medallion.periods import add_months, validate_period

SOURCE = "divvy_trips"
BRONZE_TABLE = "divvy_trips"
BASE_URL = "https://divvy-tripdata.s3.amazonaws.com"
# Divvy publishes a month's file some days to weeks after it ends: a missing file for one of the last
# PUBLICATION_LAG_MONTHS complete months is "not published yet", not an error.
PUBLICATION_LAG_MONTHS = 2

COLUMNS: tuple[str, ...] = (
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
)


class SchemaDriftError(ValueError):
    """The source file header no longer matches the expected Divvy schema."""


class SourceNotFoundError(FileNotFoundError):
    """The monthly file does not exist (HTTP 404) at the source."""


def within_publication_lag(period: str, today: date | None = None) -> bool:
    """True for the most recent complete months, whose file may legitimately not be published yet."""
    today = today or date.today()
    current = f"{today.year:04d}{today.month:02d}"
    return add_months(current, -PUBLICATION_LAG_MONTHS) <= validate_period(period) < current


def file_name(period: str) -> str:
    return f"{validate_period(period)}-divvy-tripdata.zip"


def source_url(period: str) -> str:
    return f"{BASE_URL}/{file_name(period)}"


def download_and_extract(period: str, work_dir: Path, timeout: int = 300) -> tuple[Path, Path]:
    """Download the monthly zip and extract its CSV (ignoring macOS ``__MACOSX`` entries).

    Returns ``(zip_path, csv_path)``. Raises ``SourceNotFoundError`` when the file does not exist (HTTP 404).
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    zip_path = work_dir / file_name(period)
    try:
        response = urllib.request.urlopen(source_url(period), timeout=timeout)  # noqa: S310 - fixed https host
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise SourceNotFoundError(f"{source_url(period)} not found (HTTP 404)") from exc
        raise
    with response, zip_path.open("wb") as out:
        shutil.copyfileobj(response, out)

    with zipfile.ZipFile(zip_path) as archive:
        csv_members = [m for m in archive.namelist() if m.endswith(".csv") and not m.startswith("__MACOSX")]
        if len(csv_members) != 1:
            raise ValueError(f"{zip_path.name}: expected exactly one CSV, found {csv_members}")
        csv_path = Path(archive.extract(csv_members[0], work_dir))
    return zip_path, csv_path


def validate_header(header: list[str]) -> None:
    missing = [c for c in COLUMNS if c not in header]
    unexpected = [c for c in header if c not in COLUMNS]
    if missing or unexpected:
        raise SchemaDriftError(f"Divvy schema drift: missing={missing} unexpected={unexpected}")


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read a Divvy CSV as all-string columns, binding columns by header *name* (never by position).

    Without an explicit schema and with ``inferSchema`` off, Spark keeps every column as a string and
    names it from the header, so a reordered file still lands each value under the right column.
    """
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    validate_header(df.columns)
    return df.select(*COLUMNS)
