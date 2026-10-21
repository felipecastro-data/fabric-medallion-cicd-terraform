"""Divvy (Chicago bike share) monthly trip files -> Bronze.

Source: https://divvy-tripdata.s3.amazonaws.com/<YYYYMM>-divvy-tripdata.zip (public, Lyft Bikes and
Scooters data license). All columns are kept as strings in Bronze; typing happens in Silver.
"""

from __future__ import annotations

import shutil
import urllib.request
import zipfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from divvy_medallion.periods import validate_period

SOURCE = "divvy_trips"
BRONZE_TABLE = "divvy_trips"
BASE_URL = "https://divvy-tripdata.s3.amazonaws.com"

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
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in COLUMNS])


class SchemaDriftError(ValueError):
    """The source file header no longer matches the expected Divvy schema."""


def file_name(period: str) -> str:
    return f"{validate_period(period)}-divvy-tripdata.zip"


def source_url(period: str) -> str:
    return f"{BASE_URL}/{file_name(period)}"


def download_and_extract(period: str, work_dir: Path, timeout: int = 300) -> tuple[Path, Path]:
    """Download the monthly zip and extract its CSV (ignoring macOS ``__MACOSX`` entries).

    Returns ``(zip_path, csv_path)``.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    zip_path = work_dir / file_name(period)
    with urllib.request.urlopen(source_url(period), timeout=timeout) as response, zip_path.open("wb") as out:  # noqa: S310 - fixed https host
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
    """Read a Divvy CSV as all-string columns after checking the header for schema drift."""
    header = spark.read.option("header", True).csv(path).columns
    validate_header(header)
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(path)
        .select(*COLUMNS)
    )
