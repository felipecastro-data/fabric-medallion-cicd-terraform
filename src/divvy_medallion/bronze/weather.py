"""Open-Meteo historical daily weather for Chicago -> Bronze.

Source: https://open-meteo.com (free, CC BY 4.0). One API call per period; the raw JSON response is
kept in Files for replay, and a flattened daily table is written to Bronze.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from divvy_medallion.periods import period_bounds

SOURCE = "weather_daily"
BRONZE_TABLE = "weather_daily"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
CHICAGO = {"latitude": 41.8781, "longitude": -87.6298, "timezone": "America/Chicago"}

DAILY_VARIABLES: tuple[str, ...] = (
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
)

SCHEMA = StructType(
    [StructField("date", StringType(), False), StructField("weather_code", IntegerType(), True)]
    + [StructField(v, DoubleType(), True) for v in DAILY_VARIABLES if v != "weather_code"]
    + [StructField("latitude", DoubleType(), True), StructField("longitude", DoubleType(), True)]
)


def build_url(period: str, location: dict | None = None) -> str:
    start, end = period_bounds(period)
    params = {
        **(location or CHICAGO),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
    }
    return f"{ARCHIVE_URL}?{urllib.parse.urlencode(params)}"


def fetch(period: str, timeout: int = 60) -> dict:
    with urllib.request.urlopen(build_url(period), timeout=timeout) as response:  # noqa: S310 - fixed https host
        return json.load(response)


def to_rows(payload: dict) -> list[dict]:
    """Flatten Open-Meteo's column-oriented ``daily`` block into one dict per day."""
    daily = payload.get("daily")
    if not daily or "time" not in daily:
        raise ValueError("Open-Meteo response has no 'daily.time' block")
    missing = [v for v in DAILY_VARIABLES if v not in daily]
    if missing:
        raise ValueError(f"Open-Meteo response is missing daily variables: {missing}")

    rows = []
    for i, day in enumerate(daily["time"]):
        row = {"date": day, "latitude": payload.get("latitude"), "longitude": payload.get("longitude")}
        for variable in DAILY_VARIABLES:
            value = daily[variable][i]
            row[variable] = int(value) if variable == "weather_code" and value is not None else value
        rows.append(row)
    return rows


def to_dataframe(spark: SparkSession, payload: dict) -> DataFrame:
    rows = to_rows(payload)
    for row in rows:
        for variable in DAILY_VARIABLES:
            if variable != "weather_code" and row[variable] is not None:
                row[variable] = float(row[variable])
    return spark.createDataFrame(rows, schema=SCHEMA)
