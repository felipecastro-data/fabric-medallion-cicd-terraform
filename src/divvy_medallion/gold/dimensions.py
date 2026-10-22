"""Gold dimensions: ``dim_date`` (generated calendar), ``dim_station`` and ``dim_weather`` (full rebuilds; small)."""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from divvy_medallion.gold import DOCKLESS_STATION_ID, DOCKLESS_STATION_KEY, date_key, station_key

DIM_DATE_TABLE = "dim_date"
DIM_STATION_TABLE = "dim_station"
DIM_WEATHER_TABLE = "dim_weather"

# Daily mean temperature (°C, Open-Meteo default unit) -> band; upper bounds are exclusive
TEMPERATURE_BANDS = ((0.0, "freezing"), (10.0, "cold"), (20.0, "mild"), (27.0, "warm"))


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """n-th ``weekday`` (Mon=0) of a month; ``n=-1`` is the last one."""
    if n > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    last = date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def us_holidays(year: int) -> dict[date, str]:
    """US federal holidays on their actual dates (not the observed weekday), which drive bike demand."""
    mon, thu = 0, 3
    return {
        date(year, 1, 1): "New Year's Day",
        _nth_weekday(year, 1, mon, 3): "Martin Luther King Jr. Day",
        _nth_weekday(year, 2, mon, 3): "Presidents' Day",
        _nth_weekday(year, 5, mon, -1): "Memorial Day",
        date(year, 6, 19): "Juneteenth",
        date(year, 7, 4): "Independence Day",
        _nth_weekday(year, 9, mon, 1): "Labor Day",
        _nth_weekday(year, 10, mon, 2): "Columbus Day",
        date(year, 11, 11): "Veterans Day",
        _nth_weekday(year, 11, thu, 4): "Thanksgiving Day",
        date(year, 12, 25): "Christmas Day",
    }


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    holidays = {d: name for year in range(start.year, end.year + 1) for d, name in us_holidays(year).items()}
    holiday_df = spark.createDataFrame(sorted(holidays.items()), "date date, holiday_name string")
    days = spark.createDataFrame([(start, end)], "s date, e date").select(F.explode(F.sequence("s", "e")).alias("date"))
    iso_day = (F.dayofweek("date") + 5) % 7 + 1  # Spark: Sun=1..Sat=7 -> ISO: Mon=1..Sun=7
    return (
        days.join(F.broadcast(holiday_df), "date", "left")
        .select(
            date_key(F.col("date")).alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.date_format("date", "yyyyMM").alias("period"),
            F.dayofmonth("date").alias("day"),
            iso_day.alias("day_of_week"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.weekofyear("date").alias("iso_week"),
            (iso_day >= 6).alias("is_weekend"),
            F.col("holiday_name").isNotNull().alias("is_holiday"),
            "holiday_name",
        )
        .orderBy("date_key")
    )


def build_dim_station(stations: DataFrame) -> DataFrame:
    """Silver ``stations`` plus the dockless member (key -1) so every fact row resolves to a station."""
    spark = stations.sparkSession
    members = stations.select(
        station_key(F.col("station_id")).alias("station_key"),
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "first_seen",
        "last_seen",
        F.lit(False).alias("is_dockless"),
    )
    dockless = spark.createDataFrame(
        [(DOCKLESS_STATION_KEY, DOCKLESS_STATION_ID, "Dockless (no station)", None, None, None, None, True)],
        members.schema,
    )
    return members.unionByName(dockless)


def temperature_band(mean_c: Column) -> Column:
    band = F.when(mean_c.isNull(), F.lit(None))
    for upper, name in TEMPERATURE_BANDS:
        band = band.when(mean_c < upper, F.lit(name))
    return band.otherwise(F.lit("hot"))


def build_dim_weather(weather: DataFrame) -> DataFrame:
    return weather.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        "weather_code",
        "condition",
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        temperature_band(F.col("temperature_2m_mean")).alias("temperature_band"),
        "precipitation_sum",
        "rain_sum",
        "snowfall_sum",
        "wind_speed_10m_max",
        "is_wet_day",
    )
