"""Gold dimensions: ``dim_date`` (generated calendar), ``dim_station`` (SCD2, see ADR 0004) and ``dim_weather``.

``dim_date`` and ``dim_weather`` are small full rebuilds. ``dim_station`` keeps history: each run compares the Silver
``stations`` snapshot with the current versions (snapshot-diff change detection) and MERGEs the changes.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from divvy_medallion.gold import (
    DOCKLESS_STATION_ID,
    DOCKLESS_STATION_KEY,
    date_key,
    station_key,
    station_version_key,
)
from divvy_medallion.silver.trips import haversine_km

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
    """Current snapshot of the station members: Silver ``stations`` plus the dockless member (key -1), so every fact
    row resolves to a station. ``merge_dim_station`` turns successive snapshots into SCD2 history."""
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


# --- dim_station history (SCD2) -------------------------------------------------------------------------------------

# The first version of every station is valid from the beginning of time, so as-of joins resolve every trip.
SCD2_BEGINNING = datetime(1900, 1, 1)
SCD2_OPEN_END = datetime(9999, 12, 31)
# Type 2 (new version): a new name, or the station moved more than this. Smaller coordinate changes are the median
# of GPS points shifting as months are added, so they are type 1 (overwritten in place), like first/last seen.
RELOCATION_KM = 0.1
TYPE1_COLUMNS = ("latitude", "longitude", "first_seen", "last_seen")
DIM_STATION_COLUMNS = (
    "station_version_key",
    "station_key",
    "station_id",
    "station_name",
    "latitude",
    "longitude",
    "first_seen",
    "last_seen",
    "is_dockless",
    "valid_from",
    "valid_to",
    "is_current",
    "change_reason",
)


def _versions(members: DataFrame, valid_from: Column, change_reason: Column) -> DataFrame:
    """Snapshot members as open, current versions."""
    return members.select(
        station_version_key(F.col("station_id"), valid_from).alias("station_version_key"),
        "station_key",
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "first_seen",
        "last_seen",
        "is_dockless",
        valid_from.alias("valid_from"),
        F.lit(SCD2_OPEN_END).alias("valid_to"),
        F.lit(True).alias("is_current"),
        change_reason.alias("change_reason"),
    )


def initial_dim_station(snapshot: DataFrame) -> DataFrame:
    """First load: one open version per station, valid since ``SCD2_BEGINNING``."""
    return _versions(snapshot, F.lit(SCD2_BEGINNING), F.lit("initial"))


def _staged(df: DataFrame, action: str, merge_key: Column) -> DataFrame:
    return df.withColumn("action", F.lit(action)).withColumn("merge_key", merge_key)


def scd2_station_changes(current: DataFrame, snapshot: DataFrame, effective_at: datetime) -> DataFrame:
    """Staged rows for the ``dim_station`` MERGE: ``DIM_STATION_COLUMNS`` plus ``merge_key`` and ``action``.

    * ``close`` (merge_key = current version key): a renamed or relocated station's current version ends at
      ``effective_at``; an ``insert`` row (merge_key null) opens its new version from ``effective_at``.
    * ``insert`` for a station not seen before: valid since ``SCD2_BEGINNING`` (its trips predate this run).
    * ``type1`` (merge_key = current version key): only coordinates drift or first/last seen changed.

    Stations missing from the snapshot keep their current version (history is never deleted).
    """
    cur = current.filter(F.col("is_current")).alias("c")
    snap = snapshot.alias("s")
    joined = snap.join(cur, F.col("s.station_key") == F.col("c.station_key"), "left")

    is_new = F.col("c.station_key").isNull()
    renamed = ~F.col("s.station_name").eqNullSafe(F.col("c.station_name"))
    moved_km = haversine_km(F.col("c.latitude"), F.col("c.longitude"), F.col("s.latitude"), F.col("s.longitude"))
    relocated = F.coalesce(moved_km > RELOCATION_KM, F.lit(False))
    reason = F.concat_ws("+", F.when(renamed, F.lit("renamed")), F.when(relocated, F.lit("relocated")))
    type1 = F.lit(False)
    for column in TYPE1_COLUMNS:
        type1 = type1 | ~F.col(f"s.{column}").eqNullSafe(F.col(f"c.{column}"))

    classified = joined.select(
        "s.*",
        F.col("c.station_version_key").alias("_current_key"),
        F.when(is_new, F.lit("new")).when(renamed | relocated, reason).when(type1, F.lit("type1")).alias("_change"),
    ).filter(F.col("_change").isNotNull())

    new_stations = _versions(classified.filter(F.col("_change") == "new"), F.lit(SCD2_BEGINNING), F.lit("new"))
    versioned = classified.filter(~F.col("_change").isin("new", "type1"))
    new_versions = _versions(versioned, F.lit(effective_at), F.col("_change"))
    closes = (
        current.filter(F.col("is_current"))
        .join(versioned.select(F.col("_current_key").alias("station_version_key")), "station_version_key")
        .withColumn("valid_to", F.lit(effective_at))
        .withColumn("is_current", F.lit(False))
        .select(*DIM_STATION_COLUMNS)
    )
    type1_rows = (
        current.filter(F.col("is_current"))
        .drop(*TYPE1_COLUMNS)
        .join(
            classified.filter(F.col("_change") == "type1").select(
                F.col("_current_key").alias("station_version_key"), *TYPE1_COLUMNS
            ),
            "station_version_key",
        )
        .select(*DIM_STATION_COLUMNS)
    )
    return (
        _staged(new_stations, "insert", F.lit(None).cast("long"))
        .unionByName(_staged(new_versions, "insert", F.lit(None).cast("long")))
        .unionByName(_staged(closes, "close", F.col("station_version_key")))
        .unionByName(_staged(type1_rows, "type1", F.col("station_version_key")))
    )


def needs_scd2_migration(spark: SparkSession, path: str) -> bool:
    """True when ``dim_station`` does not exist yet or still has the pre-SCD2 (type 1) schema."""
    try:
        columns = spark.read.format("delta").load(path).columns
    except Exception:  # noqa: BLE001 - table not created yet
        return True
    return "is_current" not in columns


def merge_dim_station(spark: SparkSession, snapshot: DataFrame, path: str, effective_at: datetime) -> dict[str, int]:
    """Apply one snapshot to ``dim_station`` (SCD2). Returns the number of staged rows per action.

    A missing or type 1 table is (re)initialised: the type 1 table held no history, so nothing is lost.
    """
    from delta.tables import DeltaTable

    if needs_scd2_migration(spark, path):
        initial = initial_dim_station(snapshot)
        initial.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        return {"initial": spark.read.format("delta").load(path).count()}

    target = DeltaTable.forPath(spark, path)
    changes = scd2_station_changes(target.toDF(), snapshot, effective_at).localCheckpoint()  # stable while merging
    counts = {r.action: r["count"] for r in changes.groupBy("action").count().collect()}
    if not counts:
        return {}
    insert_values = {c: F.col(f"s.{c}") for c in DIM_STATION_COLUMNS}
    (
        target.alias("t")
        .merge(changes.alias("s"), "t.station_version_key = s.merge_key")
        .whenMatchedUpdate(condition="s.action = 'close'", set={"valid_to": "s.valid_to", "is_current": "false"})
        .whenMatchedUpdate(condition="s.action = 'type1'", set={c: f"s.{c}" for c in TYPE1_COLUMNS})
        .whenNotMatchedInsert(condition="s.action = 'insert'", values=insert_values)
        .execute()
    )
    return counts


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
