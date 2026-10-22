"""Bronze ``divvy_trips`` -> Silver ``trips`` + ``trips_quarantine``.

Quality rules (a row may fail several; every failure is recorded in ``dq_reasons``):

=========================== =================================================================
``missing_ride_id``         ``ride_id`` is null/blank
``invalid_timestamp``       ``started_at`` or ``ended_at`` cannot be parsed
``non_positive_duration``   trip ends at or before it starts
``duration_under_60s``      false start / re-dock (Divvy's own published cleaning rule)
``duration_over_24h``       bike not returned (lost, stolen or maintenance)
``missing_end_coordinates`` no end location recorded
``coordinates_out_of_range`` start/end outside the Chicago service area
``unknown_rideable_type``   value outside the known bike types
``unknown_member_type``     value other than member/casual
``duplicate_ride_id``       a newer ingestion of the same ``ride_id`` was kept (applied after the rules above)
=========================== =================================================================

Missing station IDs are *not* a failure: dockless e-bike trips legitimately start/end outside a station.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from divvy_medallion.bronze import PERIOD_COLUMN

SILVER_TABLE = "trips"
QUARANTINE_TABLE = "trips_quarantine"

# Chicago service area (generous bounding box around the Divvy network)
LAT_RANGE = (41.60, 42.10)
LNG_RANGE = (-88.00, -87.50)
RIDEABLE_TYPES = ("classic_bike", "electric_bike", "electric_scooter")
MEMBER_TYPES = ("member", "casual")
MIN_DURATION_S = 60
MAX_DURATION_S = 24 * 60 * 60
EARTH_RADIUS_KM = 6371.0

SILVER_COLUMNS = (
    "ride_id",
    "rideable_type",
    "member_type",
    "started_at",
    "ended_at",
    "trip_date",
    "start_hour",
    "day_of_week",
    "duration_s",
    "start_station_id",
    "start_station_name",
    "end_station_id",
    "end_station_name",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "distance_km",
    "is_round_trip",
    "is_dockless",
    "_source_file",
    "_batch_id",
    "_ingested_at",
    PERIOD_COLUMN,
)


def _clean_str(column: str) -> Column:
    """Trim; blank strings become null."""
    trimmed = F.trim(F.col(column))
    return F.when(trimmed == "", None).otherwise(trimmed)


def _timestamp(column: str) -> Column:
    # Divvy publishes local (America/Chicago) wall-clock times, with or without milliseconds
    value = _clean_str(column)
    return F.coalesce(F.to_timestamp(value, "yyyy-MM-dd HH:mm:ss.SSS"), F.to_timestamp(value, "yyyy-MM-dd HH:mm:ss"))


def haversine_km(lat1: Column, lng1: Column, lat2: Column, lng2: Column) -> Column:
    dlat, dlng = F.radians(lat2 - lat1), F.radians(lng2 - lng1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlng / 2) ** 2
    return F.round(F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a)), 3)


def standardize(bronze: DataFrame) -> DataFrame:
    """Type and normalise Bronze columns and add derived attributes (no rows removed)."""
    df = bronze.select(
        _clean_str("ride_id").alias("ride_id"),
        F.lower(_clean_str("rideable_type")).alias("rideable_type"),
        F.lower(_clean_str("member_casual")).alias("member_type"),
        _timestamp("started_at").alias("started_at"),
        _timestamp("ended_at").alias("ended_at"),
        F.upper(_clean_str("start_station_id")).alias("start_station_id"),
        _clean_str("start_station_name").alias("start_station_name"),
        F.upper(_clean_str("end_station_id")).alias("end_station_id"),
        _clean_str("end_station_name").alias("end_station_name"),
        *[_clean_str(c).cast("double").alias(c) for c in ("start_lat", "start_lng", "end_lat", "end_lng")],
        "_source_file",
        "_batch_id",
        "_ingested_at",
        PERIOD_COLUMN,
    )
    return (
        # millisecond precision: 59.9 s must still count as "under 60 s" and 24 h + 0.5 s as "over 24 h"
        df.withColumn("duration_s", (F.unix_micros("ended_at") - F.unix_micros("started_at")) / F.lit(1_000_000.0))
        .withColumn("trip_date", F.to_date("started_at"))
        .withColumn("start_hour", F.hour("started_at"))
        .withColumn("day_of_week", F.date_format("started_at", "E"))
        .withColumn(
            "distance_km", haversine_km(F.col("start_lat"), F.col("start_lng"), F.col("end_lat"), F.col("end_lng"))
        )
        .withColumn(
            "is_round_trip",
            F.coalesce(F.col("start_station_id") == F.col("end_station_id"), F.lit(False)),
        )
        .withColumn("is_dockless", F.col("start_station_id").isNull() | F.col("end_station_id").isNull())
    )


def _in_range(column: str, low: float, high: float) -> Column:
    return F.col(column).isNull() | F.col(column).between(low, high)


def quality_rules() -> dict[str, Column]:
    """Rule name -> boolean Column that is True when the row FAILS the rule."""
    has_times = F.col("started_at").isNotNull() & F.col("ended_at").isNotNull()
    return {
        "missing_ride_id": F.col("ride_id").isNull(),
        "invalid_timestamp": ~has_times,
        "non_positive_duration": has_times & (F.col("duration_s") <= 0),
        "duration_under_60s": has_times & (F.col("duration_s") > 0) & (F.col("duration_s") < MIN_DURATION_S),
        "duration_over_24h": has_times & (F.col("duration_s") > MAX_DURATION_S),
        "missing_end_coordinates": F.col("end_lat").isNull() | F.col("end_lng").isNull(),
        "coordinates_out_of_range": ~(
            _in_range("start_lat", *LAT_RANGE)
            & _in_range("end_lat", *LAT_RANGE)
            & _in_range("start_lng", *LNG_RANGE)
            & _in_range("end_lng", *LNG_RANGE)
        ),
        "unknown_rideable_type": ~F.coalesce(F.col("rideable_type").isin(*RIDEABLE_TYPES), F.lit(False)),
        "unknown_member_type": ~F.coalesce(F.col("member_type").isin(*MEMBER_TYPES), F.lit(False)),
    }


def apply_quality_rules(df: DataFrame) -> DataFrame:
    """Add ``dq_reasons`` (array of failed rule names, empty when the row is valid)."""
    failures = [F.when(failed, F.lit(name)) for name, failed in quality_rules().items()]
    return df.withColumn("dq_reasons", F.filter(F.array(*failures), lambda reason: reason.isNotNull()))


def flag_duplicates(df: DataFrame) -> DataFrame:
    """Append ``duplicate_ride_id`` to every version of a ``ride_id`` except the most recently ingested one,
    so superseded rows are quarantined instead of silently dropped. Expects rule-passing rows only."""
    latest_first = Window.partitionBy("ride_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    superseded = F.row_number().over(latest_first) > 1
    return df.withColumn(
        "dq_reasons",
        F.when(superseded, F.array_union("dq_reasons", F.array(F.lit("duplicate_ride_id")))).otherwise(
            F.col("dq_reasons")
        ),
    )


def transform(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return ``(valid, quarantine)``: one valid row per ``ride_id``; every other Bronze row is quarantined
    with its reasons, so ``valid + quarantine == bronze`` row for row."""
    checked = apply_quality_rules(standardize(bronze))
    passing = checked.filter(F.size("dq_reasons") == 0)
    failing = checked.filter(F.size("dq_reasons") > 0)
    deduped = flag_duplicates(passing)
    is_valid = F.size("dq_reasons") == 0
    valid = deduped.filter(is_valid).select(*SILVER_COLUMNS)
    quarantine = failing.unionByName(deduped.filter(~is_valid)).withColumn("_quarantined_at", F.current_timestamp())
    return valid, quarantine
