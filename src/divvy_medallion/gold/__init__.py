"""Gold layer: star schema (``dim_*`` + ``fact_trips``) and pre-aggregated tables, built from Silver.

Surrogate keys are deterministic (date -> ``yyyyMMdd``, station -> hash of ``station_id``), so facts never
need re-keying when a dimension is rebuilt and keys are identical in every workspace. ``dim_station`` keeps history
(SCD2): ``station_key`` is the durable key facts join on, ``station_version_key`` identifies one version.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

DOCKLESS_STATION_KEY = -1  # unknown member: trips that start/end outside a station
DOCKLESS_STATION_ID = "DOCKLESS"
_NON_NEGATIVE = 0x7FFFFFFFFFFFFFFF  # clears the sign bit, so -1 stays reserved for the dockless member


def date_key(date: Column) -> Column:
    return F.date_format(date, "yyyyMMdd").cast("int")


def station_key(station_id: Column) -> Column:
    return F.when(station_id.isNull(), F.lit(DOCKLESS_STATION_KEY).cast("long")).otherwise(
        F.xxhash64(station_id).bitwiseAND(F.lit(_NON_NEGATIVE))
    )


def station_version_key(station_id: Column, valid_from: Column) -> Column:
    """Key of one SCD2 version of a station (the dockless member has a single version, key -1)."""
    is_dockless = station_id.isNull() | (station_id == DOCKLESS_STATION_ID)
    return F.when(is_dockless, F.lit(DOCKLESS_STATION_KEY).cast("long")).otherwise(
        F.xxhash64(station_id, valid_from).bitwiseAND(F.lit(_NON_NEGATIVE))
    )
