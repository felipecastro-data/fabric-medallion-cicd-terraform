"""Gold layer: star schema (``dim_*`` + ``fact_trips``) and pre-aggregated tables, built from Silver.

Surrogate keys are deterministic (date -> ``yyyyMMdd``, station -> hash of ``station_id``), so facts never
need re-keying when a dimension is rebuilt and keys are identical in every workspace.
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
