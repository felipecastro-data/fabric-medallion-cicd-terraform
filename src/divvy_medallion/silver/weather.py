"""Bronze ``weather_daily`` -> Silver ``weather_daily`` (typed date, deduplicated, readable conditions)."""

from __future__ import annotations

from itertools import chain

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from divvy_medallion.bronze import PERIOD_COLUMN

SILVER_TABLE = "weather_daily"

# WMO weather interpretation codes used by Open-Meteo, grouped into analysis-friendly conditions
WMO_CONDITIONS: dict[int, str] = {
    0: "clear",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "fog",
    **{code: "drizzle" for code in (51, 53, 55, 56, 57)},
    **{code: "rain" for code in (61, 63, 65, 66, 67, 80, 81, 82)},
    **{code: "snow" for code in (71, 73, 75, 77, 85, 86)},
    **{code: "thunderstorm" for code in (95, 96, 99)},
}


def transform(bronze: DataFrame) -> DataFrame:
    condition_map = F.create_map(*[F.lit(x) for x in chain(*WMO_CONDITIONS.items())])
    latest_first = Window.partitionBy("date").orderBy(F.col("_ingested_at").desc())
    return (
        bronze.withColumn("date", F.to_date("date"))
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter("_rn = 1")
        .select(
            "date",
            "weather_code",
            F.coalesce(condition_map[F.col("weather_code")], F.lit("unknown")).alias("condition"),
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "wind_speed_10m_max",
            (F.coalesce(F.col("precipitation_sum"), F.lit(0.0)) >= 1.0).alias("is_wet_day"),
            "_batch_id",
            "_ingested_at",
            PERIOD_COLUMN,
        )
    )
