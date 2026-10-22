import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("divvy-medallion-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


BRONZE_TRIP_COLUMNS = [
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
    "_source_file",
    "_batch_id",
    "_ingested_at",
    "_period",
]
S1 = {"name": "Clark & Elm", "id": "S1", "lat": "41.90", "lng": "-87.63"}
S2 = {"name": "Wells", "id": "S2", "lat": "41.91", "lng": "-87.63"}
DOCKLESS = {"name": None, "id": None, "lat": "41.95", "lng": "-87.70"}


def _trip(ride_id, rideable_type, started, ended, start, end, member):
    return {
        "ride_id": ride_id,
        "rideable_type": rideable_type,
        "started_at": started,
        "ended_at": ended,
        **{f"start_{k if k in ('lat', 'lng') else 'station_' + k}": v for k, v in start.items()},
        **{f"end_{k if k in ('lat', 'lng') else 'station_' + k}": v for k, v in end.items()},
        "member_casual": member,
    }


BRONZE_TRIPS = [
    _trip("R1", "classic_bike", "2026-09-01 08:00:00", "2026-09-01 08:20:00", S1, S2, "member"),
    _trip("R2", "electric_bike", "2026-09-01 09:00:00", "2026-09-01 09:30:00", DOCKLESS, S1, "casual"),
    _trip("R3", "electric_bike", "2026-09-02 17:00:00", "2026-09-02 17:10:00", S2, S2, "member"),
    _trip("R4", "classic_bike", "2026-09-02 18:00:00", "2026-09-02 18:00:30", S2, S1, "member"),  # < 60 s
    # commute filler on 2026-09-03 keeps the quarantine rate realistic (2 of 45 rows)
    *(
        _trip(f"F{i:02d}", "classic_bike", "2026-09-03 08:00:00", "2026-09-03 08:15:00", S1, S2, "member")
        for i in range(40)
    ),
]


@pytest.fixture(scope="session")
def medallion(spark):
    """A tiny, internally consistent Bronze -> Silver -> Gold dataset built with the real transforms."""
    from datetime import date, datetime

    from divvy_medallion.dq.suite import Tables
    from divvy_medallion.gold import aggregates, dimensions, facts
    from divvy_medallion.silver import stations, trips, weather

    def frozen(df):  # materialise, so every test does not re-run the whole lineage
        return spark.createDataFrame(df.collect(), df.schema)

    lineage = {"_source_file": "202609.zip", "_batch_id": "b1", "_ingested_at": datetime(2026, 10, 1)}
    rows = [trip | lineage | {"_period": "202609"} for trip in BRONZE_TRIPS]
    rows.append(rows[0] | {"_batch_id": "b0", "_ingested_at": datetime(2026, 9, 30)})  # older copy of R1
    schema = ", ".join(f"{c} {'timestamp' if c == '_ingested_at' else 'string'}" for c in BRONZE_TRIP_COLUMNS)
    bronze = frozen(spark.createDataFrame([tuple(r[c] for c in BRONZE_TRIP_COLUMNS) for r in rows], schema))

    weather_schema = (
        "date string, weather_code int, temperature_2m_max double, temperature_2m_min double, "
        "temperature_2m_mean double, precipitation_sum double, rain_sum double, snowfall_sum double, "
        "wind_speed_10m_max double, _batch_id string, _ingested_at timestamp, _period string"
    )
    weather_rows = [
        ("2026-09-01", 0, 25.0, 15.0, 20.5, 0.0, 0.0, 0.0, 12.0, "w1", datetime(2026, 10, 1), "202609"),
        ("2026-09-02", 63, 18.0, 10.0, 14.0, 6.5, 6.5, 0.0, 25.0, "w1", datetime(2026, 10, 1), "202609"),
        ("2026-09-03", 3, 22.0, 14.0, 18.0, 0.0, 0.0, 0.0, 15.0, "w1", datetime(2026, 10, 1), "202609"),
    ]
    silver_weather = frozen(weather.transform(spark.createDataFrame(weather_rows, weather_schema)))

    valid, quarantine = (frozen(df) for df in trips.transform(bronze))
    dim_weather = frozen(dimensions.build_dim_weather(silver_weather))
    fact = frozen(facts.build_fact_trips(valid))
    return Tables(
        bronze_trips=bronze,
        silver_trips=valid,
        quarantine=quarantine,
        fact_trips=fact,
        dim_date=frozen(dimensions.build_dim_date(spark, date(2026, 1, 1), date(2026, 12, 31))),
        dim_station=frozen(dimensions.build_dim_station(stations.build_stations(valid))),
        dim_weather=dim_weather,
        daily_kpis=frozen(aggregates.build_daily_kpis(fact, dim_weather)),
        station_monthly=frozen(aggregates.build_station_monthly(fact)),
    )
