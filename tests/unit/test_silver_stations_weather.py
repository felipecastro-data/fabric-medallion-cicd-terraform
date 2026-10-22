from datetime import date, datetime

from divvy_medallion.control import periods_to_process
from divvy_medallion.silver import stations, weather


def test_build_stations_picks_most_frequent_name_and_median_location(spark):
    schema = (
        "start_station_id string, start_station_name string, start_lat double, start_lng double, "
        "end_station_id string, end_station_name string, end_lat double, end_lng double, trip_date date"
    )
    rows = [
        ("S1", "Clark & Elm", 41.900, -87.630, "S2", "Wells", 41.910, -87.634, date(2026, 9, 1)),
        ("S1", "Clark & Elm", 41.902, -87.632, "S2", "Wells", 41.912, -87.636, date(2026, 9, 5)),
        ("S1", "Clark St & Elm St", 41.901, -87.631, None, None, 41.95, -87.70, date(2026, 9, 9)),
    ]
    result = {r.station_id: r for r in stations.build_stations(spark.createDataFrame(rows, schema)).collect()}

    assert set(result) == {"S1", "S2"}  # dockless endpoint is not a station
    s1 = result["S1"]
    assert s1.station_name == "Clark & Elm"
    assert s1.name_variants == 2
    assert s1.latitude == 41.901
    assert (s1.first_seen, s1.last_seen, s1.trip_endpoints) == (date(2026, 9, 1), date(2026, 9, 9), 3)


def test_weather_transform_maps_conditions_and_dedups_by_date(spark):
    schema = (
        "date string, weather_code int, temperature_2m_max double, temperature_2m_min double, "
        "temperature_2m_mean double, precipitation_sum double, rain_sum double, snowfall_sum double, "
        "wind_speed_10m_max double, _batch_id string, _ingested_at timestamp, _period string"
    )
    rows = [
        ("2026-09-01", 63, 20.0, 12.0, 16.0, 5.2, 5.2, 0.0, 30.0, "b1", datetime(2026, 10, 1), "202609"),
        ("2026-09-01", 0, 21.0, 12.0, 16.5, 0.0, 0.0, 0.0, 10.0, "b2", datetime(2026, 10, 2), "202609"),
        ("2026-09-02", 1234, 22.0, 13.0, 17.0, None, None, None, 9.0, "b2", datetime(2026, 10, 2), "202609"),
    ]
    result = {r.date: r for r in weather.transform(spark.createDataFrame(rows, schema)).collect()}

    assert len(result) == 2
    assert (result[date(2026, 9, 1)].condition, result[date(2026, 9, 1)].is_wet_day) == ("clear", False)
    assert (result[date(2026, 9, 2)].condition, result[date(2026, 9, 2)].is_wet_day) == ("unknown", False)


def test_periods_to_process_follows_upstream_watermark():
    upstream = {"202607": "b1", "202608": "b2", "202609": "b3"}
    processed = {("202607", "b1"), ("202608", "b1")}  # 202608 was re-ingested since (b2)
    requested = ["202606", "202607", "202608", "202609"]

    assert periods_to_process(upstream, processed, requested) == ["202608", "202609"]
    assert periods_to_process(upstream, processed, requested, force=True) == ["202607", "202608", "202609"]
