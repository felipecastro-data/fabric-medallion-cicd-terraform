from datetime import date

from pyspark.sql import functions as F

from divvy_medallion.control import combine_watermarks, periods_to_process
from divvy_medallion.gold import DOCKLESS_STATION_KEY, station_key
from divvy_medallion.gold.dimensions import build_dim_date, us_holidays


def test_station_key_is_deterministic_non_negative_and_reserves_minus_one_for_dockless(spark):
    df = spark.createDataFrame([("S1",), ("S1",), ("S2",), (None,)], "station_id string")
    keys = [r.k for r in df.select(station_key(F.col("station_id")).alias("k")).collect()]

    assert keys[0] == keys[1] != keys[2]
    assert keys[0] >= 0 and keys[2] >= 0
    assert keys[3] == DOCKLESS_STATION_KEY


def test_us_holidays_floating_dates():
    holidays = us_holidays(2026)
    assert holidays[date(2026, 5, 25)] == "Memorial Day"  # last Monday of May
    assert holidays[date(2026, 9, 7)] == "Labor Day"  # first Monday of September
    assert holidays[date(2026, 11, 26)] == "Thanksgiving Day"  # fourth Thursday of November
    assert holidays[date(2026, 1, 19)] == "Martin Luther King Jr. Day"
    assert len(holidays) == 11


def test_dim_date_calendar_attributes(spark):
    rows = {r.date: r for r in build_dim_date(spark, date(2026, 9, 5), date(2026, 9, 7)).collect()}

    assert len(rows) == 3
    sat, mon = rows[date(2026, 9, 5)], rows[date(2026, 9, 7)]
    assert (sat.date_key, sat.period, sat.day_of_week, sat.day_name, sat.is_weekend) == (
        20260905,
        "202609",
        6,
        "Saturday",
        True,
    )
    assert (mon.day_of_week, mon.is_weekend, mon.is_holiday, mon.holiday_name) == (1, False, True, "Labor Day")
    assert rows[date(2026, 9, 6)].is_holiday is False


def test_dim_station_has_every_station_plus_the_dockless_member(medallion):
    rows = {r.station_id: r for r in medallion.dim_station.collect()}

    assert set(rows) == {"S1", "S2", "DOCKLESS"}
    assert rows["DOCKLESS"].station_key == DOCKLESS_STATION_KEY and rows["DOCKLESS"].is_dockless
    assert rows["S1"].station_name == "Clark & Elm" and not rows["S1"].is_dockless


def test_dim_weather_keys_by_date_and_bands_temperature(medallion):
    rows = {r.date_key: r for r in medallion.dim_weather.collect()}

    assert rows[20260901].temperature_band == "warm"  # 20.5 °C
    assert rows[20260902].temperature_band == "mild"  # 14.0 °C
    assert rows[20260902].condition == "rain" and rows[20260902].is_wet_day


def test_fact_trips_grain_keys_and_measures(medallion):
    rows = {r.ride_id: r for r in medallion.fact_trips.collect()}

    assert len(rows) == 43 and "R4" not in rows  # R4 (< 60 s) and the older copy of R1 are quarantined
    r1, r2 = rows["R1"], rows["R2"]
    assert (r1.date_key, r1.duration_min, r1.is_electric, r1._bronze_batch_id) == (20260901, 20.0, False, "b1")
    assert r2.start_station_key == DOCKLESS_STATION_KEY and r2.end_station_key == r1.start_station_key
    assert r2.is_electric and r2.is_dockless
    assert rows["R3"].is_round_trip


def test_daily_kpis_per_day_and_member_type_with_weather(medallion):
    rows = {(r.date_key, r.member_type): r for r in medallion.daily_kpis.collect()}

    assert set(rows) == {(20260901, "member"), (20260901, "casual"), (20260902, "member"), (20260903, "member")}
    assert rows[(20260903, "member")].trips == 40
    casual = rows[(20260901, "casual")]
    assert (casual.trips, casual.avg_duration_min, casual.electric_share, casual.dockless_share) == (1, 30.0, 1.0, 1.0)
    assert (casual.condition, casual.temperature_2m_mean) == ("clear", 20.5)
    assert rows[(20260902, "member")].round_trip_share == 1.0


def test_station_monthly_starts_ends_and_net_flow(medallion):
    s1 = medallion.dim_station.filter("station_id = 'S1'").first().station_key
    rows = {r.station_key: r for r in medallion.station_monthly.collect()}

    assert (rows[s1].trips_started, rows[s1].trips_ended, rows[s1].net_flow) == (41, 1, -40)
    dockless = rows[DOCKLESS_STATION_KEY]
    assert (dockless.trips_started, dockless.trips_ended, dockless.net_flow, dockless.member_share) == (1, 0, -1, 0.0)
    assert sum(r.trips_started for r in rows.values()) == sum(r.trips_ended for r in rows.values()) == 43


def test_combined_watermark_reprocesses_a_period_when_any_input_changes():
    trips = {"202608": "t1", "202609": "t2"}
    weather = {"202608": "w1"}
    combined = combine_watermarks(trips, weather)

    assert combined == {"202608": "t1|w1", "202609": "t2|-"}
    processed = {("202608", "t1|w1"), ("202609", "t2|-")}
    assert periods_to_process(combined, processed, ["202608", "202609"]) == []
    weather_reloaded = combine_watermarks(trips, {"202608": "w1", "202609": "w2"})
    assert periods_to_process(weather_reloaded, processed, ["202608", "202609"]) == ["202609"]


def test_months_to_refresh_follows_both_feeding_source_files():
    from datetime import datetime

    from divvy_medallion.gold.aggregates import months_to_refresh

    t = datetime
    facts = {"202608": t(2026, 10, 1), "202609": t(2026, 10, 2)}
    # nothing aggregated yet: both file months plus the spill-over month before the first file
    assert months_to_refresh(facts, {}) == ["202607", "202608", "202609"]
    done = {"202607": t(2026, 10, 3), "202608": t(2026, 10, 3), "202609": t(2026, 10, 3)}
    assert months_to_refresh(facts, done) == []
    # reloading the 202609 file refreshes 202609 AND 202608 (its 2026-08-31 trips), not 202607
    assert months_to_refresh(facts | {"202609": t(2026, 10, 4)}, done) == ["202608", "202609"]
    # a failed 202608 aggregate build (no newer success) is retried
    assert months_to_refresh(facts, done | {"202608": t(2026, 9, 30)}) == ["202608"]


def test_boundary_day_is_one_row_even_when_split_across_source_files(spark, medallion):
    from divvy_medallion.gold import aggregates

    # the same day's trips arriving in two source files (2026-09-01 in both "202609" and "202610")
    late = medallion.fact_trips.filter("date_key = 20260901").withColumn("_period", F.lit("202610"))
    late = late.withColumn("ride_id", F.concat(F.col("ride_id"), F.lit("-late")))
    fact = medallion.fact_trips.unionByName(late)

    month = aggregates.month_facts(fact, "202609")
    kpis = aggregates.build_daily_kpis(month, medallion.dim_weather)
    day = kpis.filter("date_key = 20260901").collect()

    assert sorted(r.member_type for r in day) == ["casual", "member"]  # one row per member type, not per file
    assert sum(r.trips for r in kpis.collect()) == month.count()
    assert {r.trip_month for r in kpis.collect()} == {"202609"}
    stations = aggregates.build_station_monthly(month)
    assert stations.groupBy("trip_month", "station_key").count().filter("count > 1").count() == 0
