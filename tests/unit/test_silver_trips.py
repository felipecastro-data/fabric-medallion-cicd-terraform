from datetime import datetime

import pytest
from chispa import assert_df_equality

from divvy_medallion.silver import trips

BRONZE_COLUMNS = [
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


def _bronze(spark, rows):
    defaults = {
        "ride_id": "R1",
        "rideable_type": "classic_bike",
        "started_at": "2026-09-01 08:00:00.000",
        "ended_at": "2026-09-01 08:20:00.000",
        "start_station_name": " Clark St & Elm St ",
        "start_station_id": "chi00281",
        "end_station_name": "Wells St & Concord Ln",
        "end_station_id": "CHI00412",
        "start_lat": "41.90297",
        "start_lng": "-87.6313",
        "end_lat": "41.912133",
        "end_lng": "-87.634656",
        "member_casual": "Member",
        "_source_file": "202609.zip",
        "_batch_id": "b1",
        "_ingested_at": datetime(2026, 10, 1, 12, 0),
        "_period": "202609",
    }
    data = [tuple({**defaults, **row}[c] for c in BRONZE_COLUMNS) for row in rows]
    schema = ", ".join(f"{c} {'timestamp' if c == '_ingested_at' else 'string'}" for c in BRONZE_COLUMNS)
    return spark.createDataFrame(data, schema)


def test_standardize_types_and_derives_attributes(spark):
    from pyspark.sql import functions as F

    df = trips.standardize(_bronze(spark, [{}]))
    row = df.withColumn("started_txt", F.date_format("started_at", "yyyy-MM-dd HH:mm:ss.SSS")).first()

    # Divvy times are Chicago wall-clock; with a UTC session they round-trip unchanged
    assert row.started_txt == "2026-09-01 08:00:00.000"
    assert row.started_txt == "2026-09-01 08:00:00.000"
    assert row.duration_s == 1200
    assert (row.trip_date.isoformat(), row.start_hour, row.day_of_week) == ("2026-09-01", 8, "Tue")
    assert row.start_station_name == "Clark St & Elm St"  # trimmed
    assert row.start_station_id == "CHI00281"  # upper-cased
    assert row.member_type == "member"
    assert 1.0 < row.distance_km < 1.1
    assert row.is_round_trip is False and row.is_dockless is False


def test_timestamps_without_milliseconds_are_parsed(spark):
    row = trips.standardize(_bronze(spark, [{"started_at": "2026-09-01 08:00:00", "ended_at": "2026-09-01 08:01:30"}]))
    assert row.first().duration_s == 90.0


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"ride_id": "  "}, "missing_ride_id"),
        ({"started_at": "not-a-date"}, "invalid_timestamp"),
        ({"ended_at": "2026-09-01 07:59:00"}, "non_positive_duration"),
        ({"ended_at": "2026-09-01 08:00:30"}, "duration_under_60s"),
        ({"ended_at": "2026-09-02 09:00:00"}, "duration_over_24h"),
        ({"end_lat": "", "end_lng": ""}, "missing_end_coordinates"),
        ({"start_lat": "0.0"}, "coordinates_out_of_range"),
        ({"rideable_type": "unicycle"}, "unknown_rideable_type"),
        ({"member_casual": "staff"}, "unknown_member_type"),
    ],
)
def test_each_rule_quarantines_with_its_reason(spark, override, reason):
    valid, quarantine = trips.transform(_bronze(spark, [override]))

    assert valid.count() == 0
    assert reason in quarantine.first().dq_reasons


def test_dockless_trips_are_valid(spark):
    valid, quarantine = trips.transform(
        _bronze(spark, [{"start_station_id": "", "start_station_name": "", "end_station_id": None}])
    )
    assert (valid.count(), quarantine.count()) == (1, 0)
    assert valid.first().is_dockless is True


def test_row_failing_several_rules_records_all_reasons(spark):
    _, quarantine = trips.transform(_bronze(spark, [{"ride_id": None, "end_lat": None, "member_casual": "x"}]))
    assert set(quarantine.first().dq_reasons) == {"missing_ride_id", "missing_end_coordinates", "unknown_member_type"}


def test_duplicates_keep_latest_and_quarantine_the_rest(spark):
    rows = [
        {"_ingested_at": datetime(2026, 10, 1), "end_station_name": "old"},
        {"_ingested_at": datetime(2026, 10, 2), "end_station_name": "new"},
        {"ride_id": "R2"},
    ]
    valid, quarantine = trips.transform(_bronze(spark, rows))

    assert valid.count() == 2
    assert valid.filter("ride_id = 'R1'").first().end_station_name == "new"
    superseded = quarantine.first()
    assert (superseded.end_station_name, superseded.dq_reasons) == ("old", ["duplicate_ride_id"])


def test_failing_duplicate_does_not_displace_a_valid_version(spark):
    rows = [
        {"_ingested_at": datetime(2026, 10, 1)},  # valid
        {"_ingested_at": datetime(2026, 10, 2), "end_lat": None},  # newer but fails a rule
    ]
    valid, quarantine = trips.transform(_bronze(spark, rows))

    assert valid.count() == 1
    assert quarantine.first().dq_reasons == ["missing_end_coordinates"]


@pytest.mark.parametrize(
    ("ended_at", "expected_reasons"),
    [
        ("2026-09-01 08:00:59.900", ["duration_under_60s"]),  # 59.9 s
        ("2026-09-01 08:01:00.000", []),  # exactly 60 s
        ("2026-09-02 08:00:00.000", []),  # exactly 24 h
        ("2026-09-02 08:00:00.500", ["duration_over_24h"]),  # 24 h + 0.5 s
    ],
)
def test_duration_limits_use_millisecond_precision(spark, ended_at, expected_reasons):
    df = trips.apply_quality_rules(
        trips.standardize(_bronze(spark, [{"started_at": "2026-09-01 08:00:00.000", "ended_at": ended_at}]))
    )
    assert df.first().dq_reasons == expected_reasons


def test_fractional_duration_under_one_second_is_a_false_start(spark):
    df = trips.standardize(
        _bronze(spark, [{"started_at": "2026-09-01 08:00:00.100", "ended_at": "2026-09-01 08:00:00.900"}])
    )
    row = trips.apply_quality_rules(df).first()
    assert (row.duration_s, row.dq_reasons) == (0.8, ["duration_under_60s"])


def test_valid_output_has_the_silver_contract(spark):
    valid, _ = trips.transform(_bronze(spark, [{}]))
    assert tuple(valid.columns) == trips.SILVER_COLUMNS


def test_valid_plus_quarantine_accounts_for_every_bronze_row(spark):
    bronze = _bronze(
        spark,
        [
            {},
            {"_ingested_at": datetime(2026, 9, 30)},  # superseded duplicate of R1
            {"ride_id": "R2", "end_lat": None},
            {"ride_id": "R3", "ended_at": "bad"},
        ],
    )
    valid, quarantine = trips.transform(bronze)
    assert valid.count() + quarantine.count() == bronze.count()


def test_haversine_known_distance(spark):
    # Chicago Union Station -> Navy Pier is ~2.9 km in a straight line
    df = spark.createDataFrame([(41.8786, -87.6403, 41.8917, -87.6086)], "a double, b double, c double, d double")
    from pyspark.sql import functions as F

    km = df.select(trips.haversine_km(F.col("a"), F.col("b"), F.col("c"), F.col("d")).alias("km")).first().km
    assert 2.8 < km < 3.1


def test_quarantine_keeps_bronze_lineage(spark):
    _, quarantine = trips.transform(_bronze(spark, [{"ride_id": None}]))
    expected = spark.createDataFrame(
        [("202609.zip", "b1", "202609")], "_source_file string, _batch_id string, _period string"
    )
    assert_df_equality(quarantine.select("_source_file", "_batch_id", "_period"), expected, ignore_nullable=True)
