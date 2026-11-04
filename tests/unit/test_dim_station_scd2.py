from datetime import date, datetime

import pytest

from divvy_medallion.dq import rules
from divvy_medallion.dq.suite import scd2_overlaps
from divvy_medallion.gold import DOCKLESS_STATION_KEY
from divvy_medallion.gold.dimensions import (
    DIM_STATION_COLUMNS,
    SCD2_BEGINNING,
    SCD2_OPEN_END,
    build_dim_station,
    initial_dim_station,
    merge_dim_station,
    needs_scd2_migration,
    scd2_station_changes,
)

STATIONS_SCHEMA = (
    "station_id string, station_name string, latitude double, longitude double, first_seen date, last_seen date"
)
T1, T2 = datetime(2026, 10, 1, 6), datetime(2026, 11, 2, 6)


def snapshot(spark, *rows):
    return build_dim_station(spark.createDataFrame(list(rows), STATIONS_SCHEMA))


def station(sid, name, lat=41.8800, lng=-87.6300, first=date(2025, 10, 1), last=date(2026, 9, 30)):
    return (sid, name, lat, lng, first, last)


def by_action(changes):
    out = {}
    for r in changes.collect():
        out.setdefault(r.action, {})[r.station_id] = r
    return out


def test_initial_load_opens_one_version_per_station_since_the_beginning(spark):
    dim = initial_dim_station(snapshot(spark, station("S1", "Clark & Elm"))).collect()

    assert {r.station_id for r in dim} == {"S1", "DOCKLESS"}
    assert all(r.is_current and r.valid_from == SCD2_BEGINNING and r.valid_to == SCD2_OPEN_END for r in dim)
    assert {r.change_reason for r in dim} == {"initial"}
    dockless = next(r for r in dim if r.station_id == "DOCKLESS")
    assert dockless.station_version_key == dockless.station_key == DOCKLESS_STATION_KEY


def test_unchanged_snapshot_stages_nothing(spark):
    snap = snapshot(spark, station("S1", "Clark & Elm"))
    assert scd2_station_changes(initial_dim_station(snap), snap, T1).count() == 0


def test_rename_closes_the_current_version_and_opens_a_new_one(spark):
    current = initial_dim_station(snapshot(spark, station("S1", "Clark & Elm")))
    changes = by_action(scd2_station_changes(current, snapshot(spark, station("S1", "Clark St & Elm St")), T1))

    closed, opened = changes["close"]["S1"], changes["insert"]["S1"]
    assert (closed.station_name, closed.valid_to, closed.is_current) == ("Clark & Elm", T1, False)
    assert closed.merge_key == closed.station_version_key
    assert (opened.station_name, opened.valid_from, opened.valid_to, opened.is_current) == (
        "Clark St & Elm St",
        T1,
        SCD2_OPEN_END,
        True,
    )
    assert opened.change_reason == "renamed" and opened.merge_key is None
    assert opened.station_key == closed.station_key and opened.station_version_key != closed.station_version_key


def test_relocation_beyond_threshold_is_a_new_version_but_gps_drift_is_type1(spark):
    current = initial_dim_station(snapshot(spark, station("S1", "A"), station("S2", "B")))
    moved = station("S1", "A", lat=41.8820)  # ~220 m north
    drift = station("S2", "B", lat=41.88005, last=date(2026, 10, 31))  # ~6 m, and seen again later
    changes = by_action(scd2_station_changes(current, snapshot(spark, moved, drift), T1))

    assert changes["insert"]["S1"].change_reason == "relocated"
    assert "S1" in changes["close"]
    s2 = changes["type1"]["S2"]
    assert (s2.latitude, s2.last_seen, s2.valid_from, s2.change_reason) == (
        41.88005,
        date(2026, 10, 31),
        SCD2_BEGINNING,
        "initial",
    )
    assert "S2" not in changes.get("insert", {})


def test_new_station_is_valid_since_the_beginning_and_missing_stations_are_kept(spark):
    current = initial_dim_station(snapshot(spark, station("S1", "A"), station("S9", "Gone")))
    changes = by_action(scd2_station_changes(current, snapshot(spark, station("S1", "A"), station("S3", "C")), T1))

    assert set(changes) == {"insert"}
    assert changes["insert"]["S3"].valid_from == SCD2_BEGINNING and changes["insert"]["S3"].change_reason == "new"


def test_merge_initialises_then_keeps_history_across_runs(spark, tmp_path):
    path = str(tmp_path / "dim_station")
    assert needs_scd2_migration(spark, path)

    assert merge_dim_station(spark, snapshot(spark, station("S1", "A"), station("S2", "B")), path, T1) == {"initial": 3}
    assert not needs_scd2_migration(spark, path)
    assert merge_dim_station(spark, snapshot(spark, station("S1", "A2"), station("S2", "B")), path, T1) == {
        "insert": 1,
        "close": 1,
    }
    assert merge_dim_station(spark, snapshot(spark, station("S1", "A3"), station("S2", "B")), path, T2) == {
        "insert": 1,
        "close": 1,
    }
    assert merge_dim_station(spark, snapshot(spark, station("S1", "A3"), station("S2", "B")), path, T2) == {}

    dim = spark.read.format("delta").load(path)
    assert dim.columns == list(DIM_STATION_COLUMNS)
    s1 = sorted(dim.filter("station_id = 'S1'").collect(), key=lambda r: r.valid_from)
    assert [(r.station_name, r.valid_from, r.valid_to, r.is_current) for r in s1] == [
        ("A", SCD2_BEGINNING, T1, False),
        ("A2", T1, T2, False),
        ("A3", T2, SCD2_OPEN_END, True),
    ]
    assert rules.no_rows(scd2_overlaps(dim), "station_id", "overlapping or gapped versions").passed
    assert rules.unique(dim.filter("is_current"), "station_key").passed


def test_as_of_join_resolves_the_name_valid_when_the_trip_started(spark, tmp_path):
    path = str(tmp_path / "dim_station")
    merge_dim_station(spark, snapshot(spark, station("S1", "Old name")), path, T1)
    merge_dim_station(spark, snapshot(spark, station("S1", "New name")), path, T2)
    dim = spark.read.format("delta").load(path)
    key = dim.filter("station_id = 'S1'").first().station_key
    trips = spark.createDataFrame(
        [("before", key, datetime(2026, 10, 15)), ("after", key, datetime(2026, 11, 3))],
        "ride_id string, start_station_key long, started_at timestamp",
    )
    joined = trips.join(
        dim,
        (trips.start_station_key == dim.station_key)
        & (trips.started_at >= dim.valid_from)
        & (trips.started_at < dim.valid_to),
    )
    assert {r.ride_id: r.station_name for r in joined.collect()} == {"before": "Old name", "after": "New name"}


def test_a_type1_table_is_migrated_to_scd2(spark, tmp_path):
    path = str(tmp_path / "dim_station")
    legacy = snapshot(spark, station("S1", "A"))  # the pre-SCD2 dim_station schema
    legacy.write.format("delta").save(path)
    assert needs_scd2_migration(spark, path)

    assert merge_dim_station(spark, legacy, path, T1) == {"initial": 2}
    assert spark.read.format("delta").load(path).filter("is_current").count() == 2


@pytest.mark.parametrize(
    ("versions", "bad"),
    [
        ([(SCD2_BEGINNING, T1), (T1, SCD2_OPEN_END)], 0),
        ([(SCD2_BEGINNING, T2), (T1, SCD2_OPEN_END)], 1),  # overlap
        ([(SCD2_BEGINNING, T1), (T2, SCD2_OPEN_END)], 1),  # gap
        ([(T2, T1)], 1),  # inverted interval
    ],
)
def test_scd2_overlaps_finds_gaps_overlaps_and_inverted_intervals(spark, versions, bad):
    dim = spark.createDataFrame(
        [(1, "S1", f, t) for f, t in versions],
        "station_key long, station_id string, valid_from timestamp, valid_to timestamp",
    )
    assert scd2_overlaps(dim).count() == bad
