import pytest

from divvy_medallion.control import IngestionRun, append_runs, succeeded_periods
from divvy_medallion.delta import overwrite_partition


def _df(spark, period, n):
    return spark.createDataFrame([(f"id-{i}", period) for i in range(n)], "ride_id string, _period string")


def test_overwrite_partition_is_idempotent(spark, tmp_path):
    path = str(tmp_path / "trips")
    overwrite_partition(_df(spark, "202608", 3), path, "_period", "202608")
    overwrite_partition(_df(spark, "202609", 5), path, "_period", "202609")

    # re-running a period replaces it: no duplicates, other periods untouched
    overwrite_partition(_df(spark, "202609", 4), path, "_period", "202609")

    counts = {r._period: r["count"] for r in spark.read.format("delta").load(path).groupBy("_period").count().collect()}
    assert counts == {"202608": 3, "202609": 4}


def test_overwrite_partition_rejects_rows_from_other_periods(spark, tmp_path):
    mixed = _df(spark, "202608", 1).unionByName(_df(spark, "202609", 1))
    with pytest.raises(ValueError, match="outside partition"):
        overwrite_partition(mixed, str(tmp_path / "trips"), "_period", "202609")


def test_ingestion_log_tracks_successful_periods(spark, tmp_path):
    path = str(tmp_path / "ingestion_log")
    assert succeeded_periods(spark, path, "divvy_trips") == set()

    runs = [
        IngestionRun("divvy_trips", "202608", "b-1").succeed(10, "202608.zip"),
        IngestionRun("divvy_trips", "202609", "b-1").fail(RuntimeError("download failed")),
        IngestionRun("weather_daily", "202609", "b-1").succeed(30),
    ]
    append_runs(spark, path, runs)

    assert succeeded_periods(spark, path, "divvy_trips") == {"202608"}
    failed = spark.read.format("delta").load(path).filter("status = 'FAILED'").first()
    assert failed.error == "RuntimeError: download failed"


def test_latest_succeeded_batches_and_processed_pairs(spark, tmp_path):
    import time

    from divvy_medallion.control import latest_succeeded_batches, processed_batches

    path = str(tmp_path / "log")
    append_runs(spark, path, [IngestionRun("divvy_trips", "202609", "b1").succeed(10)])
    time.sleep(0.01)
    append_runs(spark, path, [IngestionRun("divvy_trips", "202609", "b2").succeed(11)])
    append_runs(spark, path, [IngestionRun("divvy_trips", "202608", "b3").fail(RuntimeError("x"))])

    assert latest_succeeded_batches(spark, path, "divvy_trips") == {"202609": "b2"}
    assert processed_batches(spark, path, "divvy_trips") == {("202609", "b1"), ("202609", "b2")}


def test_append_runs_evolves_logs_created_before_rows_rejected(spark, tmp_path):
    path = str(tmp_path / "old_log")
    old = spark.createDataFrame(
        [("divvy_trips", "202609", "b1", "SUCCEEDED")], "source string, period string, batch_id string, status string"
    )
    old.write.format("delta").save(path)

    append_runs(spark, path, [IngestionRun("silver.trips", "202609", "b1").succeed(9, rows_rejected=1)])

    row = spark.read.format("delta").load(path).filter("source = 'silver.trips'").first()
    assert (row.rows_written, row.rows_rejected) == (9, 1)


def test_dimension_rebuild_follows_its_own_watermark(spark, tmp_path):
    from datetime import datetime

    from divvy_medallion.control import last_success, needs_rebuild

    path = str(tmp_path / "log")
    assert last_success(spark, path, "silver.trips") is None
    assert needs_rebuild(None, None) is False  # nothing upstream yet

    append_runs(spark, path, [IngestionRun("silver.trips", "202609", "b1").succeed(10)])
    trips_at = last_success(spark, path, "silver.trips")
    assert needs_rebuild(trips_at, None) is True  # never built

    append_runs(spark, path, [IngestionRun("silver.stations", "ALL", "x").fail(RuntimeError("write failed"))])
    assert needs_rebuild(trips_at, last_success(spark, path, "silver.stations")) is True  # failure -> retry rebuilds

    append_runs(spark, path, [IngestionRun("silver.stations", "ALL", "x").succeed(1697)])
    assert needs_rebuild(trips_at, last_success(spark, path, "silver.stations")) is False
    assert needs_rebuild(datetime(2999, 1, 1), last_success(spark, path, "silver.stations")) is True  # newer trips
