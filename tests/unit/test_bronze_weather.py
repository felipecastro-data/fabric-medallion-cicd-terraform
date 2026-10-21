from urllib.parse import parse_qs, urlparse

import pytest

from divvy_medallion.bronze import weather


def _payload(days=("2026-09-01", "2026-09-02")):
    daily = {"time": list(days)}
    for i, variable in enumerate(weather.DAILY_VARIABLES):
        daily[variable] = [float(i + n) for n in range(len(days))]
    daily["precipitation_sum"] = [0, None]  # API returns ints and nulls too
    return {"latitude": 41.86, "longitude": -87.65, "daily": daily}


def test_build_url_covers_the_whole_month():
    query = parse_qs(urlparse(weather.build_url("202602")).query)

    assert query["start_date"] == ["2026-02-01"]
    assert query["end_date"] == ["2026-02-28"]
    assert query["timezone"] == ["America/Chicago"]
    assert set(query["daily"][0].split(",")) == set(weather.DAILY_VARIABLES)


def test_to_rows_flattens_daily_block():
    rows = weather.to_rows(_payload())

    assert [r["date"] for r in rows] == ["2026-09-01", "2026-09-02"]
    assert rows[0]["weather_code"] == 0 and isinstance(rows[0]["weather_code"], int)
    assert rows[1]["precipitation_sum"] is None


def test_to_rows_rejects_incomplete_payloads():
    payload = _payload()
    del payload["daily"]["rain_sum"]
    with pytest.raises(ValueError, match="rain_sum"):
        weather.to_rows(payload)
    with pytest.raises(ValueError, match="daily.time"):
        weather.to_rows({"latitude": 1})


def test_to_dataframe_casts_numeric_columns(spark):
    df = weather.to_dataframe(spark, _payload())

    assert df.count() == 2
    assert dict(df.dtypes)["precipitation_sum"] == "double"
    assert dict(df.dtypes)["weather_code"] == "int"
    assert df.filter(df.date == "2026-09-01").first().precipitation_sum == 0.0
