import io
import zipfile
from pathlib import Path

import pytest

from divvy_medallion.bronze import PERIOD_COLUMN, add_ingestion_metadata, divvy

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "divvy_sample.csv"


def test_source_url():
    assert divvy.source_url("202609") == "https://divvy-tripdata.s3.amazonaws.com/202609-divvy-tripdata.zip"


def test_validate_header_detects_drift():
    divvy.validate_header(list(divvy.COLUMNS))
    with pytest.raises(divvy.SchemaDriftError, match="missing=\\['member_casual'\\]"):
        divvy.validate_header(list(divvy.COLUMNS[:-1]))
    with pytest.raises(divvy.SchemaDriftError, match="unexpected=\\['tripduration'\\]"):
        divvy.validate_header([*divvy.COLUMNS, "tripduration"])


def test_read_raw_keeps_every_column_as_string(spark):
    df = divvy.read_raw(spark, str(FIXTURE))

    assert df.columns == list(divvy.COLUMNS)
    assert {f.dataType.simpleString() for f in df.schema.fields} == {"string"}
    assert df.count() == len(FIXTURE.read_text().splitlines()) - 1
    # dockless e-bike trips have empty station fields -> nulls, not empty strings
    assert df.filter(df.start_station_id.isNull()).count() >= 1


def test_add_ingestion_metadata(spark):
    df = add_ingestion_metadata(
        divvy.read_raw(spark, str(FIXTURE)), period="202609", batch_id="b-1", source_file="202609.zip"
    )
    row = df.select("_source_file", "_batch_id", "_ingested_at", PERIOD_COLUMN).first()

    assert (row._source_file, row._batch_id, row[PERIOD_COLUMN]) == ("202609.zip", "b-1", "202609")
    assert row._ingested_at is not None


class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_download_and_extract_ignores_macos_metadata(tmp_path, monkeypatch):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("202609-divvy-tripdata.csv", FIXTURE.read_text())
        archive.writestr("__MACOSX/._202609-divvy-tripdata.csv", "junk")
    monkeypatch.setattr(divvy.urllib.request, "urlopen", lambda url, timeout: _FakeResponse(buffer.getvalue()))

    zip_path, csv_path = divvy.download_and_extract("202609", tmp_path)

    assert zip_path.name == "202609-divvy-tripdata.zip"
    assert csv_path.name == "202609-divvy-tripdata.csv"
    assert csv_path.read_text() == FIXTURE.read_text()
