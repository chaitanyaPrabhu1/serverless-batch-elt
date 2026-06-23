import datetime as dt

import pyarrow.parquet as pq
import pytest

import common
import ingest
import transform
from conftest import fake_api_response

NOW = dt.datetime(2024, 1, 1, 13, 30, 0, tzinfo=dt.timezone.utc)


def _seed_raw(backend, cities, fetch):
    return ingest.run(now=NOW, backend=backend, cities=cities, fetch=fetch)


def test_transform_writes_parquet_clean_zone(cities, local_backend):
    ingest_result = _seed_raw(local_backend, cities, lambda url: fake_api_response())
    result = transform.run(event=ingest_result, now=NOW, backend=local_backend)

    assert result["rows_clean"] == 2
    assert result["rows_quarantined"] == 0
    assert result["clean_location"].endswith("clean/weather/dt=2024-01-01/weather-2024-01-01-13.parquet")

    table = pq.read_table(result["clean_location"])
    assert table.num_rows == 2
    cols = set(table.column_names)
    assert {"observation_id", "city_id", "temperature_c", "observed_at"} <= cols
    rows = {r["city_id"]: r for r in table.to_pylist()}
    assert rows["london"]["temperature_c"] == 12.3
    assert rows["london"]["observation_id"] == "london:2024-01-01T13:00"
    # observed_at parsed into a real timestamp.
    assert isinstance(rows["london"]["observed_at"], dt.datetime)


def test_transform_quarantines_out_of_range_temperature(cities, local_backend):
    def bad_fetch(url):
        if "139.6503" in url:  # tokyo -> impossible temperature
            return fake_api_response(temperature=999)
        return fake_api_response()

    ingest_result = _seed_raw(local_backend, cities, bad_fetch)
    result = transform.run(event=ingest_result, now=NOW, backend=local_backend)

    assert result["rows_clean"] == 1
    assert result["rows_quarantined"] == 1
    quarantine = local_backend.get_json(
        "quarantine/weather/dt=2024-01-01/quarantine-2024-01-01-13.json"
    )
    assert quarantine[0]["reason"].startswith("temperature_c out of range")


def test_transform_is_idempotent_on_rerun(cities, local_backend):
    ingest_result = _seed_raw(local_backend, cities, lambda url: fake_api_response())
    transform.run(event=ingest_result, now=NOW, backend=local_backend)
    transform.run(event=ingest_result, now=NOW, backend=local_backend)  # rerun same hour

    clean = [k for k in local_backend.list_keys("clean/weather/") if k.endswith(".parquet")]
    assert clean == ["clean/weather/dt=2024-01-01/weather-2024-01-01-13.parquet"]
    # Re-running overwrote the one file; no duplicate rows.
    table = pq.read_table(local_backend.location(clean[0]))
    assert table.num_rows == 2


def test_transform_dedupes_within_run(cities, local_backend):
    ingest_result = _seed_raw(local_backend, cities, lambda url: fake_api_response())
    # Forge a duplicate raw file for london under a different filename.
    dup = local_backend.get_json("raw/weather/dt=2024-01-01/run_hour=13/london.json")
    local_backend.put_json("raw/weather/dt=2024-01-01/run_hour=13/london_dup.json", dup)

    result = transform.run(event=ingest_result, now=NOW, backend=local_backend)
    assert result["rows_clean"] == 2  # london counted once
    assert result["rows_quarantined"] == 1  # the duplicate was quarantined


def test_validate_rules():
    base = {
        "city_id": "x", "observed_at": "2024-01-01T00:00",
        "observation_id": "x:2024-01-01T00:00", "temperature_c": 10.0,
        "relative_humidity_pct": 50.0,
    }
    assert transform.validate(base) is None
    assert "missing city_id" == transform.validate({**base, "city_id": None})
    assert transform.validate({**base, "temperature_c": None}) == "missing temperature_c"
    assert transform.validate({**base, "temperature_c": 200}).startswith("temperature_c out of range")
    assert transform.validate({**base, "relative_humidity_pct": 150}).startswith("relative_humidity_pct out of range")


def test_transform_raises_when_no_raw(local_backend):
    with pytest.raises(RuntimeError, match="no raw files"):
        transform.run(event={"dt": "2024-01-01", "hour": "13", "run_ts": "x"},
                      now=NOW, backend=local_backend)
