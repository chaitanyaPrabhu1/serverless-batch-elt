import datetime as dt

import pytest

import common
import ingest
from conftest import fake_api_response

NOW = dt.datetime(2024, 1, 1, 13, 30, 0, tzinfo=dt.timezone.utc)


def test_ingest_writes_one_raw_file_per_city(cities, local_backend):
    result = ingest.run(now=NOW, backend=local_backend, cities=cities,
                        fetch=lambda url: fake_api_response())

    assert result["written"] == 2
    assert result["failed"] == 0
    keys = local_backend.list_keys("raw/weather/")
    assert sorted(keys) == [
        "raw/weather/dt=2024-01-01/run_hour=13/london.json",
        "raw/weather/dt=2024-01-01/run_hour=13/tokyo.json",
    ]
    record = local_backend.get_json(keys[0])
    assert record["city"]["city_id"] == "london"
    assert record["api_response"]["current"]["temperature_2m"] == 12.3
    assert record["ingested_at"] == "2024-01-01T13:30:00Z"


def test_ingest_one_bad_city_does_not_kill_the_run(cities, local_backend):
    def flaky_fetch(url):
        if "139.6503" in url:  # tokyo's longitude
            raise RuntimeError("api timeout")
        return fake_api_response()

    result = ingest.run(now=NOW, backend=local_backend, cities=cities, fetch=flaky_fetch)
    assert result["written"] == 1
    assert result["failed"] == 1
    assert result["failures"][0]["city_id"] == "tokyo"


def test_ingest_raises_when_all_cities_fail(cities, local_backend):
    def dead_fetch(url):
        raise RuntimeError("down")

    with pytest.raises(RuntimeError, match="0 files"):
        ingest.run(now=NOW, backend=local_backend, cities=cities, fetch=dead_fetch)


def test_build_url_includes_current_fields(cities):
    url = ingest._build_url(cities[0])
    assert "latitude=51.5074" in url
    assert "temperature_2m" in url
    assert "timezone=UTC" in url


def test_ingest_against_mocked_s3(cities):
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")

    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-lake")
        backend = common.S3Backend("test-lake", client=client)

        result = ingest.run(now=NOW, backend=backend, cities=cities,
                            fetch=lambda url: fake_api_response())
        assert result["written"] == 2
        listed = client.list_objects_v2(Bucket="test-lake", Prefix="raw/")
        assert listed["KeyCount"] == 2
