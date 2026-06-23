"""Shared configuration, storage backends, and helpers for the ELT Lambdas.

Two storage backends are provided so the exact same ingest/transform code runs
both in AWS (S3 via boto3) and locally (a directory tree), which is what makes
``make local-run`` and the unit tests possible without an AWS account.

Parquet is written with pyarrow directly (no awswrangler dependency): in AWS,
pyarrow is supplied by the AWS-managed "AWSSDKPandas" Lambda layer; locally it
comes from requirements-dev.txt.
"""
from __future__ import annotations

import io
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

API_BASE_URL = os.environ.get("API_BASE_URL", "https://api.open-meteo.com/v1/forecast")

# The Open-Meteo "current" fields we request, in a stable order.
CURRENT_FIELDS = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "pressure_msl",
    "cloud_cover",
    "weather_code",
    "is_day",
]

RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/weather")
CLEAN_PREFIX = os.environ.get("CLEAN_PREFIX", "clean/weather")
QUARANTINE_PREFIX = os.environ.get("QUARANTINE_PREFIX", "quarantine/weather")

# Plausible physical bounds used to decide whether a record is trustworthy.
TEMP_MIN_C, TEMP_MAX_C = -90.0, 60.0
HUMIDITY_MIN, HUMIDITY_MAX = 0.0, 100.0


def _default_cities_path() -> str:
    # config/cities.json sits next to the lambda/ dir in the repo.
    here = Path(__file__).resolve().parent
    return str(here.parent / "config" / "cities.json")


def load_cities(path: str | None = None) -> list[dict[str, Any]]:
    path = path or os.environ.get("CITIES_CONFIG") or _default_cities_path()
    with open(path, "r", encoding="utf-8") as fh:
        cities = json.load(fh)
    if not isinstance(cities, list) or not cities:
        raise ValueError(f"cities config at {path} must be a non-empty JSON array")
    return cities


# --------------------------------------------------------------------------- #
# Storage backends                                                            #
# --------------------------------------------------------------------------- #


class StorageBackend(ABC):
    """Key/value-ish object storage. Keys are bucket-relative paths."""

    @abstractmethod
    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str: ...

    @abstractmethod
    def get_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...

    def put_json(self, key: str, obj: Any) -> str:
        body = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return self.put_bytes(key, body, content_type="application/json")

    def get_json(self, key: str) -> Any:
        return json.loads(self.get_bytes(key).decode("utf-8"))

    @abstractmethod
    def location(self, key: str) -> str:
        """Human-readable fully-qualified location (s3://... or a file path)."""


class LocalBackend(StorageBackend):
    """Stores objects as files under ``root``. Used for local runs and tests."""

    def __init__(self, root: str):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return self.location(key)

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        return sorted(
            str(p.relative_to(self.root)).replace(os.sep, "/")
            for p in base.rglob("*")
            if p.is_file()
        )

    def location(self, key: str) -> str:
        return str(self._path(key))


class S3Backend(StorageBackend):
    """Stores objects in S3 via boto3. Used in AWS."""

    def __init__(self, bucket: str, client: Any | None = None):
        self.bucket = bucket
        if client is None:
            import boto3  # imported lazily so local runs don't need boto3

            client = boto3.client("s3")
        self.client = client

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
        return self.location(key)

    def get_bytes(self, key: str) -> bytes:
        resp = self.client.get_object(Bucket=self.bucket, Key=key)
        return resp["Body"].read()

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token: str | None = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            keys.extend(obj["Key"] for obj in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(keys)

    def location(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"


def get_backend(client: Any | None = None) -> StorageBackend:
    """Choose a backend from the environment.

    - ``STORAGE_BACKEND=local`` (or no ``DATA_BUCKET``) -> LocalBackend at
      ``LOCAL_LAKE_DIR`` (default ``./.local_lake``).
    - otherwise -> S3Backend on ``DATA_BUCKET``.
    """
    backend = os.environ.get("STORAGE_BACKEND")
    bucket = os.environ.get("DATA_BUCKET")
    if backend == "local" or (backend is None and not bucket):
        return LocalBackend(os.environ.get("LOCAL_LAKE_DIR", "./.local_lake"))
    if not bucket:
        raise RuntimeError("DATA_BUCKET must be set when STORAGE_BACKEND is not 'local'")
    return S3Backend(bucket, client=client)


# --------------------------------------------------------------------------- #
# Partition / key helpers                                                     #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class RunContext:
    """Identifies a single hourly run. Everything keys off this."""

    dt: str          # YYYY-MM-DD  (partition value)
    hour: str        # HH          (00-23, UTC)
    run_ts: str      # YYYYMMDDTHHMMSSZ (full ingest timestamp, UTC)

    @staticmethod
    def from_datetime(now) -> "RunContext":
        return RunContext(
            dt=now.strftime("%Y-%m-%d"),
            hour=now.strftime("%H"),
            run_ts=now.strftime("%Y%m%dT%H%M%SZ"),
        )

    def raw_key(self, city_id: str) -> str:
        return f"{RAW_PREFIX}/dt={self.dt}/run_hour={self.hour}/{city_id}.json"

    def raw_prefix(self) -> str:
        return f"{RAW_PREFIX}/dt={self.dt}/run_hour={self.hour}/"

    def clean_key(self) -> str:
        # Deterministic per (dt, hour) -> re-running an hour overwrites cleanly.
        return f"{CLEAN_PREFIX}/dt={self.dt}/weather-{self.dt}-{self.hour}.parquet"

    def quarantine_key(self) -> str:
        return f"{QUARANTINE_PREFIX}/dt={self.dt}/quarantine-{self.dt}-{self.hour}.json"


# --------------------------------------------------------------------------- #
# Parquet helpers (pyarrow)                                                    #
# --------------------------------------------------------------------------- #

# Column order + types for the clean weather table. Keeping this explicit means
# the Parquet schema is stable run-to-run (so the Glue crawler never flip-flops).
PARQUET_SCHEMA_FIELDS: list[tuple[str, str]] = [
    ("observation_id", "string"),
    ("city_id", "string"),
    ("city_name", "string"),
    ("country", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("timezone", "string"),
    ("observed_at", "timestamp"),
    ("temperature_c", "double"),
    ("apparent_temperature_c", "double"),
    ("relative_humidity_pct", "double"),
    ("precipitation_mm", "double"),
    ("wind_speed_ms", "double"),
    ("wind_direction_deg", "double"),
    ("pressure_msl_hpa", "double"),
    ("cloud_cover_pct", "double"),
    ("weather_code", "int64"),
    ("is_day", "int64"),
    ("ingested_at", "timestamp"),
]


def _pyarrow_schema():
    import pyarrow as pa  # type: ignore

    mapping = {
        "string": pa.string(),
        "double": pa.float64(),
        "int64": pa.int64(),
        "timestamp": pa.timestamp("us"),
    }
    return pa.schema([(name, mapping[kind]) for name, kind in PARQUET_SCHEMA_FIELDS])


def records_to_parquet_bytes(records: Iterable[dict[str, Any]]) -> bytes:
    """Serialize records to a single Parquet file (snappy) as bytes."""
    import datetime as _dt

    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    schema = _pyarrow_schema()
    columns: dict[str, list[Any]] = {name: [] for name, _ in PARQUET_SCHEMA_FIELDS}
    for rec in records:
        for name, kind in PARQUET_SCHEMA_FIELDS:
            val = rec.get(name)
            if kind == "timestamp" and isinstance(val, str) and val:
                val = _parse_ts(val)
            columns[name].append(val)
    table = pa.table(columns, schema=schema)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()


def _parse_ts(value: str):
    """Parse Open-Meteo / ISO timestamps into naive UTC datetimes."""
    import datetime as _dt

    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dt = _dt.datetime.fromisoformat(v)
    except ValueError:
        # Open-Meteo "current.time" looks like "2024-01-01T13:00"
        dt = _dt.datetime.strptime(v, "%Y-%m-%dT%H:%M")
    if dt.tzinfo is not None:
        dt = dt.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return dt
