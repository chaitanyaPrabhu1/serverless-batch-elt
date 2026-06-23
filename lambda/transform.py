"""Transform Lambda: raw JSON snapshots -> typed, partitioned Parquet (clean zone).

Reads every raw file for one ``(dt, run_hour)`` partition, flattens the
Open-Meteo response into a flat record, **validates** it, and writes:

- valid records  -> ``clean/weather/dt=YYYY-MM-DD/weather-<dt>-<hour>.parquet``
                    (deterministic key -> idempotent on re-run)
- invalid records -> ``quarantine/weather/dt=.../quarantine-<dt>-<hour>.json``
                    (one bad city never fails the whole run)

Entry points mirror ingest.py: ``handler(event, context)`` and ``run(...)``.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
from typing import Any

import common

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _num(value: Any) -> float | None:
    """Coerce to float, returning None for missing/NaN/non-numeric values."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _int(value: Any) -> int | None:
    f = _num(value)
    return None if f is None else int(f)


def flatten(raw: dict[str, Any], ingested_fallback: str) -> dict[str, Any]:
    """Map a raw API record onto the clean schema. Does not validate."""
    city = raw.get("city", {})
    current = (raw.get("api_response") or {}).get("current") or {}
    observed_at = current.get("time")
    city_id = city.get("city_id")
    return {
        "observation_id": f"{city_id}:{observed_at}" if city_id and observed_at else None,
        "city_id": city_id,
        "city_name": city.get("city_name"),
        "country": city.get("country"),
        "latitude": _num(city.get("latitude")),
        "longitude": _num(city.get("longitude")),
        "timezone": city.get("timezone"),
        "observed_at": observed_at,
        "temperature_c": _num(current.get("temperature_2m")),
        "apparent_temperature_c": _num(current.get("apparent_temperature")),
        "relative_humidity_pct": _num(current.get("relative_humidity_2m")),
        "precipitation_mm": _num(current.get("precipitation")),
        "wind_speed_ms": _num(current.get("wind_speed_10m")),
        "wind_direction_deg": _num(current.get("wind_direction_10m")),
        "pressure_msl_hpa": _num(current.get("pressure_msl")),
        "cloud_cover_pct": _num(current.get("cloud_cover")),
        "weather_code": _int(current.get("weather_code")),
        "is_day": _int(current.get("is_day")),
        "ingested_at": raw.get("ingested_at") or ingested_fallback,
    }


def validate(rec: dict[str, Any]) -> str | None:
    """Return a reason string if the record is bad, else None."""
    if not rec.get("city_id"):
        return "missing city_id"
    if not rec.get("observed_at"):
        return "missing observed_at"
    if not rec.get("observation_id"):
        return "missing observation_id"
    temp = rec.get("temperature_c")
    if temp is None:
        return "missing temperature_c"
    if not (common.TEMP_MIN_C <= temp <= common.TEMP_MAX_C):
        return f"temperature_c out of range: {temp}"
    hum = rec.get("relative_humidity_pct")
    if hum is not None and not (common.HUMIDITY_MIN <= hum <= common.HUMIDITY_MAX):
        return f"relative_humidity_pct out of range: {hum}"
    return None


def run(
    event: dict[str, Any] | None = None,
    now: dt.datetime | None = None,
    backend: common.StorageBackend | None = None,
) -> dict[str, Any]:
    event = event or {}
    now = now or dt.datetime.now(dt.timezone.utc)
    backend = backend or common.get_backend()

    # Prefer the partition handed over by the ingest task; fall back to "now".
    derived = common.RunContext.from_datetime(now)
    ctx = common.RunContext(
        dt=event.get("dt", derived.dt),
        hour=event.get("hour", derived.hour),
        run_ts=event.get("run_ts", derived.run_ts),
    )
    ingested_fallback = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    raw_keys = [k for k in backend.list_keys(ctx.raw_prefix()) if k.endswith(".json")]
    if not raw_keys:
        raise RuntimeError(f"no raw files found under {ctx.raw_prefix()}")

    valid: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for key in raw_keys:
        try:
            raw = backend.get_json(key)
        except Exception as exc:  # noqa: BLE001
            quarantined.append({"source_key": key, "reason": f"unreadable: {exc}", "record": None})
            continue
        rec = flatten(raw, ingested_fallback)
        reason = validate(rec)
        if reason:
            quarantined.append({"source_key": key, "reason": reason, "record": rec})
            continue
        if rec["observation_id"] in seen_ids:
            # De-dupe within the run so the uniqueness test downstream holds.
            quarantined.append({"source_key": key, "reason": "duplicate observation_id", "record": rec})
            continue
        seen_ids.add(rec["observation_id"])
        valid.append(rec)

    clean_location = None
    if valid:
        parquet = common.records_to_parquet_bytes(valid)
        clean_key = ctx.clean_key()
        clean_location = backend.put_bytes(clean_key, parquet, content_type="application/octet-stream")
        logger.info("wrote %d clean rows -> %s", len(valid), clean_location)

    quarantine_location = None
    if quarantined:
        quarantine_key = ctx.quarantine_key()
        quarantine_location = backend.put_json(quarantine_key, quarantined)
        logger.warning("quarantined %d records -> %s", len(quarantined), quarantine_location)

    result = {
        "dt": ctx.dt,
        "hour": ctx.hour,
        "rows_clean": len(valid),
        "rows_quarantined": len(quarantined),
        "clean_location": clean_location,
        "quarantine_location": quarantine_location,
        "clean_partition": f"dt={ctx.dt}",
    }
    logger.info("transform summary: %s", json.dumps(result))
    return result


def handler(event, context):  # noqa: ANN001 - Lambda signature
    return run(event=event or {})
