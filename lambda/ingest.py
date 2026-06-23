"""Ingest Lambda: pull current weather per city from Open-Meteo -> S3 raw zone.

Writes one raw JSON file per city under
``raw/weather/dt=YYYY-MM-DD/run_hour=HH/<city_id>.json``. The raw file is an
immutable record of exactly what the API returned, plus the city metadata and an
``ingested_at`` stamp, so the clean zone can always be rebuilt from raw.

Entry points:
- ``handler(event, context)``  — AWS Lambda entry point.
- ``run(...)``                 — plain-Python entry point (local runs / tests).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import urllib.parse
import urllib.request
from typing import Any

import common

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HTTP_TIMEOUT_S = 15


def _build_url(city: dict[str, Any]) -> str:
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": ",".join(common.CURRENT_FIELDS),
        "timezone": "UTC",
    }
    return f"{common.API_BASE_URL}?{urllib.parse.urlencode(params)}"


def _fetch(url: str, opener=urllib.request.urlopen) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "serverless-batch-elt/1.0"})
    with opener(req, timeout=HTTP_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run(
    now: dt.datetime | None = None,
    backend: common.StorageBackend | None = None,
    cities: list[dict[str, Any]] | None = None,
    fetch=_fetch,
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.timezone.utc)
    backend = backend or common.get_backend()
    cities = cities if cities is not None else common.load_cities()
    ctx = common.RunContext.from_datetime(now)

    written, failed = [], []
    for city in cities:
        try:
            payload = fetch(_build_url(city))
            record = {
                "city": {k: city[k] for k in (
                    "city_id", "city_name", "country", "latitude", "longitude", "timezone",
                )},
                "ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "open-meteo",
                "api_response": payload,
            }
            key = ctx.raw_key(city["city_id"])
            backend.put_json(key, record)
            written.append(key)
            logger.info("ingested %s -> %s", city["city_id"], backend.location(key))
        except Exception as exc:  # noqa: BLE001 - one bad city must not kill the run
            failed.append({"city_id": city.get("city_id"), "error": str(exc)})
            # Non-fatal and expected (transient API/network errors); log concisely
            # rather than dumping a full traceback for a single handled city.
            logger.warning("failed to ingest %s: %s", city.get("city_id"), exc)

    result = {
        "dt": ctx.dt,
        "hour": ctx.hour,
        "run_ts": ctx.run_ts,
        "raw_prefix": ctx.raw_prefix(),
        "written": len(written),
        "failed": len(failed),
        "failures": failed,
    }
    logger.info("ingest summary: %s", json.dumps(result))
    if not written:
        # Every city failed -> surface as an error so the orchestrator retries.
        raise RuntimeError(f"ingest produced 0 files: {failed}")

    # Serverless chaining: when running on EventBridge without Airflow, kick off
    # the transform Lambda asynchronously for the partition we just wrote. No-op
    # locally / under Airflow (env var unset), where the orchestrator does this.
    _maybe_chain_transform(result)
    return result


def _maybe_chain_transform(event: dict[str, Any]) -> None:
    import os

    target = os.environ.get("CHAIN_TRANSFORM_LAMBDA")
    if not target:
        return
    try:
        import boto3

        boto3.client("lambda").invoke(
            FunctionName=target,
            InvocationType="Event",  # async fire-and-forget
            Payload=json.dumps({k: event[k] for k in ("dt", "hour", "run_ts")}).encode("utf-8"),
        )
        logger.info("chained transform Lambda %s for dt=%s hour=%s", target, event["dt"], event["hour"])
    except Exception:  # noqa: BLE001 - chaining is best-effort
        logger.exception("failed to chain transform Lambda %s", target)


def handler(event, context):  # noqa: ANN001 - Lambda signature
    return run()
