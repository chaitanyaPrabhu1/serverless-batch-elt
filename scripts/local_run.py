#!/usr/bin/env python3
"""Run the full ingest -> transform flow locally against a filesystem "lake".

Hits the real (free, no-auth) Open-Meteo API but writes raw JSON + clean Parquet
under ./.local_lake so you can inspect the output without an AWS account:

    make local-run
    # then look at .local_lake/raw/... and .local_lake/clean/...
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

# Make the lambda/ package importable when run from the repo root.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "lambda"))

import common  # noqa: E402
import ingest  # noqa: E402
import transform  # noqa: E402


def main() -> int:
    os.environ.setdefault("STORAGE_BACKEND", "local")
    os.environ.setdefault("LOCAL_LAKE_DIR", str(ROOT / ".local_lake"))
    backend = common.get_backend()
    now = dt.datetime.now(dt.timezone.utc)

    print(f"== ingest ==  (lake: {os.environ['LOCAL_LAKE_DIR']})")
    ingest_result = ingest.run(now=now, backend=backend)
    print(f"   wrote {ingest_result['written']} raw files, {ingest_result['failed']} failed")

    print("== transform ==")
    tr = transform.run(event=ingest_result, now=now, backend=backend)
    print(f"   clean rows: {tr['rows_clean']}  quarantined: {tr['rows_quarantined']}")
    print(f"   clean file: {tr['clean_location']}")

    # Quick read-back to prove the Parquet is valid.
    if tr["clean_location"]:
        import pyarrow.parquet as pq

        table = pq.read_table(tr["clean_location"])
        print(f"   parquet verified: {table.num_rows} rows x {table.num_columns} cols")
        print("   sample:", table.slice(0, 1).to_pylist())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
