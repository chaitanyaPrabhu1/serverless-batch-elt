"""Airflow DAG: orchestrate the serverless weather ELT pipeline.

Flow:  ingest -> transform -> crawl -> dbt_run -> dbt_test

Runs hourly. The ingest task invokes the ingest Lambda; its return value (the
partition it wrote: dt / hour / run_ts) is passed via XCom to the transform
Lambda so both operate on exactly the same partition. The Glue crawler then
refreshes the catalog, and dbt builds + tests the warehouse.

Config comes from Airflow Variables (or env vars), so nothing AWS-specific is
hard-coded:
    DATA_BUCKET, AWS_REGION, GLUE_DATABASE, GLUE_CRAWLER,
    INGEST_LAMBDA, TRANSFORM_LAMBDA, ATHENA_WORKGROUP, DBT_DIR
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _cfg(key: str, default: str | None = None) -> str:
    """Read config from an Airflow Variable, falling back to env, then default."""
    try:
        return Variable.get(key)
    except KeyError:
        val = os.environ.get(key, default)
        if val is None:
            raise
        return val


REGION = _cfg("AWS_REGION", "us-east-1")
DBT_DIR = _cfg("DBT_DIR", "/opt/airflow/dbt")


def _lambda_client():
    import boto3

    return boto3.client("lambda", region_name=REGION)


def invoke_ingest(**context):
    resp = _lambda_client().invoke(
        FunctionName=_cfg("INGEST_LAMBDA", "weather-elt-ingest"),
        InvocationType="RequestResponse",
        Payload=b"{}",
    )
    payload = json.loads(resp["Payload"].read() or b"{}")
    if resp.get("FunctionError"):
        raise RuntimeError(f"ingest Lambda failed: {payload}")
    # Pass the written partition forward to the transform task.
    return {k: payload[k] for k in ("dt", "hour", "run_ts")}


def invoke_transform(**context):
    ti = context["ti"]
    event = ti.xcom_pull(task_ids="ingest")
    resp = _lambda_client().invoke(
        FunctionName=_cfg("TRANSFORM_LAMBDA", "weather-elt-transform"),
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode("utf-8"),
    )
    payload = json.loads(resp["Payload"].read() or b"{}")
    if resp.get("FunctionError"):
        raise RuntimeError(f"transform Lambda failed: {payload}")
    return payload


def run_crawler(**context):
    import boto3

    glue = boto3.client("glue", region_name=REGION)
    crawler = _cfg("GLUE_CRAWLER", "weather-elt-crawler")
    try:
        glue.start_crawler(Name=crawler)
    except glue.exceptions.CrawlerRunningException:
        pass  # already running; just wait for it below
    # Poll until the crawler returns to READY (cap at ~10 minutes).
    for _ in range(60):
        state = glue.get_crawler(Name=crawler)["Crawler"]["State"]
        if state == "READY":
            return "crawler ready"
        time.sleep(10)
    raise TimeoutError(f"crawler {crawler} did not finish in time")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

# dbt commands share this environment (AWS creds come from the worker's role).
dbt_env = {
    "DATA_BUCKET": _cfg("DATA_BUCKET", "REPLACE_ME"),
    "AWS_REGION": REGION,
    "GLUE_DATABASE": _cfg("GLUE_DATABASE", "weather_elt"),
    "ATHENA_WORKGROUP": _cfg("ATHENA_WORKGROUP", "weather_elt"),
    "DBT_PROFILES_DIR": DBT_DIR,
    "PATH": os.environ.get("PATH", ""),
}

with DAG(
    dag_id="weather_elt",
    description="Serverless batch ELT: Open-Meteo -> S3 -> Glue -> Athena -> dbt",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["aws", "elt", "dbt", "serverless"],
) as dag:

    ingest = PythonOperator(task_id="ingest", python_callable=invoke_ingest)

    transform = PythonOperator(task_id="transform", python_callable=invoke_transform)

    crawl = PythonOperator(task_id="crawl", python_callable=run_crawler)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --target prod",
        env=dbt_env,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --target prod",
        env=dbt_env,
        append_env=True,
    )

    ingest >> transform >> crawl >> dbt_run >> dbt_test
