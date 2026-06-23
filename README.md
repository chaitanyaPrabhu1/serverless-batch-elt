# Serverless Batch ELT Pipeline on AWS

> Pull weather data from a public API on a schedule, land it in S3, transform it into clean
> analytics tables with dbt, validate it with quality tests, and orchestrate the whole thing so it
> runs itself.

**Resume line**

> Built an automated serverless ELT pipeline on AWS: ingest the Open-Meteo weather API hourly via
> Lambda → S3 (Parquet, date-partitioned) → cataloged with Glue → transformed with dbt on Athena →
> validated with data-quality tests, orchestrated by Airflow.

---

## Architecture

```
 Open-Meteo API (free, no auth)
     │  EventBridge schedule (hourly)
     ▼
 AWS Lambda  ── ingest ──▶  S3 raw zone   (raw/weather/dt=YYYY-MM-DD/run_hour=HH/<city>.json)
     │                          │
     │                          ▼  flatten + convert + partition
 AWS Lambda  ── transform ─▶  S3 clean zone (clean/weather/dt=YYYY-MM-DD/*.parquet)
     │                          │            └── malformed rows → quarantine/weather/...
     │                          ▼
     │                   Glue Data Catalog  ◀── Glue Crawler
     │                          │
     │                          ▼
     │                       Athena  ◀── dbt (staging → marts) + dbt tests
     │                          │
     ▼                          ▼
 CloudWatch logs       clean, queryable analytics tables (fct/dim/agg)

 Apache Airflow orchestrates:  ingest → transform → crawl → dbt run → dbt test
```

## What this pipeline does and why

Every hour, a Lambda fetches the *current* weather for a list of cities from the Open-Meteo API and
writes one raw JSON file per city to the S3 **raw zone**, partitioned by date. A second Lambda
flattens those JSON snapshots into a typed, columnar **Parquet** file in the S3 **clean zone**, again
date-partitioned. A Glue crawler catalogs the clean zone so Athena can query it with SQL. dbt then
builds layered analytics models (`staging → marts`) on top of Athena and runs data-quality tests.
Airflow ties the whole thing together on a schedule.

It mirrors a real AWS data-engineering job almost exactly: **scheduled ingestion → lake → catalog →
warehouse transform → quality gate → orchestration**, all serverless.

## Design choices (own these in an interview)

- **Why partition by date (`dt=YYYY-MM-DD`)?** Athena scans (and bills) by data read. Date partitions
  let queries prune to the days they need instead of scanning the whole lake. The crawler registers
  `dt` as a partition key automatically.
- **How are re-runs idempotent?** The clean-zone Parquet file for a given hour is written to a
  **deterministic key** (`weather-<dt>-<hour>.parquet`). Re-running the same hour overwrites that one
  file in place — no duplicates, no append drift. The unit of idempotency is the *ingest hour*.
- **How is bad data handled?** The transform validates every record (required fields present, numeric
  ranges sane). Malformed records are **quarantined** to `quarantine/weather/...` instead of failing
  the whole run, so one bad city never blocks the other nine. The run still succeeds; the quarantine
  count is logged and surfaced as a metric.
- **Why JSON raw → Parquet clean (two zones)?** The raw zone is an immutable, replayable record of
  exactly what the API returned (cheap insurance — you can always rebuild clean from raw). The clean
  zone is typed, compressed, columnar, and cheap to query.
- **Data-quality tests** — dbt tests for `not_null`, `unique`, `accepted_values`, `relationships`,
  and **source freshness** (alert if no new data lands).
- **Batch vs streaming — when would you pick each?** Batch (this project) fits periodic, tolerant-of-
  latency workloads where simplicity and cost matter. Streaming fits when freshness is measured in
  seconds and you process events as they arrive. Weather updated hourly is a textbook batch case.

## Repository layout

```
serverless-batch-elt/
├── README.md
├── Makefile                  # one-liners: local-run, package, plan, apply, dbt
├── requirements-dev.txt
├── config/
│   └── cities.json           # the locations we ingest
├── lambda/
│   ├── ingest.py             # API → S3 raw JSON
│   ├── transform.py          # raw JSON → S3 clean Parquet (+ quarantine)
│   ├── common.py             # shared config / s3 helpers
│   └── requirements.txt      # awswrangler etc. (provided via Lambda layer in prod)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/          # stg_weather (+ source defs, freshness, tests)
│       └── marts/            # dim_location, fct_weather_observation, agg_weather_daily
├── airflow/
│   ├── dags/weather_elt_dag.py
│   └── docker-compose.yaml
├── terraform/                # S3, Lambda x2, Glue, Athena, IAM, EventBridge
└── tests/
    ├── conftest.py
    ├── test_ingest.py
    └── test_transform.py
```

## Quickstart

### 1. Run the Python locally (no AWS needed)

```bash
make venv          # create .venv and install dev deps
make local-run     # ingest + transform end-to-end into ./.local_lake/ using a fake S3
make test          # unit tests (moto-mocked S3)
```

`make local-run` hits the real Open-Meteo API (it's free/no-auth) but writes to a local filesystem
"lake" so you can inspect the raw JSON and the Parquet output without an AWS account.

### 2. Deploy to AWS

```bash
cd terraform
terraform init
terraform apply -var="project=weather-elt" -var="alarm_email=you@example.com"
```

This provisions the S3 bucket, both Lambdas (with the AWS-managed pandas/pyarrow layer), the Glue
database + crawler, an Athena workgroup, IAM roles, an hourly EventBridge schedule, and a billing
alarm. Outputs include the bucket name and Glue database to drop into `dbt/profiles.yml`.

### 3. Build the warehouse with dbt

```bash
cd dbt
dbt deps
dbt run --target prod      # staging → marts
dbt test --target prod     # data-quality gate
dbt docs generate          # lineage graph for the README screenshot
```

### 4. Orchestrate with Airflow

```bash
cd airflow
docker compose up -d        # Airflow at http://localhost:8080  (admin / admin)
# Enable the `weather_elt` DAG; it runs ingest → transform → crawl → dbt run → dbt test hourly.
```

## Cost & teardown

Everything is serverless and sized for the **Free Tier**: Lambda (free tier covers the hourly runs),
S3 (pennies), Athena (pay-per-scan, kept tiny by partitioning), Glue crawler (on-demand). A billing
alarm is provisioned by Terraform. **Tear down when done:**

```bash
cd terraform && terraform destroy
```

## Headline numbers (fill in after a few runs)

- Ingests **N cities/hour** → ~**24·N rows/day**.
- **K dbt data-quality tests** gate every build.
- Runs **hourly, unattended**, idempotent on re-run.
