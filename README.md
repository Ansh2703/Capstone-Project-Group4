# Maven Market — End-to-End Data Engineering Pipeline

> **Team:** Capstone Project Group 4 (Rajni, Devjit, Ojasvi, Snigdha)
> **Platform:** Azure Databricks · Unity Catalog · Lakeflow Spark Declarative Pipelines
> **Repo:** Databricks Asset Bundles (DAB) with GitHub Actions CI/CD

---

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│                                                                  │
│  ┌───────────┐   ┌─────────────┐   ┌─────────────────────────┐  │
│  │ MongoDB   │   │   Kafka     │   │  Azure Blob Storage     │  │
│  │ Atlas     │   │  Streams    │   │  (ADLS Gen2 / CSV)      │  │
│  │           │   │             │   │                         │  │
│  │ customers │   │ orders      │   │ transactions  stores    │  │
│  │ products  │   │ inventory   │   │ regions  returns        │  │
│  │           │   │             │   │ calendar                │  │
│  └─────┬─────┘   └──────┬──────┘   └────────────┬────────────┘  │
└────────┼────────────────┼───────────────────────┼────────────────┘
         │                │                       │
         ▼                ▼                       ▼
┌──────────────────────────────────────────────────────────────────┐
│               AZURE DATABRICKS WORKSPACE                         │
│               Unity Catalog: maven_market_uc                     │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │            MEDALLION ARCHITECTURE (SDP)                     │  │
│  │                                                            │  │
│  │  BRONZE (raw ingestion) ─── schema: bronze                 │  │
│  │    ingest_mongo.py ──► bronze_customers, bronze_products    │  │
│  │    ingest_csv.py ────► bronze_transactions, stores,         │  │
│  │                        regions, returns, calendar            │  │
│  │    ingest_kafka_*.py ► bronze_orders, bronze_inventory      │  │
│  │                              │                              │  │
│  │                              ▼                              │  │
│  │  SILVER (cleansed) ──── schema: silver                      │  │
│  │    silver_mongo_dlt.py ► from_json + type cast              │  │
│  │    silver_csv_dlt.py ──► date parse + validation            │  │
│  │    silver_kafka_dlt.py ► stream normalize                   │  │
│  │    DLT Expectations: NULL rejection, PK/FK, ranges          │  │
│  │                              │                              │  │
│  │                              ▼                              │  │
│  │  GOLD (business-ready) ─ schema: gold                       │  │
│  │    gold_dlt.py:                                             │  │
│  │    DIMS: dim_customer, dim_product, dim_store, dim_date     │  │
│  │    FACTS: fact_sales (revenue/cost/profit), fact_returns     │  │
│  │    AGGS: executive_overview, regional_sales, customer_ltv,  │  │
│  │          inventory_alerts, orders_per_minute, space_util     │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  GOVERNANCE (Unity Catalog) — currently disabled for dev    │  │
│  │  apply_permissions.sql — tiered schema access               │  │
│  │  apply_rls.sql — row-level security on regional_sales       │  │
│  │  apply_cls.sql — PII column masking on dim_customer         │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## CI/CD Pipeline

```
  Developer pushes          GitHub Actions (deploy.yml)
  to main/dev or     ┌──────────────────────────────────────┐
  opens a PR ────────►  Job 1: TEST                          │
                     │  Setup Java 17 + Python 3.12          │
                     │  pip install pyspark==3.5.3 pytest     │
                     │  pytest tests/ (53 passed, 4 skipped)  │
                     │         │ pass                         │
                     │         ▼                              │
                     │  Job 2: DEPLOY (skip on PRs)           │
                     │  Install Databricks CLI                 │
                     │  databricks bundle deploy -t $TARGET    │
                     │  Run pipeline (manual dispatch only)    │
                     └──────────────────────────────────────┘
```

---

## Job Orchestration

```
┌──────────────────────────────────────────────────────┐
│     [${target}] Full Project Orchestration            │
│                                                      │
│  ┌────────────┐                                      │
│  │ Step 0:    │  pytest via spark_python_task         │
│  │ run_tests  │  (job cluster)                       │
│  └─────┬──────┘                                      │
│        ▼                                             │
│  ┌─────────────────┐                                 │
│  │ Step 1:         │  SDP pipeline (serverless)      │
│  │ run_bronze      │  9 streaming tables             │
│  └─────┬───────────┘                                 │
│        ▼                                             │
│  ┌─────────────────┐                                 │
│  │ Step 2:         │  SDP pipeline (serverless)      │
│  │ run_silver      │  cleanse + validate + enrich    │
│  └─────┬───────────┘                                 │
│        ▼                                             │
│  ┌─────────────────┐                                 │
│  │ Step 3:         │  SDP pipeline (serverless)      │
│  │ run_gold        │  dims + facts + aggregations    │
│  └─────────────────┘                                 │
└──────────────────────────────────────────────────────┘
```

---

## Data Flow

```
MongoDB Atlas           Kafka              Azure Blob (CSV)
┌───────────┐      ┌───────────┐      ┌──────────────────┐
│ customers │      │  orders   │      │ transactions     │
│ products  │      │ inventory │      │ stores  regions  │
└─────┬─────┘      └─────┬─────┘      │ returns calendar │
      │                  │            └────────┬─────────┘
      ▼                  ▼                     ▼
┌──────────────────────────────────────────────────────┐
│ BRONZE — Raw, append-only streaming tables            │
│ Auto Loader / read_stream — no transforms             │
└────────────────────────┬─────────────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────┐
│ SILVER — Cleansed and typed                           │
│ from_json() for MongoDB, to_date("M/d/yyyy"),         │
│ .cast() for type safety, DLT expectations,            │
│ deduplication on primary keys                         │
└────────────────────────┬─────────────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────┐
│ GOLD — Business-ready star schema + agg tables        │
│  Star Schema: fact_sales/returns joined to dims       │
│  Pre-built Aggs: ready for dashboards                 │
└──────────────────────────────────────────────────────┘
```

---

## Test Suite (57 tests)

| File | Tests | Coverage |
|---|---|---|
| test_ingestion.py | 13 | JSON parsing, date parsing, type casting |
| test_transformations.py | 13 | margin %, full name, revenue/cost/profit |
| test_data_quality.py | 11 | NULL PK/FK, price validation, gender |
| test_gold_aggregations.py | 10 | All 6 gold aggregation tables |
| test_scd2_and_joins.py | 11 | SCD-2 filtering, fact-dim joins, dedup |

| Environment | Result |
|---|---|
| GitHub Actions (PySpark 3.5.3) | 53 passed, 4 skipped |
| Databricks Runtime | 57 passed |

---

## Project Structure

```
Capstone-Project-Group4/
├── .github/workflows/
│   ├── deploy.yml              CI/CD (test then deploy)
│   └── test.yml                (redirects to deploy.yml)
├── maven_market/
│   ├── databricks.yml          DAB bundle config (dev + main)
│   ├── src/
│   │   ├── pipelines/
│   │   │   ├── bronze/         4 ingestion scripts
│   │   │   ├── silver/         3 cleansing scripts
│   │   │   └── gold/           1 business logic script
│   │   ├── governance/         RLS, CLS, permissions (disabled)
│   │   └── utils/              config, logger, validators
│   ├── resources/
│   │   ├── dlt_pipeline.yml    3 SDP pipelines (all serverless)
│   │   ├── jobs.yml            Orchestration job definition
│   │   └── setup_job.yml       Initial setup
│   ├── config/                 dev + prod YAML configs
│   ├── tests/                  57 unit tests + runner
│   ├── scripts/                UC setup script
│   └── policies/               Cluster policy JSON
└── README.md
```

---

## Cluster Permission Error

The job fails with `PERMISSION_DENIED: You are not authorized to create clusters` because the `run_tests` task uses `job_cluster_key: default_cluster` which provisions a new cluster. The SDP pipeline tasks use serverless and work fine.

**Fix:** Switch `run_tests` to serverless compute in `jobs.yml`.

---

## Key Technologies

| Component | Technology |
|---|---|
| Cloud | Azure (ADLS Gen2, Databricks) |
| Lakehouse | Delta Lake on Unity Catalog |
| Pipelines | Lakeflow Spark Declarative Pipelines |
| Orchestration | Lakeflow Jobs (DAB-managed) |
| CI/CD | GitHub Actions + Databricks Asset Bundles |
| Testing | pytest + PySpark |
| Governance | UC Permissions, Row Filters, Column Masks |
