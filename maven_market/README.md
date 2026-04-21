# Maven Market — CI/CD & Project Documentation

> **Bundle Name:** `maven_market_v3`
> **Cloud:** Microsoft Azure
> **Catalog:** `maven_market_uc` (Unity Catalog)
> **Compute:** Serverless
> **Framework:** Databricks Declarative Automation Bundles (DABs)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Structure](#2-repository-structure)
3. [Architecture — Medallion Lakehouse](#3-architecture--medallion-lakehouse)
4. [Data Sources](#4-data-sources)
5. [Pipeline Details](#5-pipeline-details)
   - 5.1 [Bronze Layer — Raw Ingestion](#51-bronze-layer--raw-ingestion)
   - 5.2 [Silver Layer — Cleansing & Conforming](#52-silver-layer--cleansing--conforming)
   - 5.3 [Gold Layer — Business Aggregations](#53-gold-layer--business-aggregations)
6. [Job Orchestration](#6-job-orchestration)
7. [Testing Strategy](#7-testing-strategy)
8. [Environment Configuration](#8-environment-configuration)
9. [Deployment Targets (CI/CD)](#9-deployment-targets-cicd)
10. [Deployment Workflow](#10-deployment-workflow)
11. [Data Governance](#11-data-governance)
12. [Infrastructure Setup](#12-infrastructure-setup)
13. [Operational Runbook](#13-operational-runbook)
14. [Bundle CLI Reference](#14-bundle-cli-reference)

---

## 1. Project Overview

Maven Market is an end-to-end data engineering platform that ingests retail data from multiple sources (CSV files, Confluent Kafka streams, MongoDB/Atlas via Delta), transforms it through a medallion architecture (Bronze → Silver → Gold), and produces analytics-ready aggregation tables for executive dashboards, regional sales reporting, customer lifetime value analysis, and real-time operations monitoring.

The entire project is managed as a **Databricks Declarative Automation Bundle**, enabling version-controlled, reproducible deployments across `dev` and `main` (production) targets.

---

## 2. Repository Structure

```
maven_market/
├── databricks.yml                    # Bundle root config — variables, targets, includes
├── README.md                         # This documentation
├── requirements.txt                  # Python dependencies
├── .gitignore                        # Excludes .databricks/
│
├── resources/                        # DABs resource definitions
│   ├── dlt_pipeline.yml              # Bronze, Silver, Gold pipeline definitions
│   ├── jobs.yml                      # Orchestration job with test gate
│   └── setup_job.yml                 # (Archived) Initial infrastructure setup job
│
├── config/
│   ├── dev_config.yaml               # Dev environment configuration
│   ├── prod_config.yaml              # Production environment configuration (TBD)
│   └── schemas/                      # JSON schemas (placeholder)
│       ├── transactions_schema.json
│       ├── orders_schema.json
│       └── products_schema.json
│
├── src/
│   ├── pipelines/
│   │   ├── bronze/                   # Raw ingestion layer
│   │   │   ├── ingest_csv.py         # Auto Loader for CSV files (transactions, regions, stores, return, calendar)
│   │   │   ├── ingest_kafka_orders.py    # Confluent Kafka → orders stream
│   │   │   ├── ingest_kafka_inventory.py # Confluent Kafka → inventory stream
│   │   │   └── ingest_mongo.py       # MongoDB Atlas (customers, products) via Delta
│   │   ├── silver/                   # Cleansing and conforming layer
│   │   │   ├── silver_csv_dlt.py     # Transactions, returns, stores (SCD-2), regions (SCD-1), calendar
│   │   │   ├── silver_kafka_dlt.py   # Orders (stream-static join), inventory (stock classification)
│   │   │   └── silver_mongo_dlt.py   # Customers (SCD-2, PII), products (SCD-2, margin calc)
│   │   └── gold/
│   │       └── gold_dlt.py           # Dimensions, facts, and business aggregation tables
│   ├── governance/
│   │   ├── apply_rls.sql             # Row-Level Security (region-based, currently disabled for dev)
│   │   ├── apply_cls.sql             # Column-Level Security (PII masking, currently disabled for dev)
│   │   ├── apply_permissions.sql     # Tiered RBAC (engineers/executives/analysts, currently disabled)
│   │   └── seed_user_region_map.py   # Seed user-to-region assignment table
│   └── utils/
│       ├── __init__.py               # Package init
│       ├── config_parser.py          # YAML config reader + table name builder
│       ├── logger.py                 # PipelineLogger — structured audit logging to maven_market_dev.audit_logs
│       ├── validators.py             # (Placeholder for custom validators)
│       └── spark_utils.py            # (Placeholder for Spark helper functions)
│
├── scripts/
│   └── setup_uc.py                   # (Archived) Bootstrap Unity Catalog catalogs and schemas
│
├── tests/                            # pytest unit test suite
│   ├── conftest.py                   # Shared SparkSession fixture (session-scoped)
│   ├── run_tests.py                  # Test runner — handles workspace filesystem limitations
│   ├── test_ingestion.py             # Bronze → Silver parsing tests (JSON, date, type casting)
│   ├── test_transformations.py       # Silver → Gold business logic (margin, revenue, stock status)
│   ├── test_data_quality.py          # DLT expectation simulation (null rejection, price checks)
│   ├── test_scd2_and_joins.py        # SCD-2 filtering, fact-dim joins, deduplication
│   └── test_gold_aggregations.py     # Gold aggregation correctness (exec overview, LTV, regional)
│
└── policies/
    └── cluster_policy.json           # Cluster policy — single-node Standard_DC4as_v5, auto-terminate 30min
```

---

## 3. Architecture — Medallion Lakehouse

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                       │
│  CSV (ADLS Gen2)  │  Confluent Kafka  │  MongoDB Atlas (via Delta)       │
└────────┬──────────┴────────┬──────────┴────────┬─────────────────────────┘
         │                   │                   │
         ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  BRONZE (Raw Ingestion)                                                   │
│  ├── bronze_transactions    (Auto Loader, CSV streaming)                 │
│  ├── bronze_regions         (Auto Loader, CSV streaming)                 │
│  ├── bronze_stores          (Auto Loader, CSV streaming)                 │
│  ├── bronze_return          (Auto Loader, CSV streaming)                 │
│  ├── bronze_calendar        (Auto Loader, CSV streaming)                 │
│  ├── bronze_orders_kafka    (Kafka structured streaming)                 │
│  ├── bronze_inventory_kafka (Kafka structured streaming)                 │
│  ├── bronze_customers       (Delta streaming from MongoDB export)        │
│  └── bronze_products        (Delta streaming from MongoDB export)        │
└────────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  SILVER (Cleansed & Conformed)                                           │
│  ├── transactions     (fact, date-enriched, quality-enforced)            │
│  ├── returns          (fact, date-enriched, quality-enforced)            │
│  ├── stores           (SCD Type-2: tracks sqft, remodel, store_type)    │
│  ├── regions          (SCD Type-1: latest hierarchy label)              │
│  ├── calendar         (enriched with day_name, is_weekend, etc.)        │
│  ├── orders           (stream-static join with stores for location)     │
│  ├── inventory        (stock health: OUT_OF_STOCK/LOW/MEDIUM/HEALTHY)   │
│  ├── customers        (SCD Type-2: PII, tracks email/address/income)    │
│  └── products         (SCD Type-2: tracks price, cost, recyclable)      │
└────────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  GOLD (Business-Ready)                                                    │
│  DIMENSIONS:                                                              │
│  ├── dim_date           (YYYYMMDD surrogate key, fiscal attributes)      │
│  ├── dim_region         (region reference, SCD-1 snapshot)               │
│  ├── dim_store          (conformed, SCD-2 current, denormalized region)  │
│  ├── dim_customer       (conformed, SCD-2 current, PII columns masked)  │
│  └── dim_product        (conformed, SCD-2 current, with margin_pct)     │
│  FACTS:                                                                   │
│  ├── fact_sales         (grain: transaction line, with revenue/cost/GP)  │
│  └── fact_returns       (grain: return line, with return revenue/cost)   │
│  AGGREGATIONS:                                                            │
│  ├── agg_executive_overview      (Revenue & Profit Margin by Year/Month)│
│  ├── agg_ops_inventory_alerts    (OUT_OF_STOCK & LOW stock alerts)      │
│  ├── agg_ops_orders_per_minute   (Real-time orders throughput)          │
│  ├── agg_regional_sales          (Revenue by store/region, RLS-ready)   │
│  ├── agg_customer_ltv            (Customer Lifetime Value)              │
│  └── agg_store_space_utilization (Revenue per square foot)              │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Sources

| Source | Format | Storage | Ingestion Method | Tables |
| --- | --- | --- | --- | --- |
| CSV flat files | CSV | ADLS Gen2 (`sgmavenmarket1`) | Auto Loader (`cloudFiles`) | transactions, regions, stores, return, calendar |
| Confluent Cloud Kafka | JSON over Kafka | `pkc-56d1g.eastus.azure.confluent.cloud:9092` | Kafka Structured Streaming | orders_topic, inventory_topic |
| MongoDB Atlas | Delta (exported) | ADLS Gen2 (`raw/atlas/mongo_maven_db/`) | Delta Streaming | customers, products |

**ADLS Base Path:** `abfss://maven-market-data@sgmavenmarket1.dfs.core.windows.net/`

---

## 5. Pipeline Details

All pipelines are defined as **Lakeflow Spark Declarative Pipelines** (serverless) in `resources/dlt_pipeline.yml`.

### 5.1 Bronze Layer — Raw Ingestion

**Pipeline:** `[${target}] Maven Market Bronze`
**Target Schema:** `maven_market_uc.bronze`

| File | Tables Created | Method | Key Features |
| --- | --- | --- | --- |
| `ingest_csv.py` | `bronze_transactions`, `bronze_regions`, `bronze_stores`, `bronze_return`, `bronze_calendar` | Auto Loader (CSV streaming) | Dynamic table creation via loop, adds `ingestion_time`, `source_file`, `source_name` |
| `ingest_kafka_orders.py` | `bronze_orders_kafka` | Kafka Structured Streaming | SASL_SSL auth, JSON deserialization with explicit schema |
| `ingest_kafka_inventory.py` | `bronze_inventory_kafka` | Kafka Structured Streaming | SASL_SSL auth, inventory event schema |
| `ingest_mongo.py` | `bronze_customers`, `bronze_products` | Delta Streaming | Reads MongoDB Atlas exports from ADLS, adds `ingestion_time` |

### 5.2 Silver Layer — Cleansing & Conforming

**Pipeline:** `[${target}] Maven Market Silver`
**Target Schema:** `maven_market_uc.silver`

| File | Tables / Views | Key Logic |
| --- | --- | --- |
| `silver_csv_dlt.py` | `transactions`, `returns`, `stores` (SCD-2), `regions` (SCD-1), `calendar` | Date parsing (`M/d/yyyy`), type casting, DLT expectations (`expect_or_drop`, `expect_or_fail`), SCD-2 via `apply_changes` tracking `total_sqft`/`grocery_sqft`/`last_remodel_date`/`store_type` |
| `silver_kafka_dlt.py` | `orders`, `inventory` | Stream-static join (orders ↔ stores for location enrichment), stock health classification (OUT_OF_STOCK/LOW/MEDIUM/HEALTHY) |
| `silver_mongo_dlt.py` | `customers` (SCD-2), `products` (SCD-2) | JSON schema parsing (`from_json`), SCD-2 via `apply_changes` tracking email/address/income (customers) and price/cost (products), margin_pct calculation |

**Data Quality Expectations Applied:**

| Table | Expectation | Action |
| --- | --- | --- |
| transactions | `valid_transaction_date`, `valid_quantity`, `valid_product_id`, `valid_store_id`, `valid_customer_id` | Drop invalid rows |
| transactions | `stock_before_sale` | Warn only |
| returns | `valid_return_date`, `valid_return_quantity`, `valid_return_product`, `valid_return_store` | Drop invalid rows |
| stores | `valid_store_pk` | Fail pipeline |
| stores | `valid_region_fk` | Drop invalid rows |
| customers | `valid_customer_pk` | Fail pipeline |
| customers | `has_email`, `valid_country`, `valid_gender` | Warn only |
| products | `valid_product_pk` | Fail pipeline |
| products | `valid_retail_price`, `valid_cost` | Drop invalid rows |
| products | `price_above_cost` | Warn only |
| orders | `valid_order_id`, `valid_order_quantity`, `valid_order_product` | Drop invalid rows |
| inventory | `valid_inventory_event`, `valid_stock_level` | Drop invalid rows |

### 5.3 Gold Layer — Business Aggregations

**Pipeline:** `[${target}] Maven Market Gold`
**Target Schema:** `maven_market_uc.gold`

| Table | Type | Description |
| --- | --- | --- |
| `dim_date` | Materialized View | YYYYMMDD surrogate keys, fiscal/calendar attributes from `silver.calendar` |
| `dim_region` | Materialized View | Region reference, deduplicated from `silver.regions` |
| `dim_store` | Materialized View | Current SCD-2 snapshot (`__END_AT IS NULL`), denormalized with region |
| `dim_customer` | Materialized View | Current SCD-2 snapshot, PII columns (masked via UC column masks) |
| `dim_product` | Materialized View | Current SCD-2 snapshot, includes `margin_pct` |
| `fact_sales` | Materialized View | Grain: one transaction line. Joins transactions ↔ products. Computes `revenue`, `cost`, `gross_profit` |
| `fact_returns` | Materialized View | Grain: one return line. Joins returns ↔ products. Computes `return_revenue`, `return_cost` |
| `agg_executive_overview` | Materialized View | `total_revenue`, `total_profit`, `profit_margin_pct` by year/month |
| `agg_ops_inventory_alerts` | Materialized View | Filters OUT_OF_STOCK and LOW stock, aggregates by product/store |
| `agg_ops_orders_per_minute` | Materialized View | Orders per minute for real-time operations dashboard |
| `agg_regional_sales` | Materialized View | Revenue by store/region. Row filter applied via `region_filter` function (RLS-ready) |
| `agg_customer_ltv` | Materialized View | Lifetime revenue, profit, total items, avg item value per customer |
| `agg_store_space_utilization` | Materialized View | Revenue and profit per square foot of store space |

---

## 6. Job Orchestration

Defined in `resources/jobs.yml` as `[${target}] Full Project Orchestration`.

```
┌─────────────────┐
│   run_tests     │  Step 0: pytest unit tests (gate)
│   (Python task) │  Environment: serverless + pytest dependency
└────────┬────────┘
         │ (pass)
         ▼
┌─────────────────────────┐
│   run_bronze_pipeline   │  Step 1: Bronze ingestion
│   (Pipeline task)       │  Pipeline: maven_market_bronze
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   run_silver_pipeline   │  Step 2: Silver transformations
│   (Pipeline task)       │  Pipeline: maven_market_silver
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   run_gold_pipeline     │  Step 3: Gold aggregations
│   (Pipeline task)       │  Pipeline: maven_market_gold
└─────────────────────────┘
```

**Key Design Decision:** Tests run FIRST as a quality gate. If any test fails, the entire orchestration job stops — no pipelines are executed. This ensures code quality is validated before any data mutation.

---

## 7. Testing Strategy

### Test Runner

`tests/run_tests.py` handles Databricks workspace constraints:
- Copies test files to `/tmp` (avoids `__pycache__` write errors on workspace filesystem)
- Runs pytest with `-v --tb=short -p no:cacheprovider`
- Returns non-zero exit code on failure (fails the job task)
- Compatible with serverless compute (handles missing `__file__`)

### Shared Fixture

`tests/conftest.py` provides a **session-scoped** `spark` fixture — SparkSession is created once and reused across all tests.

### Test Suites

| File | Tests | Coverage Area |
| --- | --- | --- |
| `test_ingestion.py` | 10+ tests | JSON schema parsing (customers, products), malformed JSON handling, date parsing (`M/d/yyyy`), type casting (INT, DOUBLE, LONG, BOOLEAN) |
| `test_transformations.py` | 10+ tests | Margin % calculation, full name concatenation, revenue/cost/profit formulas, stock status classification, is_weekend flag, date key generation, transaction date parts, edge cases |
| `test_data_quality.py` | 10 tests | DLT expectation simulation — null PK rejection, negative price rejection, gender validation, positive quantity enforcement, price-above-cost check, null FK rejection, event type validation, stock level non-negative, date not-null, combined checks |
| `test_scd2_and_joins.py` | 11 tests | SCD-2 current snapshot filter (`__END_AT IS NULL`), multi-version history, all-expired edge case, fact-product left join, unmatched FK handling, store-region denormalization, deduplication, stream-static join pattern |
| `test_gold_aggregations.py` | 10+ tests | Executive overview aggregation, regional sales by store, customer LTV, inventory alerts, orders per minute, store space utilization, single-row edge cases |

**Skip Marker:** Tests requiring Databricks runtime features (ANSI mode, `try_to_date`) use `@databricks_only` skip marker and won't fail in local/CI environments.

### Running Tests Locally

```bash
cd /Workspace/Capstone-Project-Group4/maven_market
python tests/run_tests.py
```

Or directly with pytest:
```bash
pytest tests/ -v --tb=short -p no:cacheprovider
```

---

## 8. Environment Configuration

### Bundle Variables (`databricks.yml`)

| Variable | Description | Dev Default | Prod Default |
| --- | --- | --- | --- |
| `target_catalog` | Unity Catalog destination | `maven_market_uc` | `maven_market_uc` |
| `config_file` | Path to environment config YAML | `${workspace.file_path}/config/dev_config.yaml` | (override as needed) |
| `pipelines_development` | Enable DLT development mode | `true` | `false` |

### Environment Config (`config/dev_config.yaml`)

```yaml
environment: "dev"
catalog: "maven_market_uc"
storage_root: "abfss://maven-market-data@sgmavenmarket1.dfs.core.windows.net/"

schemas:
  bronze: "bronze"
  silver: "silver"
  gold: "gold"
  audit: "audit"

paths:
  landing_zone: ".../raw/"
  transactions: ".../raw/transactions/"
  regions: ".../raw/regions/"
  stores: ".../raw/stores/"
  return: ".../raw/return/"
  calendar: ".../raw/calendar/"
  customers: ".../raw/atlas/mongo_maven_db/customers/"
  products: ".../raw/atlas/mongo_maven_db/products/"

checkpoints:
  # Auto Loader checkpoint paths per dataset
  transactions: ".../checkpoints/transactions/"
  ...
```

### Cluster Policy (`policies/cluster_policy.json`)

| Setting | Value |
| --- | --- |
| Cluster type | All-purpose, single node |
| Node type | `Standard_DC4as_v5` |
| Spark version | `15.4.x-scala2.12` |
| Auto-terminate | 30 minutes |
| Tag | `Project: maven_market` |
| Workers | 0 (single node) |

---

## 9. Deployment Targets (CI/CD)

### Target: `dev` (Default)

| Property | Value |
| --- | --- |
| Mode | `development` |
| Workspace Host | `https://adb-7405612262402698.18.azuredatabricks.net` |
| Root Path | `~/.bundle/maven_market_v3/dev` |
| Catalog | `maven_market_uc` |
| Pipeline Dev Mode | `true` (faster iteration, no production checkpoints) |

### Target: `main` (Production)

| Property | Value |
| --- | --- |
| Mode | `production` |
| Workspace Host | `https://adb-7405612262402698.18.azuredatabricks.net` |
| Root Path | `/Shared/.bundle/maven_market_v3/prod` |
| Catalog | `maven_market_uc` |
| Pipeline Dev Mode | `false` (full production runs) |
| Run As | Service Principal `5e35aeb2-73b1-4dd2-92cf-ae0e4829bb65` |

---

## 10. Deployment Workflow

### CI/CD Pipeline Flow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Develop    │────▶│   Validate   │────▶│    Deploy     │────▶│     Run      │
│  (edit code) │     │  (lint/test) │     │  (to target)  │     │ (orchestrate)│
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

### Step 1 — Validate the Bundle

```bash
databricks bundle validate --target dev
```

Checks YAML syntax, resource references, variable interpolation, and target configuration. Use `--strict` for CI pipelines to catch warnings as errors:

```bash
databricks bundle validate --strict --target dev
```

### Step 2 — Deploy to Dev

```bash
databricks bundle deploy --target dev
```

This syncs all source code, pipeline definitions, and job configurations to the dev workspace path (`~/.bundle/maven_market_v3/dev`).

### Step 3 — Run the Orchestration Job

```bash
databricks bundle run maven_market_daily_orchestration --target dev
```

Executes the full pipeline: tests → bronze → silver → gold.

### Step 4 — Promote to Production

```bash
databricks bundle validate --strict --target main
databricks bundle deploy --target main
databricks bundle run maven_market_daily_orchestration --target main
```

Production deploys to `/Shared/.bundle/maven_market_v3/prod` and runs under the service principal identity.

### Workspace UI Deployment

Alternatively, use the Databricks workspace UI:
1. Click the **deployment rocket** in the left sidebar to open the **Deployments** panel
2. Click **Deploy** to sync resources
3. Hover over a job/pipeline and click **Run**

---

## 11. Data Governance

Governance scripts are located in `src/governance/` and are **currently disabled** for development. They should be **enabled before production release**.

### Row-Level Security (`apply_rls.sql`)

- Function: `maven_market_uc.gold.region_filter(sales_region STRING)`
- Logic: Admins and executives see all regions; regional managers see only assigned regions via `gold.user_region_map`
- Applied to: `gold.agg_regional_sales`

### Column-Level Security (`apply_cls.sql`)

- Function: `maven_market_uc.gold.pii_mask(column_value STRING)` — returns `'### MASKED ###'` for non-privileged users
- Function: `maven_market_uc.gold.pii_mask_date(column_value DATE)` — returns `NULL` for non-privileged users
- Applied to: `gold.dim_customer` columns: `email_address`, `full_name`, `customer_address`, `birthdate`
- Privileged groups: `maven_admins`, `maven_engineers`

### RBAC Permissions (`apply_permissions.sql`)

| Group | Access Level |
| --- | --- |
| `maven_engineers` | ALL PRIVILEGES on bronze, silver, gold schemas |
| `maven_executives` | USAGE + SELECT on gold schema only |
| `maven_analysts` | USAGE + SELECT on gold schema only |

### User-Region Map (`seed_user_region_map.py`)

Seeds `gold.user_region_map` with team member → region assignments for RLS enforcement.

---

## 12. Infrastructure Setup

Initial one-time setup is handled by `scripts/setup_uc.py` (archived — already executed):

- Creates catalogs: `maven_market_uc`, `maven_market_dev`
- Creates schemas: `bronze`, `silver`, `gold`, `audit` in each catalog
- Grants `ALL PRIVILEGES` to admin user
- Managed locations on ADLS Gen2

---

## 13. Operational Runbook

### Monitoring Pipeline Health

1. Open the Databricks workspace
2. Navigate to **Lakeflow Jobs** → `[dev] Full Project Orchestration`
3. Check task status: `run_tests` → `run_bronze_pipeline` → `run_silver_pipeline` → `run_gold_pipeline`
4. For pipeline-specific monitoring, open the individual pipeline pages

### Common Failure Scenarios

| Scenario | Symptom | Resolution |
| --- | --- | --- |
| Test gate failure | `run_tests` task fails, no pipelines execute | Review test output; fix failing logic in `src/` or `tests/` |
| Bronze ingestion failure | `run_bronze_pipeline` fails | Check ADLS connectivity, Kafka broker status, source file format |
| Silver quality failure | `run_silver_pipeline` fails | Check `expect_or_fail` rules (e.g., null PKs); inspect bronze data quality |
| Silver cross-schema read | `Table not found` error | Verify `bundle.target_catalog` config is set; bronze pipeline completed first |
| Gold join failure | Null revenues/costs | Check SCD-2 tables have current rows (`__END_AT IS NULL`); verify product_id FK integrity |
| Kafka auth error | `SASL authentication failed` | Verify Confluent API key/secret; check topic existence |

### Audit Logging

All pipeline stages write structured logs to `maven_market_dev.audit_logs` via `PipelineLogger`:
- Fields: `timestamp`, `run_id`, `level`, `layer`, `stage`, `message`, `status`, `row_count`, `error`
- Query: `SELECT * FROM maven_market_dev.audit_logs ORDER BY timestamp DESC`

---

## 14. Bundle CLI Reference

| Command | Description |
| --- | --- |
| `databricks bundle validate --target dev` | Validate bundle configuration |
| `databricks bundle validate --strict --target dev` | Strict validation (warnings as errors) |
| `databricks bundle deploy --target dev` | Deploy to dev environment |
| `databricks bundle deploy --target main` | Deploy to production |
| `databricks bundle run maven_market_daily_orchestration --target dev` | Run full orchestration |
| `databricks bundle summary --target dev` | Show deployed resource summary |
| `databricks bundle destroy --target dev` | Tear down all deployed resources |

---

## Documentation Links

- [Databricks Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [DABs Configuration Reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Unity Catalog Governance](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
