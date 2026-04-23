# Maven Market — End-to-End Data Engineering Platform

> **Bundle Name:** `maven_market_v3`  
> **Cloud:** Microsoft Azure  
> **Catalog:** `maven_market_uc` (Unity Catalog)  
> **Compute:** Serverless  
> **Framework:** Databricks Declarative Automation Bundles (DABs)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Quick Start](#2-quick-start)
3. [Repository Structure](#3-repository-structure)
4. [Architecture — Medallion Lakehouse](#4-architecture--medallion-lakehouse)
5. [Data Sources](#5-data-sources)
6. [Pipeline Details](#6-pipeline-details)
   - 6.1 [Bronze Layer — Raw Ingestion](#61-bronze-layer--raw-ingestion)
   - 6.2 [Silver Layer — Cleansing & Conforming](#62-silver-layer--cleansing--conforming)
   - 6.3 [Gold Layer — Business Aggregations](#63-gold-layer--business-aggregations)
7. [Job Orchestration](#7-job-orchestration)
8. [Testing Strategy](#8-testing-strategy)
9. [Environment Configuration](#9-environment-configuration)
10. [Deployment Targets (CI/CD)](#10-deployment-targets-cicd)
11. [Deployment Workflow](#11-deployment-workflow)
12. [Data Governance & Security](#12-data-governance--security)
13. [Dashboards](#13-dashboards)
14. [Infrastructure Setup](#14-infrastructure-setup)
15. [Operational Runbook](#15-operational-runbook)
16. [Bundle CLI Reference](#16-bundle-cli-reference)
17. [Contributors](#17-contributors)

---

## 1. Project Overview

Maven Market is an end-to-end data engineering platform that ingests retail data from
multiple sources (CSV files on ADLS Gen2, Confluent Cloud Kafka streams, and
MongoDB Atlas exports), transforms it through a **Medallion architecture**
(Bronze → Silver → Gold), and produces analytics-ready tables for executive
dashboards, regional sales reporting, customer lifetime-value analysis, and
real-time operations monitoring.

The entire project is managed as a **Databricks Declarative Automation Bundle (DAB)**,
enabling version-controlled, reproducible deployments across `dev` and `main`
(production) targets.

**Key Highlights:**

- **Multi-source ingestion** — CSV (Auto Loader), Kafka (Structured Streaming), MongoDB (Delta)
- **SCD Type-2 tracking** on stores, customers, and products
- **Real-time streaming** for orders and inventory
- **Data quality gates** with `expect_or_drop` and `expect_or_fail` expectations
- **Active Unity Catalog governance** — Row-Level Security (RLS), Column-Level Security (CLS), tiered RBAC across 4 user groups
- **50+ unit tests** gating every deployment via a pytest quality gate
- **4 AI/BI dashboards** — Executive Overview, Regional Sales, Real-Time Ops, Platform Health
- **Audit logging** — structured pipeline telemetry written to the `audit` schema

---

## 2. Quick Start

```bash
# 1. Validate the bundle configuration
databricks bundle validate --target dev

# 2. Deploy all resources (pipelines, jobs, dashboards) to dev
databricks bundle deploy --target dev

# 3. Run the full orchestration (tests → bronze → silver → gold → audit)
databricks bundle run maven_market_daily_orchestration --target dev
```

---

## 3. Repository Structure

```
maven_market/
├── databricks.yml                    # Bundle root — variables, targets, resource includes
├── README.md                         # This documentation
├── requirements.txt                  # Python deps (pyyaml, pytest, databricks-sdk)
├── .gitignore                        # Git exclusions (bytecode, IDE, caches, logs)
│
├── resources/                        # DABs resource definitions (auto-discovered via include glob)
│   ├── dlt_pipeline.yml              # Bronze, Silver, Gold Lakeflow Spark Declarative Pipelines
│   ├── jobs.yml                      # Orchestration job (test gate → 3 pipelines → audit)
│   └── setup_job.yml                 # (Archived) One-time Unity Catalog bootstrap job
│
├── config/                           # Per-environment settings loaded by config_parser.py
│   ├── dev_config.yaml               # Dev — catalog, ADLS paths, checkpoint dirs
│   └── prod_config.yaml              # Prod — isolated checkpoints under /checkpoints/prod/
│
├── src/
│   ├── pipelines/
│   │   ├── bronze/                   # ── Raw Ingestion Layer ──
│   │   │   ├── ingest_csv.py         #   Auto Loader for CSV (transactions, regions, stores, return, calendar)
│   │   │   ├── ingest_kafka_orders.py    # Confluent Kafka → orders stream (SASL_SSL)
│   │   │   ├── ingest_kafka_inventory.py # Confluent Kafka → inventory stream (SASL_SSL)
│   │   │   └── ingest_mongo.py       #   MongoDB Atlas exports (customers, products) via Delta
│   │   ├── silver/                   # ── Cleansing & Conforming Layer ──
│   │   │   ├── silver_csv_dlt.py     #   SCD-2 stores, SCD-1 regions, transactions, returns, calendar
│   │   │   ├── silver_kafka_dlt.py   #   Orders (stream-static join), inventory (stock classification)
│   │   │   └── silver_mongo_dlt.py   #   SCD-2 customers & products, margin_pct calculation
│   │   └── gold/                     # ── Business-Ready Layer ──
│   │       └── gold_dlt.py           #   Dims, facts, and 6 aggregation materialized views
│   │
│   ├── governance/                   # ── Unity Catalog Security (ACTIVE) ──
│   │   ├── apply_permissions.sql     #   Tiered RBAC grants for 4 groups
│   │   ├── apply_cls.sql             #   Column masks — PII masking on 11 cols across 3 gold tables
│   │   ├── apply_rls.sql             #   Row filters — region-based access on 3 gold tables
│   │   └── seed_user_region_map.py   #   Seeds user → sales_region assignments for RLS
│   │
│   └── utils/                        # ── Shared Utilities ──
│       ├── __init__.py               #   Package init
│       ├── config_parser.py          #   YAML config reader + fully-qualified table name builder
│       └── logger.py                 #   PipelineLogger — structured audit logging to audit schema
│
├── scripts/
│   ├── setup_uc.py                   # (Archived) One-time Unity Catalog catalog/schema bootstrap
│   └── post_pipeline_audit.py        # Writes audit summary row after gold pipeline completes
│
├── tests/                            # ── pytest Suite (50+ tests) ──
│   ├── conftest.py                   #   Session-scoped SparkSession fixture
│   ├── run_tests.py                  #   Workspace-safe runner (copies to /tmp, exits non-zero on fail)
│   ├── test_ingestion.py             #   Bronze → Silver parsing (JSON, dates, types)
│   ├── test_transformations.py       #   Silver → Gold logic (margin, revenue, stock status)
│   ├── test_data_quality.py          #   DLT expectation simulation (null PK, negative price, gender)
│   ├── test_scd2_and_joins.py        #   SCD-2 filtering, fact-dim joins, deduplication
│   └── test_gold_aggregations.py     #   Gold aggregation correctness (exec overview, LTV, regional)
│
├── dashboards/                       # ── AI/BI Lakeview Dashboards ──
│   ├── Executive Overview            #   Revenue, profit margin %, monthly trends
│   ├── Regional Sales                #   Revenue by region/store (RLS-filtered per user)
│   ├── Real-Time Ops                 #   Orders/min, inventory alerts, stock status
│   └── Platform Health               #   Table row counts, data freshness, pipeline success
│
└── policies/
    └── cluster_policy.json           # Single-node Standard_DC4as_v5, 30 min auto-terminate
```

---

## 4. Architecture — Medallion Lakehouse

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
│  GOLD (Business-Ready)  ← CLS & RLS enforced at this layer               │
│  DIMENSIONS:                                                              │
│  ├── dim_date           (YYYYMMDD surrogate key, fiscal attributes)      │
│  ├── dim_region         (region reference, RLS filtered)                 │
│  ├── dim_store          (SCD-2 current, region denorm, RLS + CLS)       │
│  ├── dim_customer       (SCD-2 current, 7 PII columns masked via CLS)  │
│  └── dim_product        (SCD-2 current, with margin_pct)                │
│  FACTS:                                                                   │
│  ├── fact_sales         (grain: txn line, revenue/cost/gross_profit)     │
│  └── fact_returns       (grain: return line, return revenue/cost)        │
│  AGGREGATIONS:                                                            │
│  ├── agg_executive_overview      (Revenue & Profit Margin by Year/Month)│
│  ├── agg_ops_inventory_alerts    (OUT_OF_STOCK & LOW stock alerts)      │
│  ├── agg_ops_orders_per_minute   (Real-time orders throughput)          │
│  ├── agg_regional_sales          (Revenue by store/region, RLS filtered)│
│  ├── agg_customer_ltv            (Customer LTV, 2 PII cols masked)      │
│  └── agg_store_space_utilization (Revenue per square foot)              │
│  SUPPORT:                                                                 │
│  └── user_region_map             (User → sales_region map for RLS)      │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Data Sources

| Source | Format | Storage | Ingestion Method | Tables |
| --- | --- | --- | --- | --- |
| CSV flat files | CSV | ADLS Gen2 (`sgmavenmarket1`) | Auto Loader (`cloudFiles`) | transactions, regions, stores, return, calendar |
| Confluent Cloud Kafka | JSON over Kafka | `pkc-56d1g.eastus.azure.confluent.cloud:9092` | Kafka Structured Streaming | orders_topic, inventory_topic |
| MongoDB Atlas | Delta (exported) | ADLS Gen2 (`raw/atlas/mongo_maven_db/`) | Delta Streaming | customers, products |

**ADLS Base Path:** `abfss://maven-market-data@sgmavenmarket1.dfs.core.windows.net/`

**Kafka Credentials:** Stored in Databricks Secrets via an Azure Key Vault-backed scope (`kafkaKeyVault`). Never hardcoded.

---

## 6. Pipeline Details

All pipelines are defined as **Lakeflow Spark Declarative Pipelines** (serverless) in `resources/dlt_pipeline.yml`.

### 6.1 Bronze Layer — Raw Ingestion

**Pipeline:** `[${target}] Maven Market Bronze` → **Schema:** `bronze`

| File | Tables Created | Method | Key Features |
| --- | --- | --- | --- |
| `ingest_csv.py` | `bronze_transactions`, `bronze_regions`, `bronze_stores`, `bronze_return`, `bronze_calendar` | Auto Loader (CSV streaming) | Dynamic table creation via loop; adds `ingestion_time`, `source_file`, `source_name` |
| `ingest_kafka_orders.py` | `bronze_orders_kafka` | Kafka Structured Streaming | SASL_SSL auth via secrets, JSON deserialization with explicit schema |
| `ingest_kafka_inventory.py` | `bronze_inventory_kafka` | Kafka Structured Streaming | SASL_SSL auth, inventory event schema |
| `ingest_mongo.py` | `bronze_customers`, `bronze_products` | Delta Streaming | Reads MongoDB Atlas exports from ADLS, adds `ingestion_time` |

### 6.2 Silver Layer — Cleansing & Conforming

**Pipeline:** `[${target}] Maven Market Silver` → **Schema:** `silver`

| File | Tables | Key Logic |
| --- | --- | --- |
| `silver_csv_dlt.py` | `transactions`, `returns`, `stores` (SCD-2), `regions` (SCD-1), `calendar` | Date parsing (`M/d/yyyy`), type casting, DLT expectations, SCD-2 via `apply_changes` tracking sqft / remodel / store_type |
| `silver_kafka_dlt.py` | `orders`, `inventory` | Stream-static join (orders x stores for location enrichment), stock health classification |
| `silver_mongo_dlt.py` | `customers` (SCD-2), `products` (SCD-2) | JSON schema parsing (`from_json`), SCD-2 tracking email / address / income (customers) and price / cost (products), `margin_pct` calculation |

**Data Quality Expectations:**

| Table | Expectation | Action |
| --- | --- | --- |
| transactions | `valid_transaction_date`, `valid_quantity`, `valid_product_id`, `valid_store_id`, `valid_customer_id` | Drop |
| transactions | `stock_before_sale` | Warn |
| returns | `valid_return_date`, `valid_return_quantity`, `valid_return_product`, `valid_return_store` | Drop |
| stores | `valid_store_pk` | **Fail pipeline** |
| stores | `valid_region_fk` | Drop |
| customers | `valid_customer_pk` | **Fail pipeline** |
| customers | `has_email`, `valid_country`, `valid_gender` | Warn |
| products | `valid_product_pk` | **Fail pipeline** |
| products | `valid_retail_price`, `valid_cost` | Drop |
| products | `price_above_cost` | Warn |
| orders | `valid_order_id`, `valid_order_quantity`, `valid_order_product` | Drop |
| inventory | `valid_inventory_event`, `valid_stock_level` | Drop |

### 6.3 Gold Layer — Business Aggregations

**Pipeline:** `[${target}] Maven Market Gold` → **Schema:** `gold`

| Table | Type | Security | Description |
| --- | --- | --- | --- |
| `dim_date` | Materialized View | — | YYYYMMDD surrogate keys, fiscal/calendar attributes |
| `dim_region` | Materialized View | **RLS** | Region reference, deduplicated from silver.regions |
| `dim_store` | Materialized View | **RLS + CLS** | Current SCD-2 snapshot, denormalized with region. Address & phone masked |
| `dim_customer` | Materialized View | **CLS** | Current SCD-2 snapshot. 7 PII columns masked |
| `dim_product` | Materialized View | — | Current SCD-2 snapshot, includes `margin_pct` |
| `fact_sales` | Materialized View | — | Grain: transaction line. `revenue`, `cost`, `gross_profit` |
| `fact_returns` | Materialized View | — | Grain: return line. `return_revenue`, `return_cost` |
| `agg_executive_overview` | Materialized View | — | Revenue, profit, margin % by year/month |
| `agg_ops_inventory_alerts` | Materialized View | — | OUT_OF_STOCK and LOW stock alerts |
| `agg_ops_orders_per_minute` | Materialized View | — | Real-time orders throughput |
| `agg_regional_sales` | Materialized View | **RLS** | Revenue by store/region, filtered by `region_filter` |
| `agg_customer_ltv` | Materialized View | **CLS** | Lifetime revenue, profit, avg item value. 2 PII cols masked |
| `agg_store_space_utilization` | Materialized View | — | Revenue & profit per sqft |
| `user_region_map` | Managed Table | — | User email → `sales_region` mapping for RLS |

---

## 7. Job Orchestration

Defined in `resources/jobs.yml` as `[${target}] Full Project Orchestration`.

```
┌─────────────────┐
│   run_tests     │  Step 0: pytest suite (quality gate)
│   (Python task) │  Env: serverless + pytest
└────────┬────────┘
         │ pass
         ▼
┌─────────────────────────┐
│   run_bronze_pipeline   │  Step 1: Raw ingestion
│   (Pipeline task)       │  CSV + Kafka + MongoDB → bronze
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   run_silver_pipeline   │  Step 2: Cleansing & conforming
│   (Pipeline task)       │  bronze → silver (SCD-2, joins, DQ)
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   run_gold_pipeline     │  Step 3: Business aggregations
│   (Pipeline task)       │  silver → gold (dims, facts, aggs)
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   post_pipeline_audit   │  Step 4: Audit logging
│   (Python task)         │  Writes summary to audit schema
└─────────────────────────┘
```

**Design Decision:** Tests run FIRST as a quality gate. If any test fails, the entire job stops — no pipelines execute. This guarantees code quality is validated before any data mutation.

---

## 8. Testing Strategy

### Test Runner

`tests/run_tests.py` handles Databricks workspace constraints:
- Copies test files to `/tmp` (avoids `__pycache__` write errors on DBFS)
- Runs pytest with `-v --tb=short -p no:cacheprovider`
- Returns non-zero exit code on failure (fails the job task)
- Compatible with serverless compute (handles missing `__file__`)

### Shared Fixture

`tests/conftest.py` provides a **session-scoped** `spark` fixture — SparkSession is created once and reused across all test files.

### Test Suites

| File | Count | Coverage Area |
| --- | --- | --- |
| `test_ingestion.py` | 10+ | JSON schema parsing, malformed JSON, date parsing (`M/d/yyyy`), type casting |
| `test_transformations.py` | 10+ | Margin %, full name concat, revenue/cost/profit, stock status, is_weekend, date key |
| `test_data_quality.py` | 10 | DLT expectation simulation — null PK rejection, negative price, gender validation |
| `test_scd2_and_joins.py` | 11 | SCD-2 current filter (`__END_AT IS NULL`), multi-version history, fact-dim joins |
| `test_gold_aggregations.py` | 10+ | Exec overview, regional sales, customer LTV, inventory alerts, orders/min, space util |

**Skip Marker:** Tests requiring Databricks runtime features use `@databricks_only` and won't fail in local/CI environments.

```bash
# Run tests locally
cd /Workspace/Capstone-Project-Group4/maven_market
python tests/run_tests.py
```

---

## 9. Environment Configuration

### Bundle Variables (`databricks.yml`)

| Variable | Purpose | Dev Default | Prod Override |
| --- | --- | --- | --- |
| `target_catalog` | Unity Catalog destination catalog | `maven_market_uc` | `maven_market_prod` |
| `config_file` | Workspace path to env config YAML | `.../config/dev_config.yaml` | `.../config/prod_config.yaml` |
| `pipelines_development` | Pipeline dev mode (faster restarts) | `true` | `false` |
| `storage_root` | ADLS Gen2 container root for all data | `abfss://maven-market-data@sgmavenmarket1.dfs.core.windows.net` | (same) |
| `kafka_bootstrap_servers` | Confluent Cloud Kafka broker | `pkc-56d1g.eastus.azure.confluent.cloud:9092` | (same) |

### Config Files (`config/*.yaml`)

Each YAML defines the catalog name, schema names, ADLS source-data paths, and Auto Loader checkpoint directories. Prod isolates checkpoints under `/checkpoints/prod/` to prevent cross-environment state conflicts.

### Cluster Policy (`policies/cluster_policy.json`)

| Setting | Value |
| --- | --- |
| Cluster type | All-purpose, single node |
| Node type | `Standard_DC4as_v5` |
| Spark version | `15.4.x-scala2.12` |
| Auto-terminate | 30 minutes |
| Workers | 0 (single node) |

---

## 10. Deployment Targets (CI/CD)

| Property | `dev` (Default) | `main` (Production) |
| --- | --- | --- |
| Mode | `development` | `production` |
| Root Path | `~/.bundle/maven_market_v3/dev` | `/Shared/.bundle/maven_market_v3/prod` |
| Catalog | `maven_market_uc` | `maven_market_prod` |
| Pipeline Dev Mode | `true` | `false` |
| Run As | Current user | Service Principal `5e35aeb2-...` |

---

## 11. Deployment Workflow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Develop    │────▶│   Validate   │────▶│    Deploy     │────▶│     Run      │
│  (edit code) │     │  (lint/test) │     │  (to target)  │     │ (orchestrate)│
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

```bash
# Step 1 — Validate
databricks bundle validate --target dev

# Step 2 — Deploy to dev
databricks bundle deploy --target dev

# Step 3 — Run orchestration
databricks bundle run maven_market_daily_orchestration --target dev

# Step 4 — Promote to production
databricks bundle validate --strict --target main
databricks bundle deploy --target main
databricks bundle run maven_market_daily_orchestration --target main
```

Production deploys to `/Shared/.bundle/maven_market_v3/prod` and runs under the service principal identity for full audit isolation.

---

## 12. Data Governance & Security

Governance is **fully active** on the `maven_market_uc` catalog. All policies are enforced at query time by Unity Catalog — no application-level bypass is possible.

### 12.1 User Groups & Access Matrix

| Group | Role | Catalog | Bronze | Silver | Gold | PII Masking | Region Filter |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `maven_admins` | Platform owners | ALL PRIVILEGES | Inherited | Inherited | Inherited | **Bypassed** | **Bypassed** |
| `maven_engineers` | Pipeline developers | USE CATALOG | ALL PRIVILEGES | ALL PRIVILEGES | ALL PRIVILEGES | **Bypassed** | **Bypassed** |
| `maven_analysts` | Regional analysts (Snigdha) | USE CATALOG | **Denied** | **Denied** | SELECT only | `### MASKED ###` | North West, Central West, South West |
| `maven_executives` | Business leaders (Devjit) | USE CATALOG | **Denied** | **Denied** | SELECT only | `### MASKED ###` | South West only |

**Key points:**
- Analysts and executives cannot see bronze or silver data (explicitly revoked)
- Engineers need unmasked gold data to verify transformation correctness
- All governance is implemented via Unity Catalog functions (not application code)

### 12.2 Row-Level Security (RLS)

**Function:** `gold.region_filter(sales_region STRING) → BOOLEAN`

| Check | Result |
| --- | --- |
| User is in `maven_admins` or `maven_engineers` | Always `TRUE` — sees all 7 regions |
| All other users | Looks up `gold.user_region_map` for the user's assigned `sales_region` values |

**Applied to (via `ALTER MATERIALIZED VIEW ... SET ROW FILTER`):**

| Table | Filter Column |
| --- | --- |
| `gold.dim_store` | `sales_region` |
| `gold.dim_region` | `sales_region` |
| `gold.agg_regional_sales` | `sales_region` |

**Available regions:** Canada West, Central West, Mexico Central, Mexico South, Mexico West, North West, South West

**Current user → region assignments (`gold.user_region_map`):**

| User | Assigned Regions | Business Meaning |
| --- | --- | --- |
| `snigdha@rajnijha29112001gmail.onmicrosoft.com` | North West, Central West, South West | US regions ("North America") |
| `devjit@rajnijha29112001gmail.onmicrosoft.com` | South West | Los Angeles & San Diego stores |

### 12.3 Column-Level Security (CLS)

**Functions (via `ALTER MATERIALIZED VIEW ... ALTER COLUMN SET MASK`):**

| Function | Returns | Behaviour |
| --- | --- | --- |
| `gold.mask_pii_string(STRING)` | `STRING` | Returns `'### MASKED ###'` unless user is in `maven_admins` or `maven_engineers` |
| `gold.mask_pii_date(DATE)` | `DATE` | Returns `NULL` unless user is in `maven_admins` or `maven_engineers` |

**Masked columns (11 total across 3 tables):**

| Gold Table | Masked Columns | Mask Function |
| --- | --- | --- |
| `dim_customer` | `first_name`, `last_name`, `full_name`, `email_address`, `customer_address`, `customer_postal_code` | `mask_pii_string` |
| `dim_customer` | `birthdate` | `mask_pii_date` |
| `dim_store` | `store_street_address`, `store_phone` | `mask_pii_string` |
| `agg_customer_ltv` | `first_name`, `last_name` | `mask_pii_string` |

### 12.4 Governance Scripts

| Script | Purpose | When to Run |
| --- | --- | --- |
| `src/governance/apply_permissions.sql` | Documents the full RBAC grant/revoke model | Manually by admin when setting up a new catalog |
| `src/governance/apply_cls.sql` | Creates masking functions and attaches to 11 columns | Once per catalog after gold pipeline first runs |
| `src/governance/apply_rls.sql` | Creates region_filter and attaches to 3 tables | Once per catalog after gold pipeline first runs |
| `src/governance/seed_user_region_map.py` | Inserts user → region rows into `user_region_map` | When onboarding new users or changing region access |

---

## 13. Dashboards

Four AI/BI Lakeview dashboards are deployed in the `dashboards/` folder. They connect directly to the gold schema and inherit all RLS/CLS policies at query time.

| Dashboard | Audience | Key Metrics |
| --- | --- | --- |
| **Executive Overview** | Executives | Revenue, profit, margin %, monthly trends |
| **Regional Sales** | Analysts & Executives | Revenue by store/region — **RLS-filtered per user** |
| **Real-Time Ops** | Operations team | Orders per minute, inventory alerts, stock status |
| **Platform Health** | Engineers & Admins | Table row counts, data freshness, pipeline success rates |

---

## 14. Infrastructure Setup

Initial one-time setup was handled by `scripts/setup_uc.py` (archived — already executed):

- Created catalogs: `maven_market_uc`, `maven_market_dev`
- Created schemas: `bronze`, `silver`, `gold`, `audit` in each catalog
- Set managed locations on ADLS Gen2
- Granted initial admin privileges

---

## 15. Operational Runbook

### Monitoring Pipeline Health

1. Open the Databricks workspace
2. Navigate to **Lakeflow Jobs** → `[dev] Full Project Orchestration`
3. Check the 5-step task chain: `run_tests` → `run_bronze_pipeline` → `run_silver_pipeline` → `run_gold_pipeline` → `post_pipeline_audit`
4. For pipeline-specific monitoring, click into the individual pipeline pages

### Common Failure Scenarios

| Scenario | Symptom | Resolution |
| --- | --- | --- |
| Test gate failure | `run_tests` fails; no pipelines execute | Review pytest output; fix logic in `src/` or `tests/` |
| Bronze ingestion failure | `run_bronze_pipeline` fails | Check ADLS connectivity, Kafka broker status, source file format |
| Silver quality failure | `run_silver_pipeline` fails | Inspect `expect_or_fail` rules (null PKs); check bronze data quality |
| Cross-schema read error | `Table not found` | Verify `bundle.target_catalog` config; ensure predecessor pipeline ran |
| Gold join failure | Null revenues/costs | Check SCD-2 tables have current rows (`__END_AT IS NULL`); verify FK integrity |
| Kafka auth error | `SASL authentication failed` | Rotate Confluent API key/secret in Azure Key Vault; verify topic exists |
| RLS blocks all rows | User sees empty tables | Check `user_region_map` has an entry for the user's email |

### Audit Logging

All pipeline stages write structured logs to the `audit` schema via `PipelineLogger`:

- **Fields:** `timestamp`, `run_id`, `level`, `layer`, `stage`, `message`, `status`, `row_count`, `error`
- **Query:** `SELECT * FROM maven_market_uc.audit.audit_logs ORDER BY timestamp DESC`

---

## 16. Bundle CLI Reference

| Command | Description |
| --- | --- |
| `databricks bundle validate --target dev` | Validate YAML syntax, resource refs, variable interpolation |
| `databricks bundle validate --strict --target dev` | Strict mode — warnings become errors (use in CI) |
| `databricks bundle deploy --target dev` | Deploy all resources to dev workspace |
| `databricks bundle deploy --target main` | Deploy to production (uses service principal) |
| `databricks bundle run maven_market_daily_orchestration --target dev` | Run full orchestration (tests → pipelines → audit) |
| `databricks bundle summary --target dev` | Show deployed resource summary |
| `databricks bundle destroy --target dev` | Tear down all deployed resources |

---

## 17. Contributors

| Name | Group | Role |
| --- | --- | --- |
| Rajni | `maven_admins` | Project architect, platform owner |
| Devjit | `maven_executives` | Business stakeholder — South West region |
| Snigdha | `maven_analysts` | Regional analyst — North America (US regions) |

---

## Documentation Links

- [Databricks Asset Bundles (Azure)](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)
- [DABs Configuration Reference](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings)
- [Lakeflow Spark Declarative Pipelines](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/)
- [Unity Catalog Governance](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/)
- [Row Filters & Column Masks](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/row-and-column-filters)
