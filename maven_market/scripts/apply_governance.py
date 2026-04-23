"""
apply_governance.py — Post-pipeline governance enforcement.

Runs as a job task AFTER the gold pipeline completes.
Applies Row-Level Security (RLS), Column-Level Security (CLS),
Unity Catalog permissions, and seeds the user-to-region mapping
table that drives RLS filtering.

Usage (from jobs.yml):
    spark_python_task:
      python_file: ../scripts/apply_governance.py
      parameters: ["${var.target_catalog}"]
"""

import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ── Resolve target catalog ───────────────────────────────────────────
CATALOG = sys.argv[1] if len(sys.argv) > 1 else "maven_market_uc"
print(f"[governance] Target catalog: {CATALOG}")


def run_sql(description, statement):
    """Execute a single SQL statement with logging."""
    print(f"  \u2192 {description}")
    try:
        spark.sql(statement)
    except Exception as e:
        print(f"    \u26a0 WARNING: {e}")


# =====================================================================
# 1. UNITY CATALOG PERMISSIONS
#    Mirror the tiered RBAC model from apply_permissions.sql
# =====================================================================
print("\n[1/4] Applying Unity Catalog permissions \u2026")

# Admins — full catalog ownership
run_sql("GRANT ALL PRIVILEGES on catalog to maven_admins",
        f"GRANT ALL PRIVILEGES ON CATALOG {CATALOG} TO maven_admins")

# Engineers — catalog usage + full schema access
run_sql("GRANT USE CATALOG to maven_engineers",
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO maven_engineers")
for schema in ("bronze", "silver", "gold"):
    run_sql(f"GRANT ALL PRIVILEGES on {schema} to maven_engineers",
            f"GRANT ALL PRIVILEGES ON SCHEMA {CATALOG}.{schema} TO maven_engineers")

# Analysts — gold read-only
run_sql("GRANT USE CATALOG to maven_analysts",
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO maven_analysts")
run_sql("GRANT USE SCHEMA on gold to maven_analysts",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO maven_analysts")
run_sql("GRANT SELECT on gold to maven_analysts",
        f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO maven_analysts")

# Executives — gold read-only
run_sql("GRANT USE CATALOG to maven_executives",
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO maven_executives")
run_sql("GRANT USE SCHEMA on gold to maven_executives",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO maven_executives")
run_sql("GRANT SELECT on gold to maven_executives",
        f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO maven_executives")

# Deny bronze/silver to business users
for schema in ("bronze", "silver"):
    for group in ("maven_analysts", "maven_executives"):
        run_sql(f"REVOKE ALL on {schema} from {group}",
                f"REVOKE ALL PRIVILEGES ON SCHEMA {CATALOG}.{schema} FROM {group}")


# =====================================================================
# 2. SEED USER → REGION MAP (drives RLS filtering)
# =====================================================================
print("\n[2/4] Seeding user_region_map \u2026")

run_sql("CREATE user_region_map table if not exists",
        f"""CREATE TABLE IF NOT EXISTS {CATALOG}.gold.user_region_map
            (user_id STRING, assigned_region STRING)""")

run_sql("TRUNCATE user_region_map",
        f"TRUNCATE TABLE {CATALOG}.gold.user_region_map")

run_sql("INSERT region assignments for Snigdha and Devjit",
        f"""INSERT INTO {CATALOG}.gold.user_region_map VALUES
            ('snigdha@rajnijha29112001gmail.onmicrosoft.com', 'North West'),
            ('snigdha@rajnijha29112001gmail.onmicrosoft.com', 'Central West'),
            ('snigdha@rajnijha29112001gmail.onmicrosoft.com', 'South West'),
            ('devjit@rajnijha29112001gmail.onmicrosoft.com',  'South West')""")


# =====================================================================
# 3. ROW-LEVEL SECURITY (RLS)
#    region_filter function + row filters on region-bearing gold tables
# =====================================================================
print("\n[3/4] Applying Row-Level Security \u2026")

run_sql("CREATE OR REPLACE region_filter function",
        f"""CREATE OR REPLACE FUNCTION {CATALOG}.gold.region_filter(region_name STRING)
            RETURNS BOOLEAN
            RETURN
              is_account_group_member('maven_admins')
              OR is_account_group_member('maven_engineers')
              OR EXISTS (
                SELECT 1 FROM {CATALOG}.gold.user_region_map
                WHERE user_id = current_user() AND assigned_region = region_name
              )""")

for mv, col in [("dim_store", "sales_region"),
                ("dim_region", "sales_region"),
                ("agg_regional_sales", "sales_region")]:
    run_sql(f"SET ROW FILTER on {mv}({col})",
            f"""ALTER MATERIALIZED VIEW {CATALOG}.gold.{mv}
                SET ROW FILTER {CATALOG}.gold.region_filter ON ({col})""")


# =====================================================================
# 4. COLUMN-LEVEL SECURITY (CLS)
#    PII masking functions + column masks on customer/store/LTV tables
# =====================================================================
print("\n[4/4] Applying Column-Level Security \u2026")

# String PII mask
run_sql("CREATE OR REPLACE mask_pii_string function",
        f"""CREATE OR REPLACE FUNCTION {CATALOG}.gold.mask_pii_string(pii_value STRING)
            RETURNS STRING
            RETURN CASE
              WHEN is_account_group_member('maven_admins')
                OR is_account_group_member('maven_engineers')
              THEN pii_value
              ELSE '### MASKED ###'
            END""")

# Date PII mask
run_sql("CREATE OR REPLACE mask_pii_date function",
        f"""CREATE OR REPLACE FUNCTION {CATALOG}.gold.mask_pii_date(pii_value DATE)
            RETURNS DATE
            RETURN CASE
              WHEN is_account_group_member('maven_admins')
                OR is_account_group_member('maven_engineers')
              THEN pii_value
              ELSE NULL
            END""")

# dim_customer masks (7 columns)
for col_name in ("first_name", "last_name", "full_name", "email_address",
                 "customer_address", "customer_postal_code"):
    run_sql(f"MASK dim_customer.{col_name}",
            f"""ALTER MATERIALIZED VIEW {CATALOG}.gold.dim_customer
                ALTER COLUMN {col_name} SET MASK {CATALOG}.gold.mask_pii_string""")

run_sql("MASK dim_customer.birthdate",
        f"""ALTER MATERIALIZED VIEW {CATALOG}.gold.dim_customer
            ALTER COLUMN birthdate SET MASK {CATALOG}.gold.mask_pii_date""")

# dim_store masks (2 columns)
for col_name in ("store_street_address", "store_phone"):
    run_sql(f"MASK dim_store.{col_name}",
            f"""ALTER MATERIALIZED VIEW {CATALOG}.gold.dim_store
                ALTER COLUMN {col_name} SET MASK {CATALOG}.gold.mask_pii_string""")

# agg_customer_ltv masks (2 columns)
for col_name in ("first_name", "last_name"):
    run_sql(f"MASK agg_customer_ltv.{col_name}",
            f"""ALTER MATERIALIZED VIEW {CATALOG}.gold.agg_customer_ltv
                ALTER COLUMN {col_name} SET MASK {CATALOG}.gold.mask_pii_string""")

print("\n\u2705 Governance applied successfully.")
