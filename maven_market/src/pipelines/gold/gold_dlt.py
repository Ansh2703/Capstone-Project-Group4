# -----------------------------------------------------------
# Lakeflow Declarative Pipeline: Gold Layer
# -----------------------------------------------------------
# Tables and Materialized Views Created:
# 1. dim_date (Materialized View): Date dimension (YYYYMMDD keys, fiscal/calendar attributes).
#    Source: silver.calendar table
# 2. dim_region (Materialized View): Region reference dimension. Sourced from silver.regions (SCD-1, no history).
#    Source: silver.regions table
# 3. dim_store (Materialized View): Conformed store dimension. Current SCD-2 snapshot (__END_AT IS NULL).
#    Source: silver.stores and silver.regions tables
# 4. dim_customer (Materialized View): Conformed customer dimension — current SCD-2 snapshot (__END_AT IS NULL).
#    Source: silver.customers table
# 5. dim_product (Materialized View): Conformed product dimension — current SCD-2 snapshot (__END_AT IS NULL).
#    Source: silver.products table
# 6. fact_sales (Materialized View): Fact Sales — grain: one row per transaction line. Enriched with revenue, cost, gross_profit.
#    Source: silver.transactions and silver.products tables
# 7. fact_returns (Materialized View): Fact Returns — grain: one return line. Enriched with return_revenue and return_cost.
#    Source: silver.returns and silver.products tables
# 8. agg_executive_overview (Materialized View): Executive overview containing Total Revenue and Profit Margin by Year/Month.
#    Source: fact_sales table
# 9. agg_ops_inventory_alerts (Materialized View): Inventory alerts aggregation for real-time operations dashboard.
#    Source: silver.inventory table
# 10. agg_ops_orders_per_minute (Materialized View): Orders per minute aggregation for operations dashboard.
#     Source: silver.orders table
# 11. agg_regional_sales (Materialized View): Sales aggregated by store for Regional Managers. RLS row filter applied on sales_region.
#     Source: fact_sales and dim_store tables
# 12. agg_customer_ltv (Materialized View): Customer Lifetime Value (LTV) combining total spend, profit, and demographic profile.
#     Source: fact_sales and dim_customer tables
# 13. agg_store_space_utilization (Materialized View): Retail operations metric calculating revenue generated per square foot of store space.
#     Source: fact_sales and dim_store tables
# -----------------------------------------------------------

import sys

# Project root injected via pipeline configuration (bundle.project_root = ${workspace.file_path})
sys.path.insert(0, spark.conf.get("bundle.project_root"))

from src.utils.logger import PipelineLogger

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp

logger = PipelineLogger(layer="gold")

# Cross-schema reference: read catalog from pipeline configuration
CATALOG       = spark.conf.get("bundle.target_catalog")
SILVER_SCHEMA = spark.conf.get("bundle.silver_schema")

logger.info("Gold pipeline starting", stage="gold_init")

# ===================================================================
# DIMENSION TABLES
# ===================================================================

@dlt.table(
    name="dim_date",
    comment="Date dimension (YYYYMMDD keys, fiscal/calendar attributes). Source: silver.calendar table.",
    table_properties={
        "layer":        "gold",
        "domain":       "reference",
        "contains_pii": "false",
        "delta.autoOptimize.optimizeWrite":  "true",
        "pipelines.autoOptimize.zOrderCols": "date_key",
    },
)
@dlt.expect_or_drop("valid_date_key",    "date_key IS NOT NULL")
@dlt.expect_or_drop("valid_date_column", "date IS NOT NULL")
def dim_date():
    logger.info("Building dim_date from silver.calendar", stage="gold_dim_date")
    return (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.calendar")
        .select(
            F.date_format(col("date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("date"),
            col("year"),
            col("month"),
            col("quarter"),
            col("week_of_year"),
            col("day_of_week"),
            col("day_name"),
            col("month_name"),
            col("is_weekend"),
            col("year").alias("fiscal_year"),
            col("quarter").alias("fiscal_quarter"),
            current_timestamp().alias("gold_loaded_at"),
        )
        .dropDuplicates(["date_key"])
        .orderBy("date_key")
        .withColumn("date_sk", F.md5(F.concat_ws("||", F.lit("dim_date"), col("date_key").cast("string"))))
        .select(
            "date_sk", "date_key", "date", "year", "month", "quarter",
            "week_of_year", "day_of_week", "day_name", "month_name",
            "is_weekend", "fiscal_year", "fiscal_quarter", "gold_loaded_at",
        )
    )

@dlt.table(
    name="dim_region",
    comment="Region reference dimension. Sourced from silver.regions (SCD-1, no history). Source: silver.regions table.",
    table_properties={
        "layer":        "gold",
        "domain":       "reference",
        "contains_pii": "false",
        "delta.autoOptimize.optimizeWrite":  "true",
        "pipelines.autoOptimize.zOrderCols": "region_id",
    },
)
@dlt.expect_or_drop("valid_region_id",    "region_id IS NOT NULL")
@dlt.expect(        "has_sales_region",   "sales_region IS NOT NULL")
def dim_region():
    logger.info("Building dim_region from silver.regions", stage="gold_dim_region")
    return (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.regions")
        .select(
            col("region_id"),
            col("sales_district"),
            col("sales_region"),
            current_timestamp().alias("gold_loaded_at"),
        )
        .dropDuplicates(["region_id"])
        .withColumn("region_sk", F.md5(F.concat_ws("||", F.lit("dim_region"), col("region_id").cast("string"))))
        .select(
            "region_sk", "region_id", "sales_district", "sales_region", "gold_loaded_at",
        )
    )

@dlt.table(
    name="dim_store",
    comment=(
        "Conformed store dimension. Current SCD-2 snapshot (__END_AT IS NULL). "
        "Region attributes denormalised from silver.regions. Source: silver.stores and silver.regions tables."
    ),
    table_properties={
        "layer":        "gold",
        "domain":       "stores",
        "contains_pii": "false",
        "delta.autoOptimize.optimizeWrite":  "true",
        "pipelines.autoOptimize.zOrderCols": "store_id,region_id",
    },
)
@dlt.expect_or_drop("valid_store_id",  "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_region_id", "region_id IS NOT NULL")
@dlt.expect(        "has_store_name",  "store_name IS NOT NULL")
def dim_store():
    logger.info("Building dim_store (SCD-2 snapshot + region denorm)", stage="gold_dim_store")
    # Current SCD-2 snapshot — __END_AT IS NULL written explicitly
    stores = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.stores")
        .filter(col("__END_AT").isNull())
        .select(
            col("store_id"),
            col("region_id"),
            col("store_type"),
            col("store_name"),
            col("store_street_address"),
            col("store_city"),
            col("store_state"),
            col("store_country"),
            col("store_phone"),
            col("first_opened_date"),
            col("last_remodel_date"),
            col("total_sqft"),
            col("grocery_sqft"),
            col("__START_AT").alias("effective_from"),
        )
    )

    # Denormalise region attributes — standard Kimball snowflake collapse
    regions = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.regions")
        .select(
            col("region_id").alias("r_region_id"),
            col("sales_district"),
            col("sales_region"),
        )
    )

    return (
        stores
        .join(regions, stores.region_id == regions.r_region_id, "left")
        .drop("r_region_id")
        .withColumn("store_sk", F.md5(F.concat_ws("||", F.lit("dim_store"), col("store_id").cast("string"), col("effective_from").cast("string"))))
        .withColumn("gold_loaded_at", current_timestamp())
        .select(
            "store_sk", "store_id", "region_id", "store_type", "store_name",
            "store_street_address", "store_city", "store_state", "store_country",
            "store_phone", "first_opened_date", "last_remodel_date",
            "total_sqft", "grocery_sqft", "effective_from",
            "sales_district", "sales_region", "gold_loaded_at",
        )
    )

@dlt.table(
    name="dim_customer",
    comment=(
        "Conformed customer dimension — current SCD-2 snapshot (__END_AT IS NULL). "
        "PII columns present in base table; masked at query time via UC column masks. "
        "Source: silver.customers table."
    ),
    table_properties={
        "layer":        "gold",
        "domain":       "customers",
        "contains_pii": "true",
        "delta.autoOptimize.optimizeWrite":  "true",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
    },
)
@dlt.expect_or_drop("valid_customer_id",      "customer_id IS NOT NULL")
@dlt.expect(        "has_customer_country",   "customer_country IS NOT NULL")
@dlt.expect(        "valid_customer_gender",  "gender IN ('M', 'F')")
def dim_customer():
    logger.info("Building dim_customer (SCD-2 snapshot, PII-bearing)", stage="gold_dim_customer")
    return (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
        .filter(col("__END_AT").isNull())
        .select(
            col("customer_id"),
            col("customer_acct_num"),
            col("first_name"),
            col("last_name"),
            col("full_name"),            # masked by gold.pii_mask
            col("email_address"),        # masked by gold.pii_mask
            col("customer_address"),     # masked by gold.pii_mask
            col("customer_city"),
            col("customer_state_province"),
            col("customer_postal_code"),
            col("customer_country"),
            col("birthdate"),            # masked by gold.pii_mask_date
            col("gender"),
            col("total_children"),
            col("num_children_at_home"),
            col("education"),
            col("marital_status"),
            col("yearly_income"),
            col("member_card"),
            col("occupation"),
            col("homeowner"),
            col("acct_open_date"),
            col("__START_AT").alias("effective_from"),
            current_timestamp().alias("gold_loaded_at"),
        )
        .withColumn("customer_sk", F.md5(F.concat_ws("||", F.lit("dim_customer"), col("customer_id").cast("string"), col("effective_from").cast("string"))))
        .select(
            "customer_sk", "customer_id", "customer_acct_num",
            "first_name", "last_name", "full_name", "email_address",
            "customer_address", "customer_city", "customer_state_province",
            "customer_postal_code", "customer_country", "birthdate",
            "gender", "total_children", "num_children_at_home",
            "education", "marital_status", "yearly_income", "member_card",
            "occupation", "homeowner", "acct_open_date",
            "effective_from", "gold_loaded_at",
        )
    )

@dlt.table(
    name="dim_product",
    comment="Conformed product dimension — current SCD-2 snapshot (__END_AT IS NULL). Source: silver.products table.",
    table_properties={
        "layer":        "gold",
        "domain":       "products",
        "contains_pii": "false",
        "delta.autoOptimize.optimizeWrite":  "true",
        "pipelines.autoOptimize.zOrderCols": "product_id,product_brand",
    },
)
@dlt.expect_or_drop("valid_product_id",    "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_retail_price",  "product_retail_price > 0")
@dlt.expect(        "margin_is_positive",  "margin_pct > 0")
def dim_product():
    logger.info("Building dim_product (SCD-2 snapshot)", stage="gold_dim_product")
    # Current SCD-2 snapshot — __END_AT IS NULL written explicitly
    return (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
        .filter(col("__END_AT").isNull())
        .select(
            col("product_id"),
            col("product_brand"),
            col("product_name"),
            col("product_sku"),
            col("product_retail_price"),
            col("product_cost"),
            col("product_weight"),
            col("recyclable"),
            col("low_fat"),
            col("margin_pct"),
            col("__START_AT").alias("effective_from"),
            current_timestamp().alias("gold_loaded_at"),
        )
        .withColumn("product_sk", F.md5(F.concat_ws("||", F.lit("dim_product"), col("product_id").cast("string"), col("effective_from").cast("string"))))
        .select(
            "product_sk", "product_id", "product_brand", "product_name",
            "product_sku", "product_retail_price", "product_cost",
            "product_weight", "recyclable", "low_fat", "margin_pct",
            "effective_from", "gold_loaded_at",
        )
    )

# ===================================================================
# FACT TABLES
# ===================================================================

@dlt.table(
    name="fact_sales",
    comment=(
        "Fact Sales — grain: one row per transaction line. "
        "Enriched with revenue, cost, gross_profit (point-in-time current prices). "
        "Includes dimension surrogate key FKs (store_sk, customer_sk, product_sk, date_sk). "
        "Partitioned by transaction_year / transaction_month. Source: silver.transactions and silver.products tables."
    ),
    partition_cols=["transaction_year", "transaction_month"],
    table_properties={
        "layer":        "gold",
        "domain":       "sales",
        "contains_pii": "false",
        "delta.autoOptimize.optimizeWrite":  "true",
        "delta.autoOptimize.autoCompact":    "true",
        "pipelines.autoOptimize.zOrderCols": "transaction_date,store_id,product_id,customer_id",
    },
)
@dlt.expect_or_drop("valid_fs_quantity",    "quantity > 0")
@dlt.expect_or_drop("valid_fs_product_id",  "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_fs_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_fs_store_id",    "store_id IS NOT NULL")
@dlt.expect(        "non_negative_revenue",  "revenue >= 0")
def fact_sales():
    logger.info("Building fact_sales (transactions x products join)", stage="gold_fact_sales")
    # Silver transactions (cross-schema read)
    txn = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.transactions")
        .select(
            "transaction_date", "stock_date", "product_id",
            "customer_id", "store_id", "quantity",
            "transaction_year", "transaction_month", "transaction_quarter",
        )
    )

    # Current SCD-2 product prices (cross-schema read)
    prod = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
        .filter(col("__END_AT").isNull())
        .select(
            col("product_id").alias("p_product_id"),
            col("product_retail_price"),
            col("product_cost"),
        )
    )

    logger.info("Joining transactions with product prices", stage="gold_fact_sales")

    # Base fact with revenue metrics
    base = (
        txn
        .join(prod, txn.product_id == prod.p_product_id, "left")
        .drop("p_product_id")
        .withColumn("revenue",      F.round(col("quantity") * col("product_retail_price"), 2))
        .withColumn("cost",         F.round(col("quantity") * col("product_cost"), 2))
        .withColumn("gross_profit", F.round(col("revenue") - col("cost"), 2))
        .withColumn("date_key",     F.date_format(col("transaction_date"), "yyyyMMdd").cast("int"))
        .withColumn("gold_loaded_at", current_timestamp())
        .withColumn("sale_sk", F.md5(F.concat_ws("||",
            F.lit("fact_sales"),
            col("transaction_date").cast("string"),
            col("product_id").cast("string"),
            col("customer_id").cast("string"),
            col("store_id").cast("string"),
            col("quantity").cast("string"),
            col("stock_date").cast("string"),
        )))
    )

    # Dimension surrogate key lookups
    dim_s = dlt.read("dim_store").select(col("store_id").alias("ds_store_id"), "store_sk")
    dim_c = dlt.read("dim_customer").select(col("customer_id").alias("dc_customer_id"), "customer_sk")
    dim_p = dlt.read("dim_product").select(col("product_id").alias("dp_product_id"), "product_sk")
    dim_d = dlt.read("dim_date").select(col("date_key").alias("dd_date_key"), "date_sk")

    logger.info("Joining with dimension tables for surrogate key FKs", stage="gold_fact_sales")

    return (
        base
        .join(dim_s, base.store_id == dim_s.ds_store_id, "left").drop("ds_store_id")
        .join(dim_c, base.customer_id == dim_c.dc_customer_id, "left").drop("dc_customer_id")
        .join(dim_p, base.product_id == dim_p.dp_product_id, "left").drop("dp_product_id")
        .join(dim_d, base.date_key == dim_d.dd_date_key, "left").drop("dd_date_key")
        .select(
            "sale_sk",
            "store_sk", "customer_sk", "product_sk", "date_sk",
            "date_key", "transaction_date", "stock_date",
            "product_id", "customer_id", "store_id", "quantity",
            "revenue", "cost", "gross_profit",
            "transaction_year", "transaction_month", "transaction_quarter",
            "gold_loaded_at",
        )
    )

@dlt.table(
    name="fact_returns",
    comment=(
        "Fact Returns — grain: one return line. "
        "Enriched with return_revenue and return_cost. "
        "Includes dimension surrogate key FKs (store_sk, product_sk, date_sk). "
        "Partitioned by return_year. Source: silver.returns and silver.products tables."
    ),
    partition_cols=["return_year"],
    table_properties={
        "layer":        "gold",
        "domain":       "returns",
        "contains_pii": "false",
        "delta.autoOptimize.optimizeWrite":  "true",
        "pipelines.autoOptimize.zOrderCols": "return_date,store_id,product_id",
    },
)
@dlt.expect_or_drop("valid_fr_quantity",   "quantity > 0")
@dlt.expect_or_drop("valid_fr_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_fr_store_id",   "store_id IS NOT NULL")
@dlt.expect(        "non_negative_return_revenue", "return_revenue >= 0")
def fact_returns():
    logger.info("Building fact_returns (returns x products join)", stage="gold_fact_returns")
    # Silver returns (cross-schema read)
    ret = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.returns")
        .select("return_date", "product_id", "store_id", "quantity", "return_year")
    )

    # Current SCD-2 product prices (cross-schema read)
    prod = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
        .filter(col("__END_AT").isNull())
        .select(
            col("product_id").alias("p_product_id"),
            col("product_retail_price"),
            col("product_cost"),
        )
    )

    logger.info("Joining returns with product prices", stage="gold_fact_returns")

    # Base fact with return metrics
    base = (
        ret
        .join(prod, ret.product_id == prod.p_product_id, "left")
        .drop("p_product_id")
        .withColumn("return_revenue", F.round(col("quantity") * col("product_retail_price"), 2))
        .withColumn("return_cost",    F.round(col("quantity") * col("product_cost"), 2))
        .withColumn("date_key",       F.date_format(col("return_date"), "yyyyMMdd").cast("int"))
        .withColumn("gold_loaded_at", current_timestamp())
        .withColumn("return_sk", F.md5(F.concat_ws("||",
            F.lit("fact_returns"),
            col("return_date").cast("string"),
            col("product_id").cast("string"),
            col("store_id").cast("string"),
            col("quantity").cast("string"),
        )))
    )

    # Dimension surrogate key lookups
    dim_s = dlt.read("dim_store").select(col("store_id").alias("ds_store_id"), "store_sk")
    dim_p = dlt.read("dim_product").select(col("product_id").alias("dp_product_id"), "product_sk")
    dim_d = dlt.read("dim_date").select(col("date_key").alias("dd_date_key"), "date_sk")

    logger.info("Joining with dimension tables for surrogate key FKs", stage="gold_fact_returns")

    return (
        base
        .join(dim_s, base.store_id == dim_s.ds_store_id, "left").drop("ds_store_id")
        .join(dim_p, base.product_id == dim_p.dp_product_id, "left").drop("dp_product_id")
        .join(dim_d, base.date_key == dim_d.dd_date_key, "left").drop("dd_date_key")
        .select(
            "return_sk",
            "store_sk", "product_sk", "date_sk",
            "date_key", "return_date", "product_id", "store_id",
            "quantity", "return_revenue", "return_cost",
            "return_year", "gold_loaded_at",
        )
    )

# ===================================================================
# AGGREGATION TABLES (pre-computed for dashboards)
# ===================================================================

@dlt.table(
    name="agg_executive_overview",
    comment="Executive overview containing Total Revenue and Profit Margin by Year/Month. Source: fact_sales table.",
    table_properties={"layer": "gold_agg", "domain": "executive_dashboard"}
)
def agg_executive_overview():
    logger.info("Building agg_executive_overview", stage="gold_agg_executive")
    # fact_sales is in the same gold pipeline — use dlt.read()
    fact = dlt.read("fact_sales")

    return (
        fact.groupBy("transaction_year", "transaction_month")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("gross_profit").alias("total_profit"),
            F.round((F.sum("gross_profit") / F.sum("revenue")) * 100, 2).alias("profit_margin_pct")
        )
        .withColumn("exec_overview_sk", F.md5(F.concat_ws("||",
            F.lit("agg_exec_overview"),
            col("transaction_year").cast("string"),
            col("transaction_month").cast("string"),
        )))
        .select(
            "exec_overview_sk", "transaction_year", "transaction_month",
            "total_revenue", "total_profit", "profit_margin_pct",
        )
    )

@dlt.table(
    name="agg_ops_inventory_alerts",
    comment="Inventory alerts aggregation for real-time operations dashboard. Source: silver.inventory, dim_store, dim_product tables.",
    table_properties={"layer": "gold_agg", "domain": "ops_dashboard"}
)
def agg_ops_inventory_alerts():
    logger.info("Building agg_ops_inventory_alerts", stage="gold_agg_inventory")
    # Silver inventory (cross-schema read)
    inv = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.inventory")

    # Dimension surrogate key lookups
    dim_s = dlt.read("dim_store").select(col("store_id").alias("ds_store_id"), "store_sk")
    dim_p = dlt.read("dim_product").select(col("product_id").alias("dp_product_id"), "product_sk")

    return (
        inv.filter(col("stock_status").isin("OUT_OF_STOCK", "LOW"))
        .join(dim_s, inv.store_id == dim_s.ds_store_id, "left").drop("ds_store_id")
        .join(dim_p, inv.product_id == dim_p.dp_product_id, "left").drop("dp_product_id")
        .groupBy("store_sk", "product_sk", "stock_status")
        .agg(
            F.first("store_id").alias("store_id"),
            F.first("product_id").alias("product_id"),
            F.max("event_timestamp").alias("last_alert_time"),
            F.min("stock_level").alias("lowest_stock_level")
        )
        .withColumn("inventory_alert_sk", F.md5(F.concat_ws("||",
            F.lit("agg_inventory_alert"),
            col("store_sk"),
            col("product_sk"),
            col("stock_status"),
        )))
        .select(
            "inventory_alert_sk", "store_sk", "product_sk",
            "store_id", "product_id", "stock_status",
            "last_alert_time", "lowest_stock_level",
        )
    )

@dlt.table(
    name="agg_ops_orders_per_minute",
    comment="Orders per minute aggregation for operations dashboard. Source: silver.orders, dim_store tables.",
    table_properties={"layer": "gold_agg", "domain": "ops_dashboard"}
)
def agg_ops_orders_per_minute():
    logger.info("Building agg_ops_orders_per_minute", stage="gold_agg_orders_pm")
    # Silver orders (cross-schema read)
    orders = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")

    # Dimension surrogate key lookup
    dim_s = dlt.read("dim_store").select(col("store_id").alias("ds_store_id"), "store_sk")

    return (
        orders.withColumn("order_minute", F.date_trunc("minute", col("event_timestamp")))
        .join(dim_s, orders.store_id == dim_s.ds_store_id, "left").drop("ds_store_id")
        .groupBy("store_sk", "order_minute")
        .agg(
            F.first("store_id").alias("store_id"),
            F.count("order_id").alias("orders_per_minute"),
            F.sum("quantity").alias("total_quantity_per_minute")
        )
        .withColumn("orders_per_min_sk", F.md5(F.concat_ws("||",
            F.lit("agg_orders_per_min"),
            col("store_sk"),
            col("order_minute").cast("string"),
        )))
        .select(
            "orders_per_min_sk", "store_sk", "store_id", "order_minute",
            "orders_per_minute", "total_quantity_per_minute",
        )
    )

@dlt.table(
    name="agg_regional_sales",
    comment="Sales aggregated by store surrogate key for Regional Managers. RLS row filter applied on sales_region. Source: fact_sales and dim_store tables.",
    table_properties={"layer": "gold_agg", "domain": "regional_dashboard"}
)
def agg_regional_sales():
    logger.info("Building agg_regional_sales", stage="gold_agg_regional")
    # fact_sales and dim_store are in the same gold pipeline — use dlt.read()
    fact = dlt.read("fact_sales")
    dim_store = dlt.read("dim_store").drop("store_sk")

    return (
        fact.join(dim_store, on="store_id")
        .groupBy("store_sk", "transaction_year", "transaction_month")
        .agg(
            F.first("region_id").alias("region_id"),
            F.first("sales_region").alias("sales_region"),
            F.first("store_id").alias("store_id"),
            F.first("store_name").alias("store_name"),
            F.sum("revenue").alias("total_store_revenue"),
            F.sum("quantity").alias("total_items_sold")
        )
        .withColumn("regional_sales_sk", F.md5(F.concat_ws("||",
            F.lit("agg_regional_sales"),
            col("store_sk"),
            col("transaction_year").cast("string"),
            col("transaction_month").cast("string"),
        )))
        .select(
            "regional_sales_sk", "store_sk", "transaction_year", "transaction_month",
            "region_id", "sales_region", "store_id", "store_name",
            "total_store_revenue", "total_items_sold",
        )
    )

@dlt.table(
    name="agg_customer_ltv",
    comment="Customer Lifetime Value (LTV) combining total spend, profit, and demographic profile. Source: fact_sales and dim_customer tables.",
    table_properties={"layer": "gold_agg", "domain": "marketing_dashboard"}
)
def agg_customer_ltv():
    logger.info("Building agg_customer_ltv", stage="gold_agg_ltv")
    # fact_sales and dim_customer are in the same gold pipeline — use dlt.read()
    fact = dlt.read("fact_sales")
    dim_cust = dlt.read("dim_customer").drop("customer_sk")

    return (
        fact.join(dim_cust, on="customer_id")
        .groupBy("customer_sk")
        .agg(
            F.first("customer_id").alias("customer_id"),
            F.first("first_name").alias("first_name"),
            F.first("last_name").alias("last_name"),
            F.first("customer_country").alias("customer_country"),
            F.first("yearly_income").alias("yearly_income"),
            F.first("member_card").alias("member_card"),
            F.sum("revenue").alias("lifetime_revenue"),
            F.sum("gross_profit").alias("lifetime_profit"),
            F.count("date_key").alias("total_items_purchased"),
            # Average item value
            F.round(F.sum("revenue") / F.count("date_key"), 2).alias("avg_item_value")
        )
        .withColumn("customer_ltv_sk", F.md5(F.concat_ws("||",
            F.lit("agg_customer_ltv"),
            col("customer_sk"),
        )))
        .select(
            "customer_ltv_sk", "customer_sk", "customer_id", "first_name", "last_name",
            "customer_country", "yearly_income", "member_card",
            "lifetime_revenue", "lifetime_profit",
            "total_items_purchased", "avg_item_value",
        )
    )

@dlt.table(
    name="agg_store_space_utilization",
    comment="Retail operations metric calculating revenue generated per square foot of store space. Source: fact_sales and dim_store tables.",
    table_properties={"layer": "gold_agg", "domain": "ops_dashboard"}
)
def agg_store_space_utilization():
    logger.info("Building agg_store_space_utilization", stage="gold_agg_space")
    # fact_sales and dim_store are in the same gold pipeline — use dlt.read()
    fact = dlt.read("fact_sales")
    dim_store = dlt.read("dim_store").drop("store_sk")

    return (
        fact.join(dim_store, on="store_id")
        .groupBy("store_sk")
        .agg(
            F.first("store_id").alias("store_id"),
            F.first("store_name").alias("store_name"),
            F.first("store_type").alias("store_type"),
            F.first("total_sqft").alias("total_sqft"),
            F.first("grocery_sqft").alias("grocery_sqft"),
            F.sum("revenue").alias("total_revenue"),
            F.sum("gross_profit").alias("total_profit"),
        )
        # Compute per-sqft KPIs after aggregation
        .withColumn("revenue_per_sqft", F.round(col("total_revenue") / col("total_sqft"), 2))
        .withColumn("profit_per_sqft", F.round(col("total_profit") / col("total_sqft"), 2))
        .withColumn("store_space_sk", F.md5(F.concat_ws("||",
            F.lit("agg_store_space"),
            col("store_sk"),
        )))
        .select(
            "store_space_sk", "store_sk", "store_id", "store_name", "store_type",
            "total_sqft", "grocery_sqft",
            "total_revenue", "total_profit",
            "revenue_per_sqft", "profit_per_sqft",
        )
    )

logger.info("All Gold tables registered (5 dims, 2 facts, 6 aggs)", stage="gold_pipeline", status="SUCCESS")
