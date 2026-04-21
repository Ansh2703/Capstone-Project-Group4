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

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp

# Cross-schema reference: read catalog from pipeline configuration
CATALOG       = spark.conf.get("bundle.target_catalog")
SILVER_SCHEMA = "silver"

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
    return (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.regions")
        .select(
            col("region_id"),
            col("sales_district"),
            col("sales_region"),
            current_timestamp().alias("gold_loaded_at"),
        )
        .dropDuplicates(["region_id"])
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
        .withColumn("gold_loaded_at", current_timestamp())
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
    )

@dlt.table(
    name="fact_sales",
    comment=(
        "Fact Sales — grain: one row per transaction line. "
        "Enriched with revenue, cost, gross_profit (point-in-time current prices). "
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

    return (
        txn
        .join(prod, txn.product_id == prod.p_product_id, "left")
        .drop("p_product_id")
        .withColumn("revenue",      F.round(col("quantity") * col("product_retail_price"), 2))
        .withColumn("cost",         F.round(col("quantity") * col("product_cost"), 2))
        .withColumn("gross_profit", F.round(col("revenue") - col("cost"), 2))
        .withColumn("date_key",     F.date_format(col("transaction_date"), "yyyyMMdd").cast("int"))
        .withColumn("gold_loaded_at", current_timestamp())
        .select(
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

    return (
        ret
        .join(prod, ret.product_id == prod.p_product_id, "left")
        .drop("p_product_id")
        .withColumn("return_revenue", F.round(col("quantity") * col("product_retail_price"), 2))
        .withColumn("return_cost",    F.round(col("quantity") * col("product_cost"), 2))
        .withColumn("date_key",       F.date_format(col("return_date"), "yyyyMMdd").cast("int"))
        .withColumn("gold_loaded_at", current_timestamp())
        .select(
            "date_key", "return_date", "product_id", "store_id",
            "quantity", "return_revenue", "return_cost",
            "return_year", "gold_loaded_at",
        )
    )

@dlt.table(
    name="agg_executive_overview",
    comment="Executive overview containing Total Revenue and Profit Margin by Year/Month. Source: fact_sales table.",
    table_properties={"layer": "gold_agg", "domain": "executive_dashboard"}
)
def agg_executive_overview():
    # fact_sales is in the same gold pipeline — use dlt.read()
    fact = dlt.read("fact_sales")
    
    return (
        fact.groupBy("transaction_year", "transaction_month")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("gross_profit").alias("total_profit"),
            F.round((F.sum("gross_profit") / F.sum("revenue")) * 100, 2).alias("profit_margin_pct")
        )
    )

@dlt.table(
    name="agg_ops_inventory_alerts",
    comment="Inventory alerts aggregation for real-time operations dashboard. Source: silver.inventory table.",
    table_properties={"layer": "gold_agg", "domain": "ops_dashboard"}
)
def agg_ops_inventory_alerts():
    # Silver inventory (cross-schema read)
    inv = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.inventory")
    
    return (
        inv.filter(col("stock_status").isin("OUT_OF_STOCK", "LOW"))
        .groupBy("store_id", "product_id", "stock_status")
        .agg(
            F.max("event_timestamp").alias("last_alert_time"),
            F.min("stock_level").alias("lowest_stock_level")
        )
    )

@dlt.table(
    name="agg_ops_orders_per_minute",
    comment="Orders per minute aggregation for operations dashboard. Source: silver.orders table.",
    table_properties={"layer": "gold_agg", "domain": "ops_dashboard"}
)
def agg_ops_orders_per_minute():
    # Silver orders (cross-schema read)
    orders = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    
    return (
        orders.withColumn("order_minute", F.date_trunc("minute", col("event_timestamp")))
        .groupBy("store_id", "order_minute")
        .agg(
            F.count("order_id").alias("orders_per_minute"),
            F.sum("quantity").alias("total_quantity_per_minute")
        )
    )

@dlt.table(
    name="agg_regional_sales",
    comment="Sales aggregated by store for Regional Managers. RLS row filter applied on sales_region. Source: fact_sales and dim_store tables.",
    table_properties={"layer": "gold_agg", "domain": "regional_dashboard"}
)
def agg_regional_sales():
    # fact_sales and dim_store are in the same gold pipeline — use dlt.read()
    fact = dlt.read("fact_sales")
    dim_store = dlt.read("dim_store")
    
    return (
        fact.join(dim_store, on="store_id")
        .groupBy("transaction_year", "transaction_month", "region_id", "sales_region", "store_id", "store_name")
        .agg(
            F.sum("revenue").alias("total_store_revenue"),
            F.sum("quantity").alias("total_items_sold")
        )
    )

@dlt.table(
    name="agg_customer_ltv",
    comment="Customer Lifetime Value (LTV) combining total spend, profit, and demographic profile. Source: fact_sales and dim_customer tables.",
    table_properties={"layer": "gold_agg", "domain": "marketing_dashboard"}
)
def agg_customer_ltv():
    # fact_sales and dim_customer are in the same gold pipeline — use dlt.read()
    fact = dlt.read("fact_sales")
    dim_cust = dlt.read("dim_customer")
     
    return (
        fact.join(dim_cust, on="customer_id")
        .groupBy(
            "customer_id", "first_name", "last_name", 
            "customer_country", "yearly_income", "member_card"
        )
        .agg(
            F.sum("revenue").alias("lifetime_revenue"),
            F.sum("gross_profit").alias("lifetime_profit"),
            F.count("date_key").alias("total_items_purchased"),
            # Average item value
            F.round(F.sum("revenue") / F.count("date_key"), 2).alias("avg_item_value")
        )
    )

@dlt.table(
    name="agg_store_space_utilization",
    comment="Retail operations metric calculating revenue generated per square foot of store space. Source: fact_sales and dim_store tables.",
    table_properties={"layer": "gold_agg", "domain": "ops_dashboard"}
)
def agg_store_space_utilization():
    # fact_sales and dim_store are in the same gold pipeline — use dlt.read()
    fact = dlt.read("fact_sales")
    dim_store = dlt.read("dim_store")
    
    return (
        fact.join(dim_store, on="store_id")
        .groupBy(
            "store_id", "store_name", "store_type", 
            "total_sqft", "grocery_sqft"
        )
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("gross_profit").alias("total_profit"),
            # The ultimate retail KPI: Sales per Square Foot
            F.round(F.sum("revenue") / col("total_sqft"), 2).alias("revenue_per_sqft"),
            F.round(F.sum("gross_profit") / col("total_sqft"), 2).alias("profit_per_sqft")
        )
    )
