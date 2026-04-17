import dlt
from pyspark.sql.functions import col, to_date, year, month, quarter, current_timestamp

# Config-driven variables mapped via CI/CD / DABs
CATALOG = spark.conf.get("my_catalog_name", "maven_market_uc")
BRONZE_SCHEMA = spark.conf.get("my_bronze_schema", "bronze")

# ==========================================
# 1. SILVER TRANSACTIONS (Fact - Append Only)
# ==========================================
@dlt.table(
    name="transactions",
    comment="Cleansed fact table. Drops bad math, warns on suspicious dates.",
    table_properties={
        "layer": "silver",
        "domain": "sales",
        "contains_pii": "false",
        "data_quality": "enforced"
    }
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_product", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_store", "store_id IS NOT NULL")
@dlt.expect("stock_before_sale", "stock_date <= transaction_date")
def silver_transactions():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions")
        .select(
            to_date(col("transaction_date"), "M/d/yyyy").alias("transaction_date"),
            to_date(col("stock_date"), "M/d/yyyy").alias("stock_date"),
            col("product_id").cast("int"),
            col("customer_id").cast("int"),
            col("store_id").cast("int"),
            col("quantity").cast("int"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            current_timestamp().alias("silver_ingestion_time")
        )
    )

# ==========================================
# 2. SILVER RETURNS (Fact - Append Only)
# ==========================================
@dlt.table(
    name="returns",
    comment="Cleansed returns fact table. Drops invalid return quantities.",
    table_properties={
        "layer": "silver",
        "domain": "returns",
        "contains_pii": "false",
        "data_quality": "enforced"
    }
)
@dlt.expect_or_drop("valid_return_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_return_date", "return_date IS NOT NULL")
def silver_returns():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.return")
        .select(
            to_date(col("return_date"), "M/d/yyyy").alias("return_date"),
            col("product_id").cast("int"),
            col("store_id").cast("int"),
            col("quantity").cast("int"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            current_timestamp().alias("silver_ingestion_time")
        )
    )

# ==========================================
# 3. SILVER STORES (Dimension - SCD TYPE 2)
# ==========================================
@dlt.view(name="stores_cleaned_vw")
@dlt.expect_or_fail("valid_store_pk", "store_id IS NOT NULL")
@dlt.expect("has_contact_info", "store_phone IS NOT NULL")
def stores_cleaned_vw():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.stores")
        .select(
            col("store_id").cast("int"),
            col("region_id").cast("int"),
            col("store_type"),
            col("store_name"),
            col("store_street_address"),
            col("store_city"),
            col("store_state"),
            col("store_country"),
            col("store_phone"),
            to_date(col("first_opened_date"), "M/d/yyyy").alias("first_opened_date"),
            to_date(col("last_remodel_date"), "M/d/yyyy").alias("last_remodel_date"),
            col("total_sqft").cast("int"),
            col("grocery_sqft").cast("int"),
            col("ingestion_time").alias("bronze_ingestion_time")
        )
    )

dlt.create_streaming_table(
    name="stores", 
    comment="SCD Type 2 dimension for stores (Tracks historical remodel changes).",
    table_properties={
        "layer": "silver",
        "domain": "reference_data",
        "contains_pii": "false",
        "scd_type": "2"
    }
)

dlt.apply_changes(
    target="stores",
    source="stores_cleaned_vw",
    keys=["store_id"],
    sequence_by=col("bronze_ingestion_time"),
    stored_as_scd_type=2, 
    track_history_column_list=["total_sqft", "grocery_sqft", "last_remodel_date"] 
)

# ==========================================
# 4. SILVER REGIONS (Dimension - SCD TYPE 1)
# ==========================================
@dlt.view(name="regions_cleaned_vw")
@dlt.expect_or_fail("valid_region_pk", "region_id IS NOT NULL")
def regions_cleaned_vw():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.regions")
        .select(
            col("region_id").cast("int"),
            col("sales_district"),
            col("sales_region"),
            col("ingestion_time").alias("bronze_ingestion_time")
        )
    )

dlt.create_streaming_table(
    name="regions", 
    comment="SCD Type 1 dimension for regions (Overwrites typos/hierarchy shifts).",
    table_properties={
        "layer": "silver",
        "domain": "reference_data",
        "contains_pii": "false",
        "scd_type": "1"
    }
)

dlt.apply_changes(
    target="regions",
    source="regions_cleaned_vw",
    keys=["region_id"],
    sequence_by=col("bronze_ingestion_time"),
    stored_as_scd_type=1 
)

# ==========================================
# 5. SILVER CALENDAR (Dimension - Static)
# ==========================================
@dlt.table(
    name="calendar",
    comment="Enriched calendar dimension table.",
    table_properties={
        "layer": "silver",
        "domain": "reference_data",
        "contains_pii": "false"
    }
)
@dlt.expect("valid_date", "date IS NOT NULL")
def silver_calendar():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.calendar")
        .select(
            to_date(col("date"), "M/d/yyyy").alias("date"),
            col("ingestion_time").alias("bronze_ingestion_time")
        )
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("silver_ingestion_time", current_timestamp())
    )