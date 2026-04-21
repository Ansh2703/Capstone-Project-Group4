



import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, current_timestamp

# Cross-schema reference: read catalog from pipeline configuration
CATALOG       = spark.conf.get("bundle.target_catalog")
BRONZE_SCHEMA = "bronze"
ENV           = "dev"


@dlt.table(
    name="transactions",
    comment="Cleansed fact-sales table sourced from Bronze CSV Auto Loader.",
    table_properties={
        "layer":         "silver",
        "domain":        "sales",
        "contains_pii":  "false",
        "data_quality":  "enforced",
        "scd_type":      "none",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)
@dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
@dlt.expect_or_drop("valid_quantity",         "quantity > 0")
@dlt.expect_or_drop("valid_product_id",       "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_store_id",         "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id",      "customer_id IS NOT NULL")
@dlt.expect(        "stock_before_sale",      "stock_date <= transaction_date")
def silver_transactions():
    raw_txn_date = to_date(col("transaction_date"), "M/d/yyyy")
    raw_stk_date = to_date(col("stock_date"),       "M/d/yyyy")

    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_transactions")
        .select(
            raw_txn_date.alias("transaction_date"),
            raw_stk_date.alias("stock_date"),
            col("product_id").cast("int").alias("product_id"),
            col("customer_id").cast("int").alias("customer_id"),
            col("store_id").cast("int").alias("store_id"),
            col("quantity").cast("int").alias("quantity"),
            F.year(raw_txn_date).alias("transaction_year"),
            F.month(raw_txn_date).alias("transaction_month"),
            F.quarter(raw_txn_date).alias("transaction_quarter"),
            F.dayofweek(raw_txn_date).alias("transaction_day_of_week"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            col("source_file"),
            current_timestamp().alias("silver_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )


@dlt.table(
    name="returns",
    comment="Cleansed returns fact table sourced from Bronze CSV Auto Loader.",
    table_properties={
        "layer":        "silver",
        "domain":       "returns",
        "contains_pii": "false",
        "data_quality": "enforced",
        "scd_type":     "none",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)
@dlt.expect_or_drop("valid_return_date",     "return_date IS NOT NULL")
@dlt.expect_or_drop("valid_return_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_return_product",  "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_return_store",    "store_id IS NOT NULL")
def silver_returns():
    raw_date = to_date(col("return_date"), "M/d/yyyy")

    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_return")
        .select(
            raw_date.alias("return_date"),
            F.year(raw_date).alias("return_year"),
            F.month(raw_date).alias("return_month"),
            col("product_id").cast("int").alias("product_id"),
            col("store_id").cast("int").alias("store_id"),
            col("quantity").cast("int").alias("quantity"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            current_timestamp().alias("silver_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )


@dlt.view(name="stores_cleaned_vw")
@dlt.expect_or_fail("valid_store_pk",  "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_region_fk", "region_id IS NOT NULL")
@dlt.expect(        "has_store_name",  "store_name IS NOT NULL")
@dlt.expect(        "has_contact",     "store_phone IS NOT NULL")
def stores_cleaned_vw():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_stores")
        .select(
            col("store_id").cast("int").alias("store_id"),
            col("region_id").cast("int").alias("region_id"),
            col("store_type"),
            col("store_name"),
            col("store_street_address"),
            col("store_city"),
            col("store_state"),
            col("store_country"),
            col("store_phone"),
            to_date(col("first_opened_date"), "M/d/yyyy").alias("first_opened_date"),
            to_date(col("last_remodel_date"), "M/d/yyyy").alias("last_remodel_date"),
            col("total_sqft").cast("int").alias("total_sqft"),
            col("grocery_sqft").cast("int").alias("grocery_sqft"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )


dlt.create_streaming_table(
    name="stores",
    comment=(
        "SCD Type-2 store dimension. "
        "History tracked for: total_sqft, grocery_sqft, last_remodel_date, store_type. "
        "__START_AT / __END_AT columns managed by DLT apply_changes."
    ),
    table_properties={
        "layer":        "silver",
        "domain":       "reference_data",
        "contains_pii": "false",
        "scd_type":     "2",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)

dlt.apply_changes(
    target           = "stores",
    source           = "stores_cleaned_vw",
    keys             = ["store_id"],
    sequence_by      = col("bronze_ingestion_time"),
    stored_as_scd_type = 2,
    track_history_column_list = [
        "total_sqft",
        "grocery_sqft",
        "last_remodel_date",
        "store_type",
    ],
)


@dlt.view(name="regions_cleaned_vw")
@dlt.expect_or_fail("valid_region_pk", "region_id IS NOT NULL")
@dlt.expect(        "has_sales_region", "sales_region IS NOT NULL")
def regions_cleaned_vw():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_regions")
        .select(
            col("region_id").cast("int").alias("region_id"),
            col("sales_district"),
            col("sales_region"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )


dlt.create_streaming_table(
    name="regions",
    comment="SCD Type-1 region dimension. Latest hierarchy label overwrites the previous value.",
    table_properties={
        "layer":        "silver",
        "domain":       "reference_data",
        "contains_pii": "false",
        "scd_type":     "1",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)

dlt.apply_changes(
    target             = "regions",
    source             = "regions_cleaned_vw",
    keys               = ["region_id"],
    sequence_by        = col("bronze_ingestion_time"),
    stored_as_scd_type = 1,
)


@dlt.table(
    name="calendar",
    comment="Enriched calendar dimension with pre-computed date-part columns.",
    table_properties={
        "layer":        "silver",
        "domain":       "reference_data",
        "contains_pii": "false",
        "scd_type":     "none",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
def silver_calendar():
    raw_date = to_date(col("date"), "M/d/yyyy")

    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_calendar")
        .select(
            raw_date.alias("date"),
            F.year(raw_date).alias("year"),
            F.month(raw_date).alias("month"),
            F.quarter(raw_date).alias("quarter"),
            F.dayofweek(raw_date).alias("day_of_week"),
            F.weekofyear(raw_date).alias("week_of_year"),
            F.date_format(raw_date, "EEEE").alias("day_name"),
            F.date_format(raw_date, "MMMM").alias("month_name"),
            F.when(F.dayofweek(raw_date).isin(1, 7), F.lit(True))
             .otherwise(F.lit(False)).alias("is_weekend"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            current_timestamp().alias("silver_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )
