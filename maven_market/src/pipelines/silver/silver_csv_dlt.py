# ---------------------------------------------------------------------------
# Lakeflow Declarative Pipelines: Table Definitions and Descriptions
# ---------------------------------------------------------------------------
# transactions: Cleansed fact-sales table sourced from Bronze CSV Auto Loader.
# returns: Cleansed returns fact table sourced from Bronze CSV Auto Loader.
# stores: SCD Type-2 store dimension. History tracked for: total_sqft, grocery_sqft, last_remodel_date, store_type.
# regions: SCD Type-1 region dimension. Latest hierarchy label overwrites the previous value.
# calendar: Enriched calendar dimension with pre-computed date-part columns.
# ---------------------------------------------------------------------------

import uuid
from datetime import datetime, timezone
import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, current_timestamp

# ── Inline logger (DLT-safe, print-only — avoids cross-directory import) ──
class PipelineLogger:
    def __init__(self, spark=None, layer="unknown", pipeline="maven_market"):
        self.spark = spark
        self.layer = layer
        self.pipeline = pipeline
        self.run_id = str(uuid.uuid4())

    def log(self, level, message, stage,
            status="RUNNING", row_count=None, error=None):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": ts, "run_id": self.run_id,
            "pipeline": self.pipeline, "layer": self.layer,
            "stage": stage, "level": level, "message": message,
            "status": status, "row_count": row_count, "error": error,
        }
        print(f"[LOG] {log_entry}")

    def info(self, message, stage, **kw):
        self.log("INFO", message, stage, **kw)

    def warn(self, message, stage, **kw):
        self.log("WARN", message, stage, **kw)

    def error(self, message, stage, **kw):
        self.log("ERROR", message, stage, **kw)


logger = PipelineLogger(layer="silver")

# Cross-schema reference: read catalog from pipeline configuration
CATALOG       = spark.conf.get("bundle.target_catalog")
BRONZE_SCHEMA = spark.conf.get("bundle.bronze_schema")
ENV           = spark.conf.get("bundle.environment")

logger.info("Silver CSV pipeline starting", stage="silver_csv_init", status="RUNNING")

# ---------------------------------------------------------------------------
# transactions table: Cleansed fact-sales table sourced from Bronze CSV Auto Loader.
# Applies data quality expectations and enriches with date-part columns.
# ---------------------------------------------------------------------------
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
    logger.info("Starting transactions cleansing", stage="silver_transactions")
    try:
        raw_txn_date = to_date(col("transaction_date"), "M/d/yyyy")
        raw_stk_date = to_date(col("stock_date"),       "M/d/yyyy")

        df = (
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
        logger.info("Transactions transformation defined", stage="silver_transactions", status="SUCCESS")
        return df
    except Exception as e:
        logger.error("Transactions cleansing failed", stage="silver_transactions", status="FAILED", error=str(e))
        raise

# ---------------------------------------------------------------------------
# returns table: Cleansed returns fact table sourced from Bronze CSV Auto Loader.
# Applies data quality expectations and enriches with date-part columns.
# ---------------------------------------------------------------------------
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
    logger.info("Starting returns cleansing", stage="silver_returns")
    try:
        raw_date = to_date(col("return_date"), "M/d/yyyy")

        df = (
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
        logger.info("Returns transformation defined", stage="silver_returns", status="SUCCESS")
        return df
    except Exception as e:
        logger.error("Returns cleansing failed", stage="silver_returns", status="FAILED", error=str(e))
        raise

# ---------------------------------------------------------------------------
# stores_cleaned_vw view: Cleansed store dimension data for SCD Type-2 processing.
# Applies data quality expectations.
# ---------------------------------------------------------------------------
@dlt.view(name="stores_cleaned_vw")
@dlt.expect_or_fail("valid_store_pk",  "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_region_fk", "region_id IS NOT NULL")
@dlt.expect(        "has_store_name",  "store_name IS NOT NULL")
@dlt.expect(        "has_contact",     "store_phone IS NOT NULL")
def stores_cleaned_vw():
    logger.info("Starting stores cleansing view", stage="silver_stores_vw")
    try:
        df = (
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
        logger.info("Stores cleansing view defined", stage="silver_stores_vw", status="SUCCESS")
        return df
    except Exception as e:
        logger.error("Stores cleansing view failed", stage="silver_stores_vw", status="FAILED", error=str(e))
        raise

# ---------------------------------------------------------------------------
# stores table: SCD Type-2 store dimension.
# Tracks history for total_sqft, grocery_sqft, last_remodel_date, store_type.
# ---------------------------------------------------------------------------
logger.info("Registering stores SCD Type-2 streaming table", stage="silver_stores")

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

# Apply SCD Type-2 changes to stores table using apply_changes.
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

logger.info("Stores SCD Type-2 apply_changes registered", stage="silver_stores", status="SUCCESS")

# ---------------------------------------------------------------------------
# regions_cleaned_vw view: Cleansed region dimension data for SCD Type-1 processing.
# Applies data quality expectations.
# ---------------------------------------------------------------------------
@dlt.view(name="regions_cleaned_vw")
@dlt.expect_or_fail("valid_region_pk", "region_id IS NOT NULL")
@dlt.expect(        "has_sales_region", "sales_region IS NOT NULL")
def regions_cleaned_vw():
    logger.info("Starting regions cleansing view", stage="silver_regions_vw")
    try:
        df = (
            spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_regions")
            .select(
                col("region_id").cast("int").alias("region_id"),
                col("sales_district"),
                col("sales_region"),
                col("ingestion_time").alias("bronze_ingestion_time"),
                F.lit(ENV).alias("environment"),
            )
        )
        logger.info("Regions cleansing view defined", stage="silver_regions_vw", status="SUCCESS")
        return df
    except Exception as e:
        logger.error("Regions cleansing view failed", stage="silver_regions_vw", status="FAILED", error=str(e))
        raise

# ---------------------------------------------------------------------------
# regions table: SCD Type-1 region dimension.
# Latest hierarchy label overwrites the previous value.
# ---------------------------------------------------------------------------
logger.info("Registering regions SCD Type-1 streaming table", stage="silver_regions")

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

# Apply SCD Type-1 changes to regions table using apply_changes.
dlt.apply_changes(
    target             = "regions",
    source             = "regions_cleaned_vw",
    keys               = ["region_id"],
    sequence_by        = col("bronze_ingestion_time"),
    stored_as_scd_type = 1,
)

logger.info("Regions SCD Type-1 apply_changes registered", stage="silver_regions", status="SUCCESS")

# ---------------------------------------------------------------------------
# calendar table: Enriched calendar dimension with pre-computed date-part columns.
# Applies data quality expectations.
# ---------------------------------------------------------------------------
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
    logger.info("Starting calendar enrichment", stage="silver_calendar")
    try:
        raw_date = to_date(col("date"), "M/d/yyyy")

        df = (
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
        logger.info("Calendar enrichment defined", stage="silver_calendar", status="SUCCESS")
        return df
    except Exception as e:
        logger.error("Calendar enrichment failed", stage="silver_calendar", status="FAILED", error=str(e))
        raise

logger.info("All Silver CSV tables registered", stage="silver_csv_pipeline", status="SUCCESS")
