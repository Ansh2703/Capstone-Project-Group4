import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp

# Cross-schema reference: read catalog from pipeline configuration
CATALOG       = spark.conf.get("bundle.target_catalog")
BRONZE_SCHEMA = "bronze"
ENV           = "dev"


@dlt.table(
    name="orders",
    comment=(
        "Cleansed real-time order events from Confluent Kafka (via Bronze Delta). "
        "Enriched with store_city and sales_region via Stream-Static join."
    ),
    table_properties={
        "layer":        "silver",
        "domain":       "orders",
        "contains_pii": "false",
        "data_quality": "enforced",
        "join_type":    "stream_static",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
@dlt.expect_or_drop("valid_order_id",      "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_order_product",  "product_id IS NOT NULL")
@dlt.expect(        "has_store_id",         "store_id IS NOT NULL")
def silver_orders():
    orders_stream = (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_orders_kafka")
        .select(
            col("order_id").cast("int").alias("order_id"),
            F.to_timestamp(col("event_time")).alias("event_timestamp"),
            col("product_id").cast("int").alias("product_id"),
            col("store_id").cast("int").alias("store_id"),
            col("customer_id").cast("int").alias("customer_id"),
            col("quantity").cast("int").alias("quantity"),
            F.to_date(F.to_timestamp(col("event_time"))).alias("order_date"),
            F.year(F.to_timestamp(col("event_time"))).alias("order_year"),
            F.month(F.to_timestamp(col("event_time"))).alias("order_month"),
        )
    )

    # stores is in the same silver pipeline, use dlt.read for pipeline-internal reference
    stores_static = (
        dlt.read("stores")
        .filter(col("__END_AT").isNull())
        .select(
            col("store_id").alias("s_store_id"),
            col("store_city"),
            col("store_state"),
            col("region_id"),
        )
    )

    return (
        orders_stream
        .join(stores_static, orders_stream.store_id == stores_static.s_store_id, "left")
        .select(
            col("order_id"),
            col("event_timestamp"),
            col("product_id"),
            col("store_id"),
            col("customer_id"),
            col("quantity"),
            col("order_date"),
            col("order_year"),
            col("order_month"),
            col("store_city"),
            col("store_state"),
            col("region_id"),
            current_timestamp().alias("silver_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )


@dlt.table(
    name="inventory",
    comment="Real-time inventory events from Confluent Kafka with stock-health classification.",
    table_properties={
        "layer":        "silver",
        "domain":       "inventory",
        "contains_pii": "false",
        "data_quality": "enforced",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
@dlt.expect_or_drop("valid_inventory_event", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_stock_level",     "stock_level >= 0")
@dlt.expect(        "valid_event_type",      "event_type IN ('RESTOCK', 'SALE', 'ADJUSTMENT', 'RETURN')")
def silver_inventory():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_inventory_kafka")
        .select(
            col("event_id").cast("int").alias("event_id"),
            F.to_timestamp(col("event_time")).alias("event_timestamp"),
            col("product_id").cast("int").alias("product_id"),
            col("store_id").cast("int").alias("store_id"),
            col("stock_level").cast("int").alias("stock_level"),
            col("event_type"),
            F.when(col("stock_level") == 0,  F.lit("OUT_OF_STOCK"))
             .when(col("stock_level") < 10,  F.lit("LOW"))
             .when(col("stock_level") < 50,  F.lit("MEDIUM"))
             .otherwise(F.lit("HEALTHY"))
             .alias("stock_status"),
            current_timestamp().alias("silver_ingestion_time"),
            F.lit(ENV).alias("environment"),
        )
    )
