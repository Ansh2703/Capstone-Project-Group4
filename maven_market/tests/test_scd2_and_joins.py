"""
test_scd2_and_joins.py — Unit tests for SCD-2 filtering, joins, and deduplication.

PURPOSE:
    The Gold layer reads SCD-2 tables (customers, products, stores) by filtering
    on __END_AT IS NULL to get the current snapshot. It then joins fact tables
    with dimension tables and applies deduplication. These tests verify that
    logic in isolation using small in-memory DataFrames.

WHAT THIS FILE COVERS:
    1.  SCD-2 Current Snapshot Filter – __END_AT IS NULL selects active rows
    2.  SCD-2 With History Rows       – Expired rows (__END_AT not null) excluded
    3.  SCD-2 All Expired             – Edge case: no current records
    4.  Fact-to-Product Left Join     – fact_sales enriched with product prices
    5.  Fact-to-Product Unmatched     – Left join preserves facts with no product match
    6.  Store-to-Region Denormalization – dim_store joined with regions
    7.  Store-to-Region Missing FK    – Left join handles NULL region_id
    8.  Deduplication by Key          – dropDuplicates on date_key, region_id, etc.
    9.  Deduplication Preserves First – Only first occurrence kept
    10. Fact Returns Join             – fact_returns enriched with product prices
    11. Stream-Static Join Pattern    – Orders joined with current SCD-2 stores

HOW TO RUN:
    From the repo root:  pytest tests/test_scd2_and_joins.py -v
"""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType,
    TimestampType, DateType
)
from datetime import datetime, date


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1: SCD-2 Current Snapshot — __END_AT IS NULL
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → dim_customer, dim_product, dim_store
# LOGIC:  .filter(col("__END_AT").isNull())
#
# SIMPLE EXPLANATION:
#   SCD-2 tables track history. Each row has __START_AT (when it became active)
#   and __END_AT (when it was superseded). __END_AT IS NULL = current row.
#   The Gold layer only wants the CURRENT version of each entity.

def test_scd2_current_snapshot_filter(spark):
    """Only rows where __END_AT IS NULL should survive the filter."""

    data = [
        (1, "Alice", "alice@v1.com", datetime(2023, 1, 1), datetime(2024, 1, 1)),  # expired
        (1, "Alice", "alice@v2.com", datetime(2024, 1, 1), None),                   # current
        (2, "Bob",   "bob@test.com", datetime(2023, 6, 1), None),                   # current
    ]
    schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("first_name", StringType()),
        StructField("email_address", StringType()),
        StructField("__START_AT", TimestampType(), True),
        StructField("__END_AT", TimestampType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    current_df = df.filter(F.col("__END_AT").isNull())

    assert current_df.count() == 2

    rows = current_df.orderBy("customer_id").collect()
    # Alice's current email should be v2
    assert rows[0]["email_address"] == "alice@v2.com"
    # Bob should be present
    assert rows[1]["customer_id"] == 2


def test_scd2_multiple_history_versions(spark):
    """A product with 3 versions should only return the latest (current) one."""

    data = [
        (10, "Widget", 5.99, 3.00, datetime(2022, 1, 1), datetime(2023, 1, 1)),
        (10, "Widget", 6.99, 3.50, datetime(2023, 1, 1), datetime(2024, 1, 1)),
        (10, "Widget", 7.99, 4.00, datetime(2024, 1, 1), None),  # current
        (20, "Gadget", 9.99, 5.00, datetime(2023, 6, 1), None),  # current
    ]
    schema = StructType([
        StructField("product_id", IntegerType()),
        StructField("product_name", StringType()),
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType()),
        StructField("__START_AT", TimestampType(), True),
        StructField("__END_AT", TimestampType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    current_df = df.filter(F.col("__END_AT").isNull())

    assert current_df.count() == 2

    rows = current_df.orderBy("product_id").collect()
    # Widget's current price should be 7.99 (the latest)
    assert rows[0]["product_retail_price"] == 7.99
    assert rows[0]["product_cost"] == 4.00
    assert rows[1]["product_id"] == 20


def test_scd2_all_expired_returns_empty(spark):
    """If all rows are expired (no current version), result should be empty."""

    data = [
        (1, "OldProduct", 5.0, datetime(2020, 1, 1), datetime(2021, 1, 1)),
        (1, "OldProduct", 6.0, datetime(2021, 1, 1), datetime(2022, 1, 1)),
    ]
    schema = StructType([
        StructField("product_id", IntegerType()),
        StructField("product_name", StringType()),
        StructField("product_retail_price", DoubleType()),
        StructField("__START_AT", TimestampType(), True),
        StructField("__END_AT", TimestampType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    current_df = df.filter(F.col("__END_AT").isNull())
    assert current_df.count() == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4: Fact Sales ↔ Product Left Join (Revenue Enrichment)
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → fact_sales
# LOGIC:
#   txn.join(prod, txn.product_id == prod.p_product_id, "left")
#   .withColumn("revenue", round(quantity * product_retail_price, 2))
#   .withColumn("cost", round(quantity * product_cost, 2))
#   .withColumn("gross_profit", round(revenue - cost, 2))

def test_fact_sales_product_join_enrichment(spark):
    """Fact sales joined with products must compute revenue, cost, and profit."""

    # Transactions
    txn_data = [
        (date(2024, 1, 15), 100, 1, 1, 3),
        (date(2024, 1, 16), 200, 2, 1, 1),
    ]
    txn_schema = StructType([
        StructField("transaction_date", DateType()),
        StructField("product_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("store_id", IntegerType()),
        StructField("quantity", IntegerType()),
    ])
    txn = spark.createDataFrame(txn_data, txn_schema)

    # Current products (already filtered by __END_AT IS NULL)
    prod_data = [
        (100, 10.00, 6.00),
        (200, 25.00, 15.00),
    ]
    prod_schema = StructType([
        StructField("p_product_id", IntegerType()),
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType()),
    ])
    prod = spark.createDataFrame(prod_data, prod_schema)

    # Apply exact same join logic as gold_dlt.py
    result_df = (
        txn.join(prod, txn.product_id == prod.p_product_id, "left")
        .drop("p_product_id")
        .withColumn("revenue", F.round(F.col("quantity") * F.col("product_retail_price"), 2))
        .withColumn("cost", F.round(F.col("quantity") * F.col("product_cost"), 2))
        .withColumn("gross_profit", F.round(F.col("revenue") - F.col("cost"), 2))
    )

    rows = result_df.orderBy("product_id").collect()

    # Product 100: 3 * 10.00 = 30.00 revenue, 3 * 6.00 = 18.00 cost
    assert rows[0]["revenue"] == 30.00
    assert rows[0]["cost"] == 18.00
    assert rows[0]["gross_profit"] == 12.00

    # Product 200: 1 * 25.00 = 25.00 revenue, 1 * 15.00 = 15.00 cost
    assert rows[1]["revenue"] == 25.00
    assert rows[1]["cost"] == 15.00
    assert rows[1]["gross_profit"] == 10.00


def test_fact_sales_unmatched_product_left_join(spark):
    """Left join: if product_id doesn't match, revenue/cost should be NULL."""

    txn_data = [(date(2024, 1, 15), 999, 1, 1, 2)]
    txn_schema = StructType([
        StructField("transaction_date", DateType()),
        StructField("product_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("store_id", IntegerType()),
        StructField("quantity", IntegerType()),
    ])
    txn = spark.createDataFrame(txn_data, txn_schema)

    # Product 999 does NOT exist in products table
    prod_data = [(100, 10.00, 6.00)]
    prod_schema = StructType([
        StructField("p_product_id", IntegerType()),
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType()),
    ])
    prod = spark.createDataFrame(prod_data, prod_schema)

    result_df = (
        txn.join(prod, txn.product_id == prod.p_product_id, "left")
        .drop("p_product_id")
        .withColumn("revenue", F.round(F.col("quantity") * F.col("product_retail_price"), 2))
    )

    row = result_df.collect()[0]

    # Transaction row should survive (left join), but price columns are NULL
    assert row["product_id"] == 999
    assert row["product_retail_price"] is None
    assert row["revenue"] is None


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 6: Store ↔ Region Denormalization Join
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → dim_store
# LOGIC:
#   stores.join(regions, stores.region_id == regions.r_region_id, "left")
#   Collapses the Kimball snowflake by bringing region attributes into dim_store

def test_store_region_denormalization(spark):
    """Store dimension must inherit sales_district and sales_region from regions."""

    stores_data = [
        (1, 10, "Supermarket", "Downtown Store"),
        (2, 20, "Deluxe",      "Mall Store"),
    ]
    stores_schema = StructType([
        StructField("store_id", IntegerType()),
        StructField("region_id", IntegerType()),
        StructField("store_type", StringType()),
        StructField("store_name", StringType()),
    ])
    stores = spark.createDataFrame(stores_data, stores_schema)

    regions_data = [
        (10, "Northwest", "North America"),
        (20, "Central",   "Europe"),
    ]
    regions_schema = StructType([
        StructField("r_region_id", IntegerType()),
        StructField("sales_district", StringType()),
        StructField("sales_region", StringType()),
    ])
    regions = spark.createDataFrame(regions_data, regions_schema)

    result_df = (
        stores.join(regions, stores.region_id == regions.r_region_id, "left")
        .drop("r_region_id")
        .orderBy("store_id")
    )

    rows = result_df.collect()

    assert rows[0]["store_name"] == "Downtown Store"
    assert rows[0]["sales_district"] == "Northwest"
    assert rows[0]["sales_region"] == "North America"

    assert rows[1]["store_name"] == "Mall Store"
    assert rows[1]["sales_region"] == "Europe"


def test_store_region_missing_region_fk(spark):
    """If a store has a region_id that doesn't exist in regions, left join fills NULLs."""

    stores_data = [(1, 10, "Known Store"), (2, 99, "Orphan Store")]
    stores_schema = StructType([
        StructField("store_id", IntegerType()),
        StructField("region_id", IntegerType()),
        StructField("store_name", StringType()),
    ])
    stores = spark.createDataFrame(stores_data, stores_schema)

    regions_data = [(10, "Northwest", "North America")]
    regions_schema = StructType([
        StructField("r_region_id", IntegerType()),
        StructField("sales_district", StringType()),
        StructField("sales_region", StringType()),
    ])
    regions = spark.createDataFrame(regions_data, regions_schema)

    result_df = (
        stores.join(regions, stores.region_id == regions.r_region_id, "left")
        .drop("r_region_id")
        .orderBy("store_id")
    )

    rows = result_df.collect()

    # Store 1 matches region 10
    assert rows[0]["sales_region"] == "North America"
    # Store 2 has no matching region — should be NULL, not dropped
    assert rows[1]["store_name"] == "Orphan Store"
    assert rows[1]["sales_region"] is None


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 8: Deduplication — dropDuplicates on Key Column
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → dim_date, dim_region
# LOGIC:  .dropDuplicates(["date_key"]) / .dropDuplicates(["region_id"])
#
# SIMPLE EXPLANATION:
#   If the same date or region appears multiple times (e.g., from multiple
#   ingestion batches), the Gold layer deduplicates on the primary key.

def test_deduplication_by_date_key(spark):
    """Duplicate date_key rows must be reduced to one per key."""

    data = [
        (20240101, date(2024, 1, 1), 2024, 1),
        (20240101, date(2024, 1, 1), 2024, 1),  # duplicate
        (20240102, date(2024, 1, 2), 2024, 1),
    ]
    schema = StructType([
        StructField("date_key", IntegerType()),
        StructField("date", DateType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = df.dropDuplicates(["date_key"])

    # 3 input rows → 2 unique date_keys
    assert result_df.count() == 2


def test_deduplication_by_region_id(spark):
    """Duplicate region_id rows must be reduced to one per region."""

    data = [
        (10, "Northwest", "North America"),
        (10, "Northwest", "North America"),  # exact duplicate
        (20, "Central",   "Europe"),
    ]
    schema = StructType([
        StructField("region_id", IntegerType()),
        StructField("sales_district", StringType()),
        StructField("sales_region", StringType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = df.dropDuplicates(["region_id"])
    assert result_df.count() == 2


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 10: Fact Returns ↔ Product Join
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → fact_returns
# LOGIC:
#   ret.join(prod, ret.product_id == prod.p_product_id, "left")
#   .withColumn("return_revenue", round(quantity * product_retail_price, 2))
#   .withColumn("return_cost", round(quantity * product_cost, 2))

def test_fact_returns_product_join(spark):
    """Fact returns must be enriched with return_revenue and return_cost."""

    ret_data = [
        (date(2024, 2, 1), 100, 1, 2, 2024),
        (date(2024, 3, 1), 200, 2, 1, 2024),
    ]
    ret_schema = StructType([
        StructField("return_date", DateType()),
        StructField("product_id", IntegerType()),
        StructField("store_id", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("return_year", IntegerType()),
    ])
    ret = spark.createDataFrame(ret_data, ret_schema)

    prod_data = [
        (100, 10.00, 6.00),
        (200, 25.00, 15.00),
    ]
    prod_schema = StructType([
        StructField("p_product_id", IntegerType()),
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType()),
    ])
    prod = spark.createDataFrame(prod_data, prod_schema)

    result_df = (
        ret.join(prod, ret.product_id == prod.p_product_id, "left")
        .drop("p_product_id")
        .withColumn("return_revenue", F.round(F.col("quantity") * F.col("product_retail_price"), 2))
        .withColumn("return_cost", F.round(F.col("quantity") * F.col("product_cost"), 2))
    )

    rows = result_df.orderBy("product_id").collect()

    # Product 100: 2 * 10.00 = 20.00 return revenue
    assert rows[0]["return_revenue"] == 20.00
    assert rows[0]["return_cost"] == 12.00

    # Product 200: 1 * 25.00 = 25.00 return revenue
    assert rows[1]["return_revenue"] == 25.00
    assert rows[1]["return_cost"] == 15.00


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 11: Stream-Static Join Pattern — Orders ↔ Current SCD-2 Stores
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_kafka_dlt.py → silver_orders
# LOGIC:
#   stores_static = dlt.read("stores").filter(col("__END_AT").isNull())
#   orders_stream.join(stores_static, ..., "left")
#
# SIMPLE EXPLANATION:
#   Kafka orders are joined with the current snapshot of the stores table
#   to enrich each order with store_city and region_id.

def test_orders_join_current_scd2_stores(spark):
    """Orders must be enriched with store attributes from current SCD-2 snapshot."""

    # Orders
    orders_data = [
        (1, 1, datetime(2024, 1, 1, 10, 0), 100, 3),
        (2, 2, datetime(2024, 1, 1, 11, 0), 200, 1),
        (3, 99, datetime(2024, 1, 1, 12, 0), 300, 2),  # store 99 doesn't exist
    ]
    orders_schema = StructType([
        StructField("order_id", IntegerType()),
        StructField("store_id", IntegerType()),
        StructField("event_timestamp", TimestampType()),
        StructField("product_id", IntegerType()),
        StructField("quantity", IntegerType()),
    ])
    orders = spark.createDataFrame(orders_data, orders_schema)

    # Current stores (already filtered __END_AT IS NULL)
    stores_data = [
        (1, "Portland", "Oregon", 10),
        (2, "Seattle",  "Washington", 10),
    ]
    stores_schema = StructType([
        StructField("s_store_id", IntegerType()),
        StructField("store_city", StringType()),
        StructField("store_state", StringType()),
        StructField("region_id", IntegerType()),
    ])
    stores = spark.createDataFrame(stores_data, stores_schema)

    result_df = (
        orders.join(stores, orders.store_id == stores.s_store_id, "left")
        .select("order_id", "store_id", "product_id", "quantity",
                "store_city", "store_state", "region_id")
        .orderBy("order_id")
    )

    rows = result_df.collect()

    # Order 1: store 1 → Portland
    assert rows[0]["store_city"] == "Portland"
    assert rows[0]["region_id"] == 10

    # Order 2: store 2 → Seattle
    assert rows[1]["store_city"] == "Seattle"

    # Order 3: store 99 → no match, NULL enrichment (left join)
    assert rows[2]["store_city"] is None
    assert rows[2]["region_id"] is None
    assert rows[2]["store_id"] == 99  # order preserved
