"""
test_data_quality.py — Tests that simulate DLT data quality expectations.

PURPOSE:
    Your pipeline uses DLT expectations like:
      - @dlt.expect_or_fail("valid_customer_pk", "customer_id IS NOT NULL")
      - @dlt.expect_or_drop("valid_retail_price", "product_retail_price > 0")
      - @dlt.expect("valid_gender", "gender IN ('M', 'F')")

    DLT expectations only run inside a live pipeline — you CANNOT unit test them
    directly. Instead, this file recreates the same filter logic so you can verify
    that your data quality rules work BEFORE deploying to production.

WHAT THIS FILE COVERS:
    1.  Null Primary Key Rejection   – customer_id, product_id, store_id must not be NULL
    2.  Negative Price Rejection     – product_retail_price and product_cost must be > 0
    3.  Gender Validation            – gender must be 'M' or 'F'
    4.  Quantity Must Be Positive    – quantity > 0 for transactions and returns
    5.  Price Above Cost Rule        – retail_price should exceed cost
    6.  Null Foreign Key Rejection   – store_id, region_id in joins must not be NULL
    7.  Valid Event Type Check       – Kafka inventory event_type must be in allowed list
    8.  Stock Level Non-Negative     – stock_level >= 0 for inventory events
    9.  Date Not Null Rejection      – transaction_date, return_date must not be NULL
    10. Combined Quality Check       – multiple rules applied to one DataFrame

HOW TO RUN:
    From the repo root:  pytest tests/test_data_quality.py -v
"""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, DateType
)
from datetime import date


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1: Null Primary Key Rejection (expect_or_fail simulation)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect_or_fail("valid_customer_pk", "customer_id IS NOT NULL")
#
# SIMPLE EXPLANATION:
#   Every customer MUST have a customer_id. If it's NULL, the pipeline should
#   FAIL (expect_or_fail). This test verifies that a filter on IS NOT NULL
#   correctly removes rows with NULL IDs.

def test_null_customer_id_rejected(spark):
    """Rows with NULL customer_id must be filtered out (simulates expect_or_fail)."""

    data = [(1, "alice@test.com"), (None, "bob@test.com"), (3, None)]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("email_address", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    # Simulate: expect_or_fail("valid_customer_pk", "customer_id IS NOT NULL")
    valid_df = df.filter(F.col("customer_id").isNotNull())

    # Only 2 rows should survive — the one with NULL customer_id is removed
    assert valid_df.count() == 2

    # Verify the NULL row is gone
    ids = [row["customer_id"] for row in valid_df.collect()]
    assert None not in ids
    assert 1 in ids
    assert 3 in ids


def test_null_product_id_rejected(spark):
    """Rows with NULL product_id must be filtered out."""

    data = [(None, "Widget"), (10, "Gadget")]
    schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    valid_df = df.filter(F.col("product_id").isNotNull())
    assert valid_df.count() == 1
    assert valid_df.collect()[0]["product_name"] == "Gadget"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2: Negative and Zero Price Rejection (expect_or_drop simulation)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect_or_drop("valid_retail_price", "product_retail_price > 0")
#            @dlt.expect_or_drop("valid_cost", "product_cost > 0")
#
# SIMPLE EXPLANATION:
#   A product with a $0 or negative price is bad data — it would break
#   margin calculations. expect_or_drop silently removes these rows.

def test_zero_and_negative_prices_dropped(spark):
    """Products with price <= 0 or cost <= 0 must be dropped."""

    data = [
        (1, 10.0, 5.0),     # valid — keep
        (2, 0.0, 5.0),      # invalid price — drop
        (3, 10.0, -1.0),    # invalid cost — drop
        (4, -5.0, -3.0),    # both invalid — drop
        (5, 0.01, 0.01),    # edge case: tiny but positive — keep
    ]
    schema = StructType([
        StructField("product_id", IntegerType()),
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)

    # Simulate: expect_or_drop for BOTH price AND cost
    valid_df = df.filter(
        (F.col("product_retail_price") > 0) & (F.col("product_cost") > 0)
    )

    assert valid_df.count() == 2
    valid_ids = [row["product_id"] for row in valid_df.collect()]
    assert 1 in valid_ids
    assert 5 in valid_ids


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3: Gender Validation (expect — soft warning, no drop)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect("valid_gender", "gender IN ('M', 'F')")
#
# SIMPLE EXPLANATION:
#   DLT "expect" (without _or_fail/_or_drop) logs a warning but keeps the row.
#   This test checks which rows WOULD pass the rule and which would not.

def test_gender_validation(spark):
    """Only 'M' and 'F' should pass the gender validation rule."""

    data = [("M",), ("F",), ("X",), ("",), (None,)]
    df = spark.createDataFrame(data, ["gender"])

    # Count rows that PASS the gender rule
    passing_df = df.filter(F.col("gender").isin("M", "F"))
    failing_df = df.filter(~F.col("gender").isin("M", "F") | F.col("gender").isNull())

    assert passing_df.count() == 2   # Only "M" and "F"
    assert failing_df.count() == 3   # "X", "", and NULL


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4: Quantity Must Be Positive (expect_or_drop simulation)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect_or_drop("valid_quantity", "quantity > 0")
#
# SIMPLE EXPLANATION:
#   A transaction with 0 or negative quantity makes no business sense.
#   These rows are silently dropped by the pipeline.

def test_quantity_must_be_positive(spark):
    """Transactions with quantity <= 0 must be dropped."""

    data = [(1, 5), (2, 0), (3, -1), (4, 1)]
    schema = StructType([
        StructField("transaction_id", IntegerType()),
        StructField("quantity", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)

    valid_df = df.filter(F.col("quantity") > 0)

    assert valid_df.count() == 2
    valid_ids = [row["transaction_id"] for row in valid_df.collect()]
    assert 1 in valid_ids
    assert 4 in valid_ids


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 5: Price Above Cost Rule (expect — soft warning)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect("price_above_cost", "product_retail_price > product_cost")
#
# SIMPLE EXPLANATION:
#   Normally a product's retail price is higher than its cost.
#   If not, it means the store is selling at a loss. The pipeline logs a
#   warning but keeps the row (it might be a valid clearance sale).

def test_price_above_cost_check(spark):
    """Identify rows where retail price does NOT exceed cost."""

    data = [
        (1, 10.0, 5.0),    # normal — price > cost ✓
        (2, 5.0, 5.0),     # edge — price == cost (fails the > check)
        (3, 3.0, 8.0),     # loss — price < cost (fails)
    ]
    schema = StructType([
        StructField("product_id", IntegerType()),
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)

    passing = df.filter(F.col("product_retail_price") > F.col("product_cost"))
    failing = df.filter(~(F.col("product_retail_price") > F.col("product_cost")))

    assert passing.count() == 1      # Only product_id=1
    assert failing.count() == 2      # product_id=2 and 3


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 6: Null Foreign Key Rejection (stores must have region_id)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect_or_drop("valid_region_fk", "region_id IS NOT NULL")
#
# SIMPLE EXPLANATION:
#   Every store must belong to a region. If region_id is NULL, the Gold
#   layer join to dim_region would produce incomplete data.

def test_null_region_id_dropped(spark):
    """Stores without a region_id must be filtered out."""

    data = [(1, 10), (2, None), (3, 20)]
    schema = StructType([
        StructField("store_id", IntegerType()),
        StructField("region_id", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    valid_df = df.filter(F.col("region_id").isNotNull())
    assert valid_df.count() == 2


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 7: Valid Event Type Check (Kafka Inventory)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect("valid_event_type",
#              "event_type IN ('RESTOCK', 'SALE', 'ADJUSTMENT', 'RETURN')")
#
# SIMPLE EXPLANATION:
#   The Kafka inventory stream should only send 4 types of events.
#   Anything else (like "UNKNOWN") is flagged as a data quality issue.

def test_valid_inventory_event_types(spark):
    """Only the 4 allowed event types should pass validation."""

    valid_types = ["RESTOCK", "SALE", "ADJUSTMENT", "RETURN"]
    data = [("RESTOCK",), ("SALE",), ("ADJUSTMENT",), ("RETURN",), ("UNKNOWN",), ("",)]
    df = spark.createDataFrame(data, ["event_type"])

    passing = df.filter(F.col("event_type").isin(valid_types))
    failing = df.filter(~F.col("event_type").isin(valid_types))

    assert passing.count() == 4
    assert failing.count() == 2


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 8: Stock Level Non-Negative (Kafka Inventory)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect_or_drop("valid_stock_level", "stock_level >= 0")
#
# SIMPLE EXPLANATION:
#   You can have 0 items in stock (out of stock), but you can never have
#   -5 items. Negative stock levels are data errors.

def test_stock_level_non_negative(spark):
    """Negative stock levels must be filtered out."""

    data = [(1, 100), (2, 0), (3, -5), (4, 1)]
    schema = StructType([
        StructField("event_id", IntegerType()),
        StructField("stock_level", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)

    valid_df = df.filter(F.col("stock_level") >= 0)
    assert valid_df.count() == 3  # event_id 1, 2, 4

    invalid_df = df.filter(F.col("stock_level") < 0)
    assert invalid_df.count() == 1  # only event_id=3


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 9: Date Not Null Rejection (Transactions and Returns)
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATES: @dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
#            @dlt.expect_or_drop("valid_return_date", "return_date IS NOT NULL")
#
# SIMPLE EXPLANATION:
#   A transaction without a date cannot be placed on a timeline or joined
#   to the calendar dimension. These rows must be dropped.

def test_null_dates_dropped(spark):
    """Rows with NULL transaction_date must be removed."""

    data = [
        (1, date(2024, 1, 15)),
        (2, None),
        (3, date(2024, 6, 1)),
    ]
    schema = StructType([
        StructField("transaction_id", IntegerType()),
        StructField("transaction_date", DateType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    valid_df = df.filter(F.col("transaction_date").isNotNull())
    assert valid_df.count() == 2


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 10: Combined Quality Check (Multiple Rules on One DataFrame)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: In production, MULTIPLE expectations are applied to the same table.
#       This test simulates applying all transaction expectations at once,
#       just like the real silver_csv_dlt.py does.
#
# SIMPLE EXPLANATION:
#   A transaction must pass ALL of these to survive:
#     - transaction_date IS NOT NULL
#     - quantity > 0
#     - product_id IS NOT NULL
#     - store_id IS NOT NULL
#     - customer_id IS NOT NULL

def test_combined_transaction_quality_rules(spark):
    """Apply all 5 transaction expectations at once — only clean rows survive."""

    data = [
        # (txn_date,          product_id, store_id, customer_id, quantity)
        (date(2024, 1, 1),    1,          1,        1,           5),       # VALID
        (None,                2,          2,        2,           3),       # bad: null date
        (date(2024, 2, 1),    None,       3,        3,           2),       # bad: null product
        (date(2024, 3, 1),    4,          None,     4,           1),       # bad: null store
        (date(2024, 4, 1),    5,          5,        None,        4),       # bad: null customer
        (date(2024, 5, 1),    6,          6,        6,           0),       # bad: zero quantity
        (date(2024, 6, 1),    7,          7,        7,           -1),      # bad: negative qty
        (date(2024, 7, 1),    8,          8,        8,           10),      # VALID
    ]
    schema = StructType([
        StructField("transaction_date", DateType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("quantity", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)

    # Apply ALL 5 rules simultaneously (same as silver_csv_dlt.py)
    valid_df = df.filter(
        (F.col("transaction_date").isNotNull()) &
        (F.col("product_id").isNotNull()) &
        (F.col("store_id").isNotNull()) &
        (F.col("customer_id").isNotNull()) &
        (F.col("quantity") > 0)
    )

    # Only 2 rows should survive — the first and the last
    assert valid_df.count() == 2

    quantities = sorted([row["quantity"] for row in valid_df.collect()])
    assert quantities == [5, 10]
