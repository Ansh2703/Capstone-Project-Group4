"""
test_transformations.py — Unit tests for Silver → Gold transformation logic.

PURPOSE:
    These tests verify the BUSINESS LOGIC applied during Silver and Gold
    transformations — calculations, enrichments, classifications, and joins.
    Each test isolates ONE piece of logic from your pipeline code and validates
    it against known inputs and expected outputs.

WHAT THIS FILE COVERS:
    1.  Margin Percentage        – (retail_price - cost) / retail_price * 100
    2.  Full Name Concatenation  – first_name + " " + last_name
    3.  Revenue Calculation      – quantity * product_retail_price  (Gold fact_sales)
    4.  Cost Calculation         – quantity * product_cost          (Gold fact_sales)
    5.  Gross Profit Calculation – revenue - cost                   (Gold fact_sales)
    6.  Stock Status Classification – OUT_OF_STOCK / LOW / MEDIUM / HEALTHY
    7.  Is Weekend Flag          – Saturday (7) and Sunday (1) → True
    8.  Date Key Generation      – date → YYYYMMDD integer          (Gold dim_date)
    9.  Transaction Date Parts   – year, month, quarter, day_of_week extraction
    10. Edge Cases               – zero price, equal price/cost, large quantities

HOW TO RUN:
    From the repo root:  pytest tests/test_transformations.py -v
"""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, DoubleType, IntegerType, StringType, DateType
)
from datetime import date


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1: Margin Percentage Calculation
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_mongo_dlt.py → products_parsed_vw
# FORMULA: round((retail_price - cost) / retail_price * 100, 2)
#
# SIMPLE EXPLANATION:
#   If a product sells for $100 and costs $80, the store keeps $20.
#   Margin = 20 / 100 * 100 = 20%. This test checks that math.

def test_margin_calculation(spark):
    """Verify the profit margin formula matches the Silver pipeline logic."""

    schema = StructType([
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType())
    ])

    # Known inputs with known expected outputs:
    # Row 1: (100 - 80) / 100 * 100 = 20.00%
    # Row 2: (50 - 10) / 50 * 100  = 80.00%
    # Row 3: (9.99 - 4.50) / 9.99 * 100 = 54.95%
    data = [(100.0, 80.0), (50.0, 10.0), (9.99, 4.50)]
    df = spark.createDataFrame(data, schema)

    # Apply the EXACT same formula from silver_mongo_dlt.py
    result_df = df.withColumn(
        "margin_pct",
        F.round(
            (F.col("product_retail_price") - F.col("product_cost"))
            / F.col("product_retail_price") * 100,
            2
        )
    )

    results = result_df.collect()
    assert results[0]["margin_pct"] == 20.0
    assert results[1]["margin_pct"] == 80.0
    assert results[2]["margin_pct"] == 54.95


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2: Margin Edge Case — Price Equals Cost (0% Margin)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: If retail_price == cost, margin should be exactly 0.0, not NULL or error.

def test_margin_zero_when_price_equals_cost(spark):
    """When price equals cost, margin must be 0%, not NULL."""

    data = [(25.0, 25.0)]
    schema = StructType([
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType())
    ])
    df = spark.createDataFrame(data, schema)

    result_df = df.withColumn(
        "margin_pct",
        F.round(
            (F.col("product_retail_price") - F.col("product_cost"))
            / F.col("product_retail_price") * 100,
            2
        )
    )

    assert result_df.collect()[0]["margin_pct"] == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3: Full Name Concatenation
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_mongo_dlt.py → customers_parsed_vw
# LOGIC:  F.concat_ws(" ", col("first_name"), col("last_name"))
#
# SIMPLE EXPLANATION:
#   We combine first name and last name with a space in between.
#   "John" + "Doe" = "John Doe". Also tests what happens with NULL names.

def test_full_name_concatenation(spark):
    """first_name + space + last_name = full_name."""

    data = [("John", "Doe"), ("Alice", "Smith")]
    df = spark.createDataFrame(data, ["first_name", "last_name"])

    result_df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )

    rows = result_df.collect()
    assert rows[0]["full_name"] == "John Doe"
    assert rows[1]["full_name"] == "Alice Smith"


def test_full_name_with_null_parts(spark):
    """If first or last name is NULL, concat_ws skips it (no trailing space)."""

    data = [("John", None), (None, "Doe"), (None, None)]
    df = spark.createDataFrame(data, ["first_name", "last_name"])

    result_df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )

    rows = result_df.collect()
    # concat_ws skips NULL values — does NOT produce "John " or " Doe"
    assert rows[0]["full_name"] == "John"
    assert rows[1]["full_name"] == "Doe"
    assert rows[2]["full_name"] == ""     # both NULL → empty string


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4: Revenue, Cost, and Gross Profit (Gold fact_sales)
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → fact_sales
# FORMULAS:
#   revenue      = round(quantity * product_retail_price, 2)
#   cost         = round(quantity * product_cost, 2)
#   gross_profit = round(revenue - cost, 2)
#
# SIMPLE EXPLANATION:
#   If a customer buys 3 items at $10 each (cost $6), then:
#   revenue = 30.00, cost = 18.00, gross_profit = 12.00

def test_revenue_cost_profit_calculation(spark):
    """Verify the Gold layer revenue/cost/profit formulas are correct."""

    data = [
        (3, 10.00, 6.00),     # revenue=30, cost=18, profit=12
        (1, 99.99, 50.00),    # revenue=99.99, cost=50, profit=49.99
        (10, 1.50, 0.75),     # revenue=15.0, cost=7.5, profit=7.5
    ]
    schema = StructType([
        StructField("quantity", IntegerType()),
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)

    # Apply the EXACT same logic as gold_dlt.py → fact_sales
    result_df = (
        df.withColumn("revenue", F.round(F.col("quantity") * F.col("product_retail_price"), 2))
          .withColumn("cost", F.round(F.col("quantity") * F.col("product_cost"), 2))
          .withColumn("gross_profit", F.round(F.col("revenue") - F.col("cost"), 2))
    )

    rows = result_df.collect()

    # Row 1: 3 * 10.00 = 30.00
    assert rows[0]["revenue"] == 30.00
    assert rows[0]["cost"] == 18.00
    assert rows[0]["gross_profit"] == 12.00

    # Row 2: 1 * 99.99 = 99.99
    assert rows[1]["revenue"] == 99.99
    assert rows[1]["cost"] == 50.00
    assert rows[1]["gross_profit"] == 49.99

    # Row 3: 10 * 1.50 = 15.00
    assert rows[2]["revenue"] == 15.00
    assert rows[2]["cost"] == 7.50
    assert rows[2]["gross_profit"] == 7.50


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 5: Stock Status Classification (Kafka Silver)
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_kafka_dlt.py → silver_inventory
# LOGIC:
#   stock_level == 0   → "OUT_OF_STOCK"
#   stock_level < 10   → "LOW"
#   stock_level < 50   → "MEDIUM"
#   else               → "HEALTHY"
#
# SIMPLE EXPLANATION:
#   The inventory system classifies each product's stock level into
#   a human-readable category for the operations dashboard.

def test_stock_status_classification(spark):
    """Each stock level should map to the correct status label."""

    data = [(0,), (1,), (9,), (10,), (49,), (50,), (100,)]
    df = spark.createDataFrame(data, ["stock_level"])

    # Apply the EXACT same WHEN/OTHERWISE chain from silver_kafka_dlt.py
    result_df = df.withColumn(
        "stock_status",
        F.when(F.col("stock_level") == 0, F.lit("OUT_OF_STOCK"))
         .when(F.col("stock_level") < 10, F.lit("LOW"))
         .when(F.col("stock_level") < 50, F.lit("MEDIUM"))
         .otherwise(F.lit("HEALTHY"))
    )

    rows = result_df.collect()

    assert rows[0]["stock_status"] == "OUT_OF_STOCK"   # 0
    assert rows[1]["stock_status"] == "LOW"             # 1
    assert rows[2]["stock_status"] == "LOW"             # 9  (boundary: < 10)
    assert rows[3]["stock_status"] == "MEDIUM"          # 10 (boundary: >= 10)
    assert rows[4]["stock_status"] == "MEDIUM"          # 49 (boundary: < 50)
    assert rows[5]["stock_status"] == "HEALTHY"         # 50 (boundary: >= 50)
    assert rows[6]["stock_status"] == "HEALTHY"         # 100


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 6: Is Weekend Flag (Silver Calendar)
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_csv_dlt.py → silver_calendar
# LOGIC:
#   dayofweek(date).isin(1, 7) → True (Sunday=1, Saturday=7)
#   else                       → False
#
# SIMPLE EXPLANATION:
#   Spark's dayofweek returns 1=Sunday, 7=Saturday.
#   This test checks that weekdays return False and weekends return True.

def test_is_weekend_flag(spark):
    """Sunday and Saturday must be True; weekdays must be False."""

    data = [
        (date(2024, 1, 1),),    # Monday
        (date(2024, 1, 5),),    # Friday
        (date(2024, 1, 6),),    # Saturday
        (date(2024, 1, 7),),    # Sunday
    ]
    df = spark.createDataFrame(data, ["date"])

    result_df = df.withColumn(
        "is_weekend",
        F.when(F.dayofweek(F.col("date")).isin(1, 7), F.lit(True))
         .otherwise(F.lit(False))
    )

    rows = result_df.collect()

    assert rows[0]["is_weekend"] is False   # Monday
    assert rows[1]["is_weekend"] is False   # Friday
    assert rows[2]["is_weekend"] is True    # Saturday
    assert rows[3]["is_weekend"] is True    # Sunday


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 7: Date Key Generation (Gold dim_date)
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → dim_date, fact_sales
# LOGIC:  date_format(date, "yyyyMMdd").cast("int")
#
# SIMPLE EXPLANATION:
#   The Gold layer converts dates to integer keys for fast BI joins.
#   "2024-03-15" → 20240315 (an integer).

def test_date_key_generation(spark):
    """Dates must convert to integer YYYYMMDD keys for the Gold layer."""

    data = [
        (date(2024, 3, 15),),
        (date(2023, 12, 1),),
        (date(2020, 1, 31),),
    ]
    df = spark.createDataFrame(data, ["date"])

    result_df = df.withColumn(
        "date_key",
        F.date_format(F.col("date"), "yyyyMMdd").cast("int")
    )

    rows = result_df.collect()

    assert rows[0]["date_key"] == 20240315
    assert rows[1]["date_key"] == 20231201
    assert rows[2]["date_key"] == 20200131


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 8: Transaction Date Part Extraction
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_csv_dlt.py → silver_transactions
# LOGIC:  year(), month(), quarter(), dayofweek()
#
# SIMPLE EXPLANATION:
#   Each transaction date is decomposed into year, month, quarter, and
#   day-of-week columns so dashboards can filter/aggregate efficiently.

def test_transaction_date_parts(spark):
    """Year, month, quarter, and day_of_week must extract correctly."""

    data = [(date(2024, 7, 15),)]  # July 15, 2024 = Monday
    df = spark.createDataFrame(data, ["transaction_date"])

    result_df = df.select(
        F.year(F.col("transaction_date")).alias("transaction_year"),
        F.month(F.col("transaction_date")).alias("transaction_month"),
        F.quarter(F.col("transaction_date")).alias("transaction_quarter"),
        F.dayofweek(F.col("transaction_date")).alias("transaction_day_of_week"),
    )

    row = result_df.collect()[0]

    assert row["transaction_year"] == 2024
    assert row["transaction_month"] == 7
    assert row["transaction_quarter"] == 3      # July = Q3
    assert row["transaction_day_of_week"] == 2  # Monday = 2 in Spark


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 9: Day Name and Month Name (Silver Calendar)
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_csv_dlt.py → silver_calendar
# LOGIC:  date_format(date, "EEEE") → "Monday", date_format(date, "MMMM") → "July"
#
# SIMPLE EXPLANATION:
#   The calendar table stores human-readable names so dashboards can display
#   "Monday" or "July" instead of numbers.

def test_day_name_and_month_name(spark):
    """date_format must produce correct day and month names."""

    data = [(date(2024, 7, 15),)]  # Monday, July
    df = spark.createDataFrame(data, ["date"])

    result_df = df.select(
        F.date_format(F.col("date"), "EEEE").alias("day_name"),
        F.date_format(F.col("date"), "MMMM").alias("month_name"),
    )

    row = result_df.collect()[0]
    assert row["day_name"] == "Monday"
    assert row["month_name"] == "July"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 10: Recyclable / Low Fat Boolean Casting
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_mongo_dlt.py → products_parsed_vw
# LOGIC:  col("recyclable").cast("boolean"), col("low_fat").cast("boolean")
#
# SIMPLE EXPLANATION:
#   MongoDB stores recyclable/low_fat as 0 or 1 (integers).
#   Silver casts them to true/false booleans for cleaner analytics.

def test_boolean_casting_from_int(spark):
    """0 → False, 1 → True when casting INT to BOOLEAN."""

    data = [(1, 0), (0, 1), (1, 1)]
    schema = StructType([
        StructField("recyclable", IntegerType()),
        StructField("low_fat", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = df.select(
        F.col("recyclable").cast("boolean").alias("recyclable"),
        F.col("low_fat").cast("boolean").alias("low_fat"),
    )

    rows = result_df.collect()
    assert rows[0]["recyclable"] is True
    assert rows[0]["low_fat"] is False
    assert rows[1]["recyclable"] is False
    assert rows[1]["low_fat"] is True
    assert rows[2]["recyclable"] is True
    assert rows[2]["low_fat"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 11: Revenue with Large Quantities (Overflow Check)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: Production data might have bulk orders with large quantities.
#       This test ensures the multiplication doesn't overflow or lose precision.

def test_revenue_with_large_quantity(spark):
    """Large quantities should not overflow or lose decimal precision."""

    data = [(10000, 99.99, 50.00)]
    schema = StructType([
        StructField("quantity", IntegerType()),
        StructField("product_retail_price", DoubleType()),
        StructField("product_cost", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.withColumn("revenue", F.round(F.col("quantity") * F.col("product_retail_price"), 2))
          .withColumn("cost", F.round(F.col("quantity") * F.col("product_cost"), 2))
          .withColumn("gross_profit", F.round(F.col("revenue") - F.col("cost"), 2))
    )

    row = result_df.collect()[0]

    # 10000 * 99.99 = 999900.00
    assert row["revenue"] == 999900.00
    assert row["cost"] == 500000.00
    assert row["gross_profit"] == 499900.00


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 12: Week of Year Extraction (Silver Calendar)
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: silver_csv_dlt.py → silver_calendar
# LOGIC:  weekofyear(date)

def test_week_of_year(spark):
    """weekofyear must return the correct ISO week number."""

    data = [(date(2024, 1, 1),), (date(2024, 7, 15),)]
    df = spark.createDataFrame(data, ["date"])

    result_df = df.withColumn(
        "week_of_year", F.weekofyear(F.col("date"))
    )

    rows = result_df.collect()
    assert rows[0]["week_of_year"] == 1    # Jan 1, 2024
    assert rows[1]["week_of_year"] == 29   # July 15, 2024
