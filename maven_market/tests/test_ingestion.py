"""
test_ingestion.py — Unit tests for the Bronze → Silver ingestion layer.

PURPOSE:
    These tests verify that raw data (JSON strings from MongoDB, CSV rows)
    is correctly parsed, cast, and enriched BEFORE it lands in the Silver tables.
    We recreate the exact same logic used in silver_mongo_dlt.py and silver_csv_dlt.py,
    but test it on small, in-memory DataFrames — no live tables needed.

WHAT THIS FILE COVERS:
    1. JSON Schema Parsing   – Can we parse a MongoDB JSON string into typed columns?
    2. Full Schema Validation – Does every field in the 21-column customer schema parse?
    3. Product Schema Parsing – Does the 9-column product schema parse correctly?
    4. Malformed JSON Handling – What happens when JSON is broken or missing fields?
    5. Date Parsing           – Does the "M/d/yyyy" format convert to proper DATE types?
    6. Type Casting           – Are INT, DOUBLE, LONG, BOOLEAN casts applied correctly?

HOW TO RUN:
    From the repo root:  pytest tests/test_ingestion.py -v
"""

import os
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ── Skip marker for Databricks-only tests ────────────────────────────────────
# These tests rely on ANSI strict mode (on by default in Databricks) and
# Databricks-only SQL functions (try_to_date, try_cast) that don't exist in
# open-source PySpark 3.5.x.  They pass on Databricks but must be skipped in CI.
_ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
databricks_only = pytest.mark.skipif(
    not _ON_DATABRICKS,
    reason="Requires Databricks runtime (ANSI mode / try_* functions)"
)


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMAS — Copied exactly from silver_mongo_dlt.py so tests stay in sync
# ═══════════════════════════════════════════════════════════════════════════════

CUSTOMERS_JSON_SCHEMA = """
    STRUCT<
        customer_id:             INT,
        customer_acct_num:       STRING,
        first_name:              STRING,
        last_name:               STRING,
        email_address:           STRING,
        customer_address:        STRING,
        customer_city:           STRING,
        customer_state_province: STRING,
        customer_postal_code:    STRING,
        customer_country:        STRING,
        birthdate:               STRING,
        marital_status:          STRING,
        yearly_income:           STRING,
        gender:                  STRING,
        total_children:          INT,
        num_children_at_home:    INT,
        education:               STRING,
        acct_open_date:          STRING,
        member_card:             STRING,
        occupation:              STRING,
        homeowner:               STRING
    >
"""

PRODUCTS_JSON_SCHEMA = """
    STRUCT<
        product_id:           INT,
        product_brand:        STRING,
        product_name:         STRING,
        product_sku:          LONG,
        product_retail_price: DOUBLE,
        product_cost:         DOUBLE,
        product_weight:       DOUBLE,
        recyclable:           INT,
        low_fat:              INT
    >
"""


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1: Basic Customer JSON Parsing (3 key fields)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: This is the simplest "smoke test." If from_json can't parse customer_id,
#       email, and gender, nothing downstream will work.

def test_customer_json_parsing_basic(spark):
    """Parse a minimal JSON string and check the 3 most important fields."""

    # Simulate a single row from bronze_customers with a 'data' column
    raw_data = [
        ('{"customer_id": 1, "email_address": "test@test.com", "gender": "M"}',)
    ]
    df = spark.createDataFrame(raw_data, ["data"])

    # Apply the same from_json logic as silver_mongo_dlt.py
    parsed_df = (
        df.withColumn("_parsed", F.from_json(F.col("data"), CUSTOMERS_JSON_SCHEMA))
          .select("_parsed.*")
    )

    row = parsed_df.collect()[0]

    # customer_id should be parsed as an integer, not a string
    assert row["customer_id"] == 1
    # email_address should survive the parse as-is
    assert row["email_address"] == "test@test.com"
    # gender must be exactly "M" (used in DLT expect: gender IN ('M','F'))
    assert row["gender"] == "M"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2: Full Customer Schema (all 21 fields)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: The Silver layer selects ALL 21 fields from the parsed struct.
#       If any field name has a typo or wrong type, the pipeline silently
#       returns NULLs. This test catches that.

def test_customer_full_schema_parsing(spark):
    """Parse a complete customer JSON and verify every single field."""

    full_json = """{
        "customer_id": 100,
        "customer_acct_num": "CA-100",
        "first_name": "Jane",
        "last_name": "Doe",
        "email_address": "jane@example.com",
        "customer_address": "123 Main St",
        "customer_city": "Portland",
        "customer_state_province": "Oregon",
        "customer_postal_code": "97201",
        "customer_country": "USA",
        "birthdate": "3/15/1990",
        "marital_status": "S",
        "yearly_income": "$70K - $90K",
        "gender": "F",
        "total_children": 2,
        "num_children_at_home": 1,
        "education": "Bachelors",
        "acct_open_date": "1/10/2015",
        "member_card": "Gold",
        "occupation": "Professional",
        "homeowner": "Y"
    }"""

    df = spark.createDataFrame([(full_json,)], ["data"])
    parsed_df = (
        df.withColumn("_parsed", F.from_json(F.col("data"), CUSTOMERS_JSON_SCHEMA))
          .select("_parsed.*")
    )

    row = parsed_df.collect()[0]

    # Verify every field parsed correctly — if any returns None, the schema is wrong
    assert row["customer_id"] == 100
    assert row["customer_acct_num"] == "CA-100"
    assert row["first_name"] == "Jane"
    assert row["last_name"] == "Doe"
    assert row["email_address"] == "jane@example.com"
    assert row["customer_address"] == "123 Main St"
    assert row["customer_city"] == "Portland"
    assert row["customer_state_province"] == "Oregon"
    assert row["customer_postal_code"] == "97201"
    assert row["customer_country"] == "USA"
    assert row["birthdate"] == "3/15/1990"           # still a string at this point
    assert row["marital_status"] == "S"
    assert row["yearly_income"] == "$70K - $90K"
    assert row["gender"] == "F"
    assert row["total_children"] == 2                 # should be INT, not string
    assert row["num_children_at_home"] == 1
    assert row["education"] == "Bachelors"
    assert row["acct_open_date"] == "1/10/2015"       # still a string at this point
    assert row["member_card"] == "Gold"
    assert row["occupation"] == "Professional"
    assert row["homeowner"] == "Y"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3: Product JSON Schema Parsing
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: Products come from MongoDB too. The product schema has DOUBLE and LONG
#       types that must parse correctly for margin_pct to be calculated later.

def test_product_json_parsing(spark):
    """Parse a full product JSON and verify numeric types are correct."""

    product_json = """{
        "product_id": 50,
        "product_brand": "Hermanos",
        "product_name": "Hermanos Green Pepper",
        "product_sku": 1234567890,
        "product_retail_price": 3.49,
        "product_cost": 1.40,
        "product_weight": 12.5,
        "recyclable": 1,
        "low_fat": 0
    }"""

    df = spark.createDataFrame([(product_json,)], ["data"])
    parsed_df = (
        df.withColumn("_parsed", F.from_json(F.col("data"), PRODUCTS_JSON_SCHEMA))
          .select("_parsed.*")
    )

    row = parsed_df.collect()[0]

    assert row["product_id"] == 50                      # INT
    assert row["product_brand"] == "Hermanos"            # STRING
    assert row["product_name"] == "Hermanos Green Pepper"
    assert row["product_sku"] == 1234567890              # LONG
    assert abs(row["product_retail_price"] - 3.49) < 0.01   # DOUBLE — use tolerance
    assert abs(row["product_cost"] - 1.40) < 0.01
    assert abs(row["product_weight"] - 12.5) < 0.01
    assert row["recyclable"] == 1                        # INT (cast to boolean in Silver)
    assert row["low_fat"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4: Malformed JSON Returns NULLs (doesn't crash)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: MongoDB can send corrupted data. from_json should return a row of NULLs,
#       NOT crash the entire pipeline. DLT expectations then drop NULL rows.

def test_malformed_json_returns_nulls(spark):
    """Broken JSON should not crash — all fields become NULL."""

    # Not valid JSON — missing closing brace
    bad_data = [('{"customer_id": 1, "email_address": ',)]
    df = spark.createDataFrame(bad_data, ["data"])

    parsed_df = (
        df.withColumn("_parsed", F.from_json(F.col("data"), CUSTOMERS_JSON_SCHEMA))
          .select("_parsed.*")
    )

    row = parsed_df.collect()[0]

    # All fields should be NULL because the entire JSON failed to parse
    assert row["customer_id"] is None
    assert row["email_address"] is None


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 5: Missing JSON Fields Default to NULL
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: If MongoDB sends a customer with only customer_id and nothing else,
#       the schema should still parse the row — missing fields become NULL.
#       This is normal and expected; DLT expectations handle downstream filtering.

def test_partial_json_fills_nulls(spark):
    """JSON with only some fields should parse — missing ones become NULL."""

    # Only customer_id is present; everything else is missing
    partial_json = [('{"customer_id": 42}',)]
    df = spark.createDataFrame(partial_json, ["data"])

    parsed_df = (
        df.withColumn("_parsed", F.from_json(F.col("data"), CUSTOMERS_JSON_SCHEMA))
          .select("_parsed.*")
    )

    row = parsed_df.collect()[0]

    # customer_id should parse fine
    assert row["customer_id"] == 42
    # All other fields should be NULL (not empty string, not zero)
    assert row["email_address"] is None
    assert row["first_name"] is None
    assert row["gender"] is None
    assert row["total_children"] is None


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 6: Date Parsing with "M/d/yyyy" Format
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: silver_mongo_dlt.py and silver_csv_dlt.py both use:
#          to_date(col("birthdate"), "M/d/yyyy")
#       This test verifies common date variations all parse correctly:
#       - "1/5/1990"  → single-digit month and day
#       - "12/25/2000" → double-digit month and day

def test_date_parsing_m_d_yyyy(spark):
    """The M/d/yyyy format must handle single and double digit months/days."""

    data = [
        ("1/5/1990",),     # single-digit month AND day
        ("12/25/2000",),   # double-digit month AND day
        ("3/15/1985",),    # single month, double day
    ]
    df = spark.createDataFrame(data, ["raw_date"])

    result_df = df.withColumn(
        "parsed_date", F.to_date(F.col("raw_date"), "M/d/yyyy")
    )

    rows = result_df.collect()

    # to_date returns a datetime.date object — check year, month, day
    assert str(rows[0]["parsed_date"]) == "1990-01-05"
    assert str(rows[1]["parsed_date"]) == "2000-12-25"
    assert str(rows[2]["parsed_date"]) == "1985-03-15"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 7: Invalid Date — ANSI Mode Behavior
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: Databricks has ANSI mode ON by default. This means:
#       - to_date("not-a-date") will THROW an error, not return NULL.
#       - This is INTENTIONAL — it forces you to catch bad data early.
#       - For fault-tolerant parsing, use try_to_date() instead.
#
# SIMPLE EXPLANATION:
#   This test proves TWO things:
#   1. to_date CRASHES on garbage input (Databricks strict mode)
#   2. try_to_date safely returns NULL — this is the safer alternative
#
# NOTE: Your pipeline uses to_date(), which is fine because DLT expectations
#       (expect_or_drop) filter out rows with NULL/invalid dates BEFORE
#       to_date is applied. If bad data ever bypasses expectations, the
#       pipeline would fail loudly — which is the CORRECT behavior in production.

@databricks_only
def test_invalid_date_strict_mode_raises_error(spark):
    """to_date THROWS on invalid input because Databricks ANSI mode is ON."""

    data = [("not-a-date",)]
    df = spark.createDataFrame(data, ["raw_date"])

    result_df = df.withColumn(
        "parsed_date", F.to_date(F.col("raw_date"), "M/d/yyyy")
    )

    # Databricks ANSI mode: to_date raises an error on unparseable strings
    with pytest.raises(Exception):
        result_df.collect()


@databricks_only
def test_try_to_date_returns_null_for_invalid_input(spark):
    """try_to_date safely returns NULL for garbage dates (fault-tolerant)."""

    data = [("not-a-date",), ("",), ("13/32/2020",)]
    df = spark.createDataFrame(data, ["raw_date"])

    # try_to_date is a SQL-only function — must use F.expr() to call it
    result_df = df.withColumn(
        "parsed_date", F.expr("try_to_date(raw_date, 'M/d/yyyy')")
    )

    rows = result_df.collect()

    # All three should be NULL — none of these are valid M/d/yyyy dates
    assert rows[0]["parsed_date"] is None
    assert rows[1]["parsed_date"] is None
    assert rows[2]["parsed_date"] is None


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 8: Type Casting — Valid Values (INT, DOUBLE, LONG, BOOLEAN)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: Silver layers cast columns like:
#       - col("store_id").cast("int")
#       - col("product_retail_price").cast("double")
#       - col("recyclable").cast("boolean")
#       This test confirms valid strings cast correctly.

def test_type_casting_valid_values(spark):
    """Valid string values should cast to the correct numeric types."""

    data = [("42", "3.49", "1234567890", "1")]
    df = spark.createDataFrame(data, ["int_val", "double_val", "long_val", "bool_val"])

    result_df = df.select(
        F.col("int_val").cast("int").alias("as_int"),
        F.col("double_val").cast("double").alias("as_double"),
        F.col("long_val").cast("long").alias("as_long"),
        F.col("bool_val").cast("boolean").alias("as_bool"),
    )

    row = result_df.collect()[0]

    assert row["as_int"] == 42
    assert abs(row["as_double"] - 3.49) < 0.01
    assert row["as_long"] == 1234567890
    assert row["as_bool"] is True    # "1" → True


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 8b: Type Casting — Invalid Values (ANSI Mode)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: Databricks ANSI mode makes .cast() THROW on invalid strings like "abc".
#       This is intentional — bad data should fail loudly, not silently become NULL.
#       Use try_cast() for fault-tolerant behavior.
#
# SIMPLE EXPLANATION:
#   Trying to convert "abc" to a number is an error. Databricks catches this
#   immediately instead of hiding it as NULL. This protects data quality.

@databricks_only
def test_type_casting_invalid_values_strict_mode(spark):
    """.cast() THROWS on invalid values in Databricks ANSI mode."""

    data = [("abc",)]
    df = spark.createDataFrame(data, ["int_val"])

    result_df = df.select(
        F.col("int_val").cast("int").alias("as_int"),
    )

    # ANSI mode: casting "abc" to int raises an error
    with pytest.raises(Exception):
        result_df.collect()


@databricks_only
def test_try_cast_returns_null_for_invalid_values(spark):
    """try_cast safely returns NULL for non-castable values (fault-tolerant)."""

    data = [("abc", "xyz", "not_a_number")]
    df = spark.createDataFrame(data, ["int_val", "double_val", "long_val"])

    # try_cast is a SQL-only function — must use F.expr() to call it
    result_df = df.select(
        F.expr("try_cast(int_val AS int)").alias("as_int"),
        F.expr("try_cast(double_val AS double)").alias("as_double"),
        F.expr("try_cast(long_val AS long)").alias("as_long"),
    )

    row = result_df.collect()[0]

    # All casts should return NULL gracefully
    assert row["as_int"] is None
    assert row["as_double"] is None
    assert row["as_long"] is None


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 9: Multiple Customer Rows (Batch Parsing)
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: Production Bronze tables have thousands of rows. This test verifies
#       that from_json works correctly across multiple rows in one DataFrame,
#       not just a single-row edge case.

def test_batch_customer_parsing(spark):
    """Parsing should work correctly across multiple rows, not just one."""

    raw_data = [
        ('{"customer_id": 1, "first_name": "Alice", "gender": "F"}',),
        ('{"customer_id": 2, "first_name": "Bob", "gender": "M"}',),
        ('{"customer_id": 3, "first_name": "Charlie", "gender": "M"}',),
    ]
    df = spark.createDataFrame(raw_data, ["data"])

    parsed_df = (
        df.withColumn("_parsed", F.from_json(F.col("data"), CUSTOMERS_JSON_SCHEMA))
          .select("_parsed.*")
    )

    rows = parsed_df.orderBy("customer_id").collect()

    assert len(rows) == 3
    assert rows[0]["customer_id"] == 1
    assert rows[0]["first_name"] == "Alice"
    assert rows[1]["customer_id"] == 2
    assert rows[1]["first_name"] == "Bob"
    assert rows[2]["customer_id"] == 3
    assert rows[2]["gender"] == "M"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 10: Empty String JSON Field vs NULL
# ═══════════════════════════════════════════════════════════════════════════════
# WHY: MongoDB sometimes stores "" (empty string) instead of null.
#       This test confirms that from_json preserves the distinction —
#       an empty string is NOT the same as NULL, and your DLT expectations
#       need to handle both cases.

def test_empty_string_vs_null_in_json(spark):
    """Empty string in JSON is different from a missing field (NULL)."""

    # email_address is "" (empty), gender is missing entirely
    json_str = '{"customer_id": 10, "email_address": ""}'
    df = spark.createDataFrame([(json_str,)], ["data"])

    parsed_df = (
        df.withColumn("_parsed", F.from_json(F.col("data"), CUSTOMERS_JSON_SCHEMA))
          .select("_parsed.*")
    )

    row = parsed_df.collect()[0]

    # Empty string is preserved — it is NOT null
    assert row["email_address"] == ""
    # Missing field becomes NULL
    assert row["gender"] is None
