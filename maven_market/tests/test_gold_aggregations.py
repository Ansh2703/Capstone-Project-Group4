"""
test_gold_aggregations.py — Unit tests for Gold aggregation table logic.

PURPOSE:
    These tests verify the GROUP BY + aggregation logic used in the Gold
    aggregation tables. Each test builds small in-memory DataFrames that
    simulate the output of fact/dimension tables, then applies the exact
    same aggregation formulas from gold_dlt.py.

WHAT THIS FILE COVERS:
    1.  agg_executive_overview    – total_revenue, total_profit, profit_margin_pct by year/month
    2.  agg_regional_sales        – total_store_revenue, total_items_sold by region/store
    3.  agg_customer_ltv          – lifetime_revenue, lifetime_profit, total_items, avg_item_value
    4.  agg_ops_inventory_alerts  – filter OUT_OF_STOCK/LOW, max alert time, min stock level
    5.  agg_ops_orders_per_minute – orders_per_minute, total_quantity by store/minute
    6.  agg_store_space_utilization – revenue_per_sqft, profit_per_sqft by store

HOW TO RUN:
    From the repo root:  pytest tests/test_gold_aggregations.py -v
"""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType,
    TimestampType
)
from datetime import datetime, date


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1: agg_executive_overview — Revenue & Profit Margin by Year/Month
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → agg_executive_overview
# LOGIC:
#   groupBy(transaction_year, transaction_month)
#   .agg(sum(revenue), sum(gross_profit),
#        round(sum(gross_profit) / sum(revenue) * 100, 2))

def test_executive_overview_aggregation(spark):
    """Monthly revenue, profit, and margin % must aggregate correctly."""

    data = [
        (2024, 1, 500.0, 150.0),
        (2024, 1, 300.0, 100.0),
        (2024, 2, 200.0, 80.0),
    ]
    schema = StructType([
        StructField("transaction_year", IntegerType()),
        StructField("transaction_month", IntegerType()),
        StructField("revenue", DoubleType()),
        StructField("gross_profit", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.groupBy("transaction_year", "transaction_month")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("gross_profit").alias("total_profit"),
            F.round((F.sum("gross_profit") / F.sum("revenue")) * 100, 2).alias("profit_margin_pct")
        )
        .orderBy("transaction_month")
    )

    rows = result_df.collect()

    # Jan 2024: revenue = 500+300 = 800, profit = 150+100 = 250, margin = 31.25%
    assert rows[0]["total_revenue"] == 800.0
    assert rows[0]["total_profit"] == 250.0
    assert rows[0]["profit_margin_pct"] == 31.25

    # Feb 2024: revenue = 200, profit = 80, margin = 40%
    assert rows[1]["total_revenue"] == 200.0
    assert rows[1]["total_profit"] == 80.0
    assert rows[1]["profit_margin_pct"] == 40.0


def test_executive_overview_single_transaction_month(spark):
    """A month with a single transaction should still calculate margin correctly."""

    data = [(2024, 6, 99.99, 33.33)]
    schema = StructType([
        StructField("transaction_year", IntegerType()),
        StructField("transaction_month", IntegerType()),
        StructField("revenue", DoubleType()),
        StructField("gross_profit", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.groupBy("transaction_year", "transaction_month")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("gross_profit").alias("total_profit"),
            F.round((F.sum("gross_profit") / F.sum("revenue")) * 100, 2).alias("profit_margin_pct")
        )
    )

    row = result_df.collect()[0]
    assert row["total_revenue"] == 99.99
    assert row["profit_margin_pct"] == 33.33


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2: agg_regional_sales — Revenue & Items Sold by Region/Store
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → agg_regional_sales
# LOGIC:
#   fact_sales.join(dim_store, on="store_id")
#   .groupBy(year, month, region_id, sales_region, store_id, store_name)
#   .agg(sum(revenue), sum(quantity))

def test_regional_sales_aggregation(spark):
    """Revenue and items sold must aggregate per store/region correctly."""

    data = [
        (2024, 1, 10, "North America", 1, "Store A", 100.0, 3),
        (2024, 1, 10, "North America", 1, "Store A", 200.0, 5),
        (2024, 1, 20, "Europe",        2, "Store B", 150.0, 2),
    ]
    schema = StructType([
        StructField("transaction_year", IntegerType()),
        StructField("transaction_month", IntegerType()),
        StructField("region_id", IntegerType()),
        StructField("sales_region", StringType()),
        StructField("store_id", IntegerType()),
        StructField("store_name", StringType()),
        StructField("revenue", DoubleType()),
        StructField("quantity", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.groupBy("transaction_year", "transaction_month", "region_id",
                    "sales_region", "store_id", "store_name")
        .agg(
            F.sum("revenue").alias("total_store_revenue"),
            F.sum("quantity").alias("total_items_sold")
        )
        .orderBy("store_id")
    )

    rows = result_df.collect()

    # Store A (North America): 100+200 = 300 revenue, 3+5 = 8 items
    assert rows[0]["total_store_revenue"] == 300.0
    assert rows[0]["total_items_sold"] == 8
    assert rows[0]["sales_region"] == "North America"

    # Store B (Europe): 150 revenue, 2 items
    assert rows[1]["total_store_revenue"] == 150.0
    assert rows[1]["total_items_sold"] == 2


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3: agg_customer_ltv — Lifetime Value per Customer
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → agg_customer_ltv
# LOGIC:
#   fact_sales.join(dim_customer, on="customer_id")
#   .groupBy(customer_id, first_name, last_name, country, income, member_card)
#   .agg(sum(revenue), sum(gross_profit), count(date_key),
#        round(sum(revenue) / count(date_key), 2))

def test_customer_ltv_aggregation(spark):
    """Customer LTV must correctly sum revenue/profit and compute avg item value."""

    data = [
        (1, "Alice", "Smith", "USA", "$70K", "Gold", 50.0, 20.0, 20240101),
        (1, "Alice", "Smith", "USA", "$70K", "Gold", 30.0, 10.0, 20240102),
        (2, "Bob",   "Jones", "Canada", "$50K", "Silver", 100.0, 40.0, 20240101),
    ]
    schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("customer_country", StringType()),
        StructField("yearly_income", StringType()),
        StructField("member_card", StringType()),
        StructField("revenue", DoubleType()),
        StructField("gross_profit", DoubleType()),
        StructField("date_key", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.groupBy(
            "customer_id", "first_name", "last_name",
            "customer_country", "yearly_income", "member_card"
        )
        .agg(
            F.sum("revenue").alias("lifetime_revenue"),
            F.sum("gross_profit").alias("lifetime_profit"),
            F.count("date_key").alias("total_items_purchased"),
            F.round(F.sum("revenue") / F.count("date_key"), 2).alias("avg_item_value")
        )
        .orderBy("customer_id")
    )

    rows = result_df.collect()

    # Alice: revenue = 50+30 = 80, profit = 20+10 = 30, items = 2, avg = 40.0
    assert rows[0]["lifetime_revenue"] == 80.0
    assert rows[0]["lifetime_profit"] == 30.0
    assert rows[0]["total_items_purchased"] == 2
    assert rows[0]["avg_item_value"] == 40.0

    # Bob: revenue = 100, profit = 40, items = 1, avg = 100.0
    assert rows[1]["lifetime_revenue"] == 100.0
    assert rows[1]["total_items_purchased"] == 1
    assert rows[1]["avg_item_value"] == 100.0


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4: agg_ops_inventory_alerts — Filter + Aggregate Low Stock
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → agg_ops_inventory_alerts
# LOGIC:
#   inventory.filter(stock_status IN ("OUT_OF_STOCK", "LOW"))
#   .groupBy(store_id, product_id, stock_status)
#   .agg(max(event_timestamp), min(stock_level))

def test_inventory_alerts_filter_and_aggregate(spark):
    """Only OUT_OF_STOCK and LOW rows should be aggregated for alerts."""

    data = [
        (1, 100, "OUT_OF_STOCK", 0,  datetime(2024, 1, 1, 10, 0)),
        (1, 100, "OUT_OF_STOCK", 0,  datetime(2024, 1, 1, 14, 0)),
        (1, 200, "LOW",          5,  datetime(2024, 1, 1, 9, 0)),
        (2, 300, "HEALTHY",      80, datetime(2024, 1, 1, 12, 0)),
        (2, 400, "MEDIUM",       30, datetime(2024, 1, 1, 11, 0)),
    ]
    schema = StructType([
        StructField("store_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("stock_status", StringType()),
        StructField("stock_level", IntegerType()),
        StructField("event_timestamp", TimestampType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.filter(F.col("stock_status").isin("OUT_OF_STOCK", "LOW"))
        .groupBy("store_id", "product_id", "stock_status")
        .agg(
            F.max("event_timestamp").alias("last_alert_time"),
            F.min("stock_level").alias("lowest_stock_level")
        )
        .orderBy("store_id", "product_id")
    )

    rows = result_df.collect()

    assert len(rows) == 2

    assert rows[0]["store_id"] == 1
    assert rows[0]["product_id"] == 100
    assert rows[0]["last_alert_time"] == datetime(2024, 1, 1, 14, 0)
    assert rows[0]["lowest_stock_level"] == 0

    assert rows[1]["product_id"] == 200
    assert rows[1]["lowest_stock_level"] == 5


def test_inventory_alerts_no_alerts_returns_empty(spark):
    """If all stock is HEALTHY/MEDIUM, the result should be empty."""

    data = [
        (1, 100, "HEALTHY", 80, datetime(2024, 1, 1, 10, 0)),
        (2, 200, "MEDIUM",  30, datetime(2024, 1, 1, 11, 0)),
    ]
    schema = StructType([
        StructField("store_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("stock_status", StringType()),
        StructField("stock_level", IntegerType()),
        StructField("event_timestamp", TimestampType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.filter(F.col("stock_status").isin("OUT_OF_STOCK", "LOW"))
        .groupBy("store_id", "product_id", "stock_status")
        .agg(
            F.max("event_timestamp").alias("last_alert_time"),
            F.min("stock_level").alias("lowest_stock_level")
        )
    )

    assert result_df.count() == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 5: agg_ops_orders_per_minute — Order Throughput
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → agg_ops_orders_per_minute
# LOGIC:
#   orders.withColumn("order_minute", date_trunc("minute", event_timestamp))
#   .groupBy(store_id, order_minute)
#   .agg(count(order_id), sum(quantity))

def test_orders_per_minute_aggregation(spark):
    """Orders in the same minute/store must be counted together."""

    data = [
        (1, 1, datetime(2024, 1, 1, 10, 30, 15), 2),
        (2, 1, datetime(2024, 1, 1, 10, 30, 45), 3),
        (3, 1, datetime(2024, 1, 1, 10, 31, 10), 1),
        (4, 2, datetime(2024, 1, 1, 10, 30, 20), 5),
    ]
    schema = StructType([
        StructField("order_id", IntegerType()),
        StructField("store_id", IntegerType()),
        StructField("event_timestamp", TimestampType()),
        StructField("quantity", IntegerType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.withColumn("order_minute", F.date_trunc("minute", F.col("event_timestamp")))
        .groupBy("store_id", "order_minute")
        .agg(
            F.count("order_id").alias("orders_per_minute"),
            F.sum("quantity").alias("total_quantity_per_minute")
        )
        .orderBy("store_id", "order_minute")
    )

    rows = result_df.collect()

    assert rows[0]["store_id"] == 1
    assert rows[0]["orders_per_minute"] == 2
    assert rows[0]["total_quantity_per_minute"] == 5

    assert rows[1]["orders_per_minute"] == 1
    assert rows[1]["total_quantity_per_minute"] == 1

    assert rows[2]["store_id"] == 2
    assert rows[2]["orders_per_minute"] == 1
    assert rows[2]["total_quantity_per_minute"] == 5


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 6: agg_store_space_utilization — Revenue per Square Foot
# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE: gold_dlt.py → agg_store_space_utilization
# LOGIC:
#   fact_sales.join(dim_store, on="store_id")
#   .groupBy(store_id, store_name, store_type, total_sqft, grocery_sqft)
#   .agg(sum(revenue), sum(gross_profit),
#        round(sum(revenue) / total_sqft, 2), round(sum(profit) / total_sqft, 2))

def test_store_space_utilization(spark):
    """Revenue and profit per square foot must be calculated correctly."""

    data = [
        (1, "Downtown", "Supermarket", 10000, 6000, 500.0, 200.0),
        (1, "Downtown", "Supermarket", 10000, 6000, 300.0, 100.0),
        (2, "Mall",     "Deluxe",      5000,  3000, 400.0, 150.0),
    ]
    schema = StructType([
        StructField("store_id", IntegerType()),
        StructField("store_name", StringType()),
        StructField("store_type", StringType()),
        StructField("total_sqft", IntegerType()),
        StructField("grocery_sqft", IntegerType()),
        StructField("revenue", DoubleType()),
        StructField("gross_profit", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.groupBy("store_id", "store_name", "store_type", "total_sqft", "grocery_sqft")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("gross_profit").alias("total_profit"),
            F.round(F.sum("revenue") / F.col("total_sqft"), 2).alias("revenue_per_sqft"),
            F.round(F.sum("gross_profit") / F.col("total_sqft"), 2).alias("profit_per_sqft")
        )
        .orderBy("store_id")
    )

    rows = result_df.collect()

    assert rows[0]["total_revenue"] == 800.0
    assert rows[0]["total_profit"] == 300.0
    assert rows[0]["revenue_per_sqft"] == 0.08
    assert rows[0]["profit_per_sqft"] == 0.03

    assert rows[1]["total_revenue"] == 400.0
    assert rows[1]["revenue_per_sqft"] == 0.08
    assert rows[1]["profit_per_sqft"] == 0.03


def test_store_space_utilization_high_density(spark):
    """A small store with high revenue should show high revenue_per_sqft."""

    data = [(1, "Kiosk", "Small", 100, 50, 10000.0, 5000.0)]
    schema = StructType([
        StructField("store_id", IntegerType()),
        StructField("store_name", StringType()),
        StructField("store_type", StringType()),
        StructField("total_sqft", IntegerType()),
        StructField("grocery_sqft", IntegerType()),
        StructField("revenue", DoubleType()),
        StructField("gross_profit", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = (
        df.groupBy("store_id", "store_name", "store_type", "total_sqft", "grocery_sqft")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("gross_profit").alias("total_profit"),
            F.round(F.sum("revenue") / F.col("total_sqft"), 2).alias("revenue_per_sqft"),
            F.round(F.sum("gross_profit") / F.col("total_sqft"), 2).alias("profit_per_sqft")
        )
    )

    row = result_df.collect()[0]
    assert row["revenue_per_sqft"] == 100.0
    assert row["profit_per_sqft"] == 50.0
