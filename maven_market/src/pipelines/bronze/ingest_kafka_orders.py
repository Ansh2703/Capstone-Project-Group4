# ================================
# KAFKA → DATABRICKS BRONZE INGESTION (ORDERS)
# ================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

# ================================
# CONFIG (YOUR DETAILS)
# ================================

BOOTSTRAP_SERVER = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
TOPIC = "orders_topic"

API_KEY = "OWQL53ZQ5HIRVEIX"
API_SECRET = "cflt9GTSkONEgeiTaVr887GsEoXc36+3dquLtF/XbuTQr3gSNYFMhneZsrGN7VjA"

# FIXED: Removed space in .dfs.core
CHECKPOINT_PATH = "abfss://maven-market-data@mavengrp4.dfs.core.windows.net/checkpoints/kafka/orders/"
TABLE_NAME = "maven_market_uc.bronze.orders_kafka"

# ================================
# INIT SPARK
# ================================
spark = SparkSession.builder.getOrCreate()

print("🚀 Starting Kafka Orders ingestion...")

# ================================
# READ FROM KAFKA
# ================================
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
    .option("subscribe", TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'
    )
    .option("startingOffsets", "earliest")
    .load()
)

# ================================
# DEFINE SCHEMA (YOUR SCHEMA)
# ================================
schema = StructType([
    StructField("event_time", StringType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("store_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("quantity", IntegerType())
])

# ================================
# PARSE JSON
# ================================
df_parsed = (
    df_kafka.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# ================================
# WRITE TO DELTA TABLE
# ================================
query = (
    df_parsed.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True) # Change to .trigger(processingTime='10 seconds') for continuous
    .toTable(TABLE_NAME)
)

print(f"Streaming started. Writing to: {TABLE_NAME}")
query.awaitTermination()