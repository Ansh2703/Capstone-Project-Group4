import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# 1. SHARED CONFIGURATION (Ideally pulled from your dev_config.yaml)
kafka_server = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_api_key = "OWQL53ZQ5HIRVEIX"
kafka_secret = "cflt9GTSkONEgeiTaVr887GsEoXc36+3dquLtF/XbuTQr3gSNYFMhneZsrGN7VjA"
base_path = "abfss://maven-market-data@mavengrp4.df s.core.windows.net"

# 2. SHARED SCHEMA DEFINITION
address_schema = StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True)
])

order_schema = StructType([
    StructField("ordertime", LongType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("itemid", StringType(), True),
    StructField("orderunits", DoubleType(), True),
    StructField("address", address_schema, True)
])

# 3. REUSABLE INGESTION FUNCTION
def ingest_kafka_topic(topic_name, target_name):
    print(f"Starting ingestion for topic: {topic_name}")
    
    df_kafka = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", topic_name)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_api_key}' password='{kafka_secret}';")
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = df_kafka.select(F.from_json(F.col("value").cast("string"), order_schema).alias("data")).select("data.*")

    # Use UNIQUE paths for each topic
    checkpoint_path = f"{base_path}/checkpoints/kafka/{target_name}"
    data_path = f"{base_path}/raw/kafka/{target_name}"

    return (df_parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        # This automatically registers the table in Unity Catalog
        .toTable(f"maven_market_uc.bronze.{target_name}_kafka"))

# 4. EXECUTE FOR BOTH TOPICS
ingest_kafka_topic("inventory_topic", "inventory")
ingest_kafka_topic("orders_topic", "orders")


