import sys

# Project root injected via pipeline configuration (bundle.project_root = ${workspace.file_path})
sys.path.insert(0, spark.conf.get("bundle.project_root"))

from src.utils.logger import PipelineLogger

import dlt
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logger = PipelineLogger(layer="bronze")

@dlt.table(
    name="bronze_inventory_kafka",
    comment="Ingesting inventory updates from Kafka"
)
def bronze_inventory_kafka():
    logger.info("Starting Kafka inventory ingestion", stage="bronze_inventory_kafka")

    # Read Kafka settings from pipeline configuration (set in dlt_pipeline.yml)
    bootstrap_servers = spark.conf.get("bundle.kafka_bootstrap_servers")
    topic             = spark.conf.get("bundle.kafka_inventory_topic")
    secret_scope      = spark.conf.get("bundle.kafka_secret_scope")
    api_key_secret    = spark.conf.get("bundle.kafka_api_key_secret")
    api_secret_secret = spark.conf.get("bundle.kafka_api_secret_secret")

    # Retrieve credentials from Databricks Secrets (never store in code)
    API_KEY    = dbutils.secrets.get(scope=secret_scope, key=api_key_secret)
    API_SECRET = dbutils.secrets.get(scope=secret_scope, key=api_secret_secret)

    schema = StructType([
        StructField("event_id", IntegerType()),
        StructField("event_time", StringType()),
        StructField("product_id", IntegerType()),
        StructField("store_id", IntegerType()),
        StructField("stock_level", IntegerType()),
        StructField("event_type", StringType())
    ])

    logger.info("Kafka inventory transformation defined", stage="bronze_inventory_kafka", status="SUCCESS")

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'
        )
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("ingestion_time", current_timestamp())
    )
