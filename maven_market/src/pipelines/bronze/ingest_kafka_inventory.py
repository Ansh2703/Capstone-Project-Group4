import dlt
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utils.logger import PipelineLogger

# INIT LOGGER
logger = PipelineLogger(spark, layer="bronze")

@dlt.table(
    name="bronze_inventory_kafka",
    comment="Ingesting inventory updates from Kafka"
)
def bronze_inventory_kafka():

    stage = "bronze_inventory_kafka"

    logger.log("INFO", "Starting Kafka ingestion", stage)

    try:
        API_KEY = "OWQL53ZQ5HIRVEIX"
        API_SECRET = "cflt9GTSkONEgeiTaVr887GsEoXc36+3dquLtF/XbuTQr3gSNYFMhneZsrGN7VjA"

        schema = StructType([
            StructField("event_id", IntegerType()),
            StructField("event_time", StringType()),
            StructField("product_id", IntegerType()),
            StructField("store_id", IntegerType()),
            StructField("stock_level", IntegerType()),
            StructField("event_type", StringType())
        ])

        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "pkc-56d1g.eastus.azure.confluent.cloud:9092")
            .option("subscribe", "inventory_topic")
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

        logger.log(
            "INFO",
            "Kafka stream transformation defined",
            stage,
            status="SUCCESS"
        )

        return df

    except Exception as e:
        logger.log(
            "ERROR",
            "Kafka ingestion failed",
            stage,
            status="FAILED",
            error=str(e)
        )
        raise