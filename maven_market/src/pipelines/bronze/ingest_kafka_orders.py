import uuid
from datetime import datetime, timezone
import dlt
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ── Inline logger (DLT-safe, print-only — avoids cross-directory import) ──
class PipelineLogger:
    def __init__(self, spark=None, layer="unknown", pipeline="maven_market"):
        self.spark = spark
        self.layer = layer
        self.pipeline = pipeline
        self.run_id = str(uuid.uuid4())

    def log(self, level, message, stage,
            status="RUNNING", row_count=None, error=None):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": ts, "run_id": self.run_id,
            "pipeline": self.pipeline, "layer": self.layer,
            "stage": stage, "level": level, "message": message,
            "status": status, "row_count": row_count, "error": error,
        }
        print(f"[LOG] {log_entry}")

    def info(self, message, stage, **kw):
        self.log("INFO", message, stage, **kw)

    def warn(self, message, stage, **kw):
        self.log("WARN", message, stage, **kw)

    def error(self, message, stage, **kw):
        self.log("ERROR", message, stage, **kw)


logger = PipelineLogger(layer="bronze")

@dlt.table(
    name="bronze_orders_kafka",
    comment="Ingesting orders from Confluent Cloud Kafka"
)
def bronze_orders_kafka():

    stage = "bronze_orders_kafka"

    logger.info("Starting Kafka ingestion", stage)

    try:
        # Read Kafka settings from pipeline configuration (set in dlt_pipeline.yml)
        bootstrap_servers = spark.conf.get("bundle.kafka_bootstrap_servers")
        topic             = spark.conf.get("bundle.kafka_orders_topic")
        secret_scope      = spark.conf.get("bundle.kafka_secret_scope")
        api_key_secret    = spark.conf.get("bundle.kafka_api_key_secret")
        api_secret_secret = spark.conf.get("bundle.kafka_api_secret_secret")

        # Retrieve credentials from Databricks Secrets (never store in code)
        API_KEY    = dbutils.secrets.get(scope=secret_scope, key=api_key_secret)
        API_SECRET = dbutils.secrets.get(scope=secret_scope, key=api_secret_secret)

        schema = StructType([
            StructField("event_time", StringType()),
            StructField("order_id", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("store_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("quantity", IntegerType())
        ])

        df = (
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

        logger.info("Kafka stream transformation defined", stage, status="SUCCESS")

        return df

    except Exception as e:
        logger.error("Kafka ingestion failed", stage, status="FAILED", error=str(e))
        raise
