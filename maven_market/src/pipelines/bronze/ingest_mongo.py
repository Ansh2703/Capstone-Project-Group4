import uuid
from datetime import datetime, timezone
import dlt
from pyspark.sql.functions import current_timestamp, lit

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

datasets = ["customers", "products"]

def create_mongo_bronze(dataset_name):

    table_name = f"bronze_{dataset_name}"
    stage = table_name

    @dlt.table(
        name=table_name,
        comment=f"Ingesting MongoDB data for {dataset_name}"
    )
    def mongo_definition():

        logger.info("Starting Mongo ingestion", stage)

        try:
            # Fetch path from config
            source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")

            df = (
                spark.readStream.format("delta")
                .load(source_path)
                .withColumn("ingestion_time", current_timestamp())
                .withColumn("source", lit(dataset_name))
            )

            logger.info("Mongo stream transformation defined", stage, status="SUCCESS")

            return df

        except Exception as e:
            logger.error("Mongo ingestion failed", stage, status="FAILED", error=str(e))
            raise

# CREATE TABLES
for dataset in datasets:
    create_mongo_bronze(dataset)

logger.info("All Mongo Bronze tables registered", stage="bronze_mongo_pipeline")
