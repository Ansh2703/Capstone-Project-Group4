import uuid
from datetime import datetime, timezone
import dlt
from pyspark.sql.functions import col, current_timestamp, lit

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

datasets = ["transactions", "regions", "stores", "return", "calendar"]

def create_bronze_table(dataset_name):

    table_name = f"bronze_{dataset_name}"
    stage = table_name

    @dlt.table(
        name=table_name,
        comment=f"Raw ingestion of {dataset_name} data via Auto Loader."
    )
    def table_definition():

        logger.info("Starting ingestion", stage)

        try:
            source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")

            df = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("cloudFiles.inferColumnTypes", "true")
                .load(source_path)
                .withColumn("ingestion_time", current_timestamp())
                .withColumn("source_file", col("_metadata.file_path"))
                .withColumn("source_name", lit(dataset_name))
            )

            logger.info("Transformation defined", stage, status="SUCCESS")
            return df

        except Exception as e:
            logger.error("Ingestion failed", stage, status="FAILED", error=str(e))
            raise


for dataset in datasets:
    create_bronze_table(dataset)

logger.info("All Bronze CSV tables registered", stage="bronze_pipeline")
