import sys

# Project root injected via pipeline configuration (bundle.project_root = ${workspace.file_path})
sys.path.insert(0, spark.conf.get("bundle.project_root"))

from src.utils.logger import PipelineLogger

import dlt
from pyspark.sql.functions import current_timestamp, lit

logger = PipelineLogger(layer="bronze")

datasets = ["customers", "products"]

def create_mongo_bronze(dataset_name):

    table_name = f"bronze_{dataset_name}"

    @dlt.table(
        name=table_name,
        comment=f"Ingesting MongoDB data for {dataset_name}"
    )
    def mongo_definition():
        logger.info(f"Starting Mongo ingestion for {dataset_name}", stage=table_name)

        # Fetch path from config
        source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")

        return (
            spark.readStream.format("delta")
            .load(source_path)
            .withColumn("ingestion_time", current_timestamp())
            .withColumn("source", lit(dataset_name))
        )

# CREATE TABLES
for dataset in datasets:
    create_mongo_bronze(dataset)

logger.info("All Mongo Bronze tables registered", stage="bronze_mongo_pipeline", status="SUCCESS")
