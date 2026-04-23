# Bronze Layer - MongoDB Ingestion Pipeline
# Reads MongoDB-sourced data

import sys

# Project root injected via pipeline configuration (bundle.project_root = ${workspace.file_path})
sys.path.insert(0, spark.conf.get("bundle.project_root"))

from src.utils.logger import PipelineLogger

# 'dlt' is provided by the Declarative Pipeline runtime — it won't work in interactive notebooks
import dlt
from pyspark.sql.functions import current_timestamp, lit

# Logger scoped to the bronze layer
logger = PipelineLogger(layer="bronze")

# Each name here maps to a Delta source (originally from MongoDB) and produces a bronze_<name> table
datasets = ["customers", "products"]

# Factory function — takes a dataset name and registers a DLT table for it
def create_mongo_bronze(dataset_name):

    table_name = f"bronze_{dataset_name}"

    # @dlt.table registers this as a DLT-managed streaming table
    @dlt.table(
        name=table_name,
        comment=f"Ingesting MongoDB data for {dataset_name}"
    )
    def mongo_definition():
        logger.info(f"Starting Mongo ingestion for {dataset_name}", stage=table_name)

        # Source path defined per-dataset in databricks.yml config
        source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")

        return (
            spark.readStream.format("delta")                        #delta streaming to read the delta tables.            
            .load(source_path)
            .withColumn("ingestion_time", current_timestamp())      # When the row was ingested
            .withColumn("source", lit(dataset_name))                # Dataset identifier
        )

# Register a bronze table for each dataset
for dataset in datasets:
    create_mongo_bronze(dataset)

logger.info("All Mongo Bronze tables registered", stage="bronze_mongo_pipeline", status="SUCCESS")
