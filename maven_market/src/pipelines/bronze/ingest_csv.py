# Bronze Layer - CSV Ingestion Pipeline
# Reads raw CSV files into bronze streaming tables using Auto Loader

import sys

# Project root injected via pipeline configuration (bundle.project_root = ${workspace.file_path})
sys.path.insert(0, spark.conf.get("bundle.project_root"))

from src.utils.logger import PipelineLogger

# 'dlt' is provided by the Declarative Pipeline runtime — it won't work in interactive notebooks
import dlt
from pyspark.sql.functions import col, current_timestamp, lit

# Logger scoped to the bronze layer
logger = PipelineLogger(layer="bronze")

# Each name here maps to a CSV source and produces a bronze_<name> streaming table
datasets = ["transactions", "regions", "stores", "return", "calendar"]

# Factory function — takes a dataset name and registers a DLT table for it
def create_bronze_table(dataset_name):

    table_name = f"bronze_{dataset_name}"

    # @dlt.table registers this as a DLT-managed streaming table
    # Databricks handles schema evolution, checkpointing, and incremental processing
    @dlt.table(
        name=table_name,
        comment=f"Raw ingestion of {dataset_name} data via Auto Loader."
    )
    def table_definition():
        logger.info(f"Starting ingestion for {dataset_name}", stage=table_name)

        # Source path defined per-dataset in databricks.yml config
        source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")

        return (
            spark.readStream.format("cloudFiles")           # Auto Loader for incremental file ingestion
            .option("cloudFiles.format", "csv")              # File format
            .option("header", "true")                        # First row is column headers
            .option("cloudFiles.inferColumnTypes", "true")   # Auto-detect column types
            .load(source_path)
            .withColumn("ingestion_time", current_timestamp())      # When the row was ingested
            .withColumn("source_file", col("_metadata.file_path"))  # Original file path
            .withColumn("source_name", lit(dataset_name))           # Dataset identifier
        )

# Register a bronze table for each dataset
for dataset in datasets:
    create_bronze_table(dataset)
