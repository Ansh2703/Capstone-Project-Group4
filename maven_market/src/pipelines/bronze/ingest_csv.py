import sys

# Project root injected via pipeline configuration (bundle.project_root = ${workspace.file_path})
sys.path.insert(0, spark.conf.get("bundle.project_root"))

from src.utils.logger import PipelineLogger

import dlt
from pyspark.sql.functions import col, current_timestamp, lit

logger = PipelineLogger(layer="bronze")

datasets = ["transactions", "regions", "stores", "return", "calendar"]

#This is a factory function — it takes a dataset name and creates a DLT table for it
def create_bronze_table(dataset_name):

    table_name = f"bronze_{dataset_name}"

    
#The @dlt.table decorator registers this function as a DLT-managed table. Databricks will automatically handle

    @dlt.table(
        name=table_name,
        comment=f"Raw ingestion of {dataset_name} data via Auto Loader."
    )
    def table_definition():
        logger.info(f"Starting ingestion for {dataset_name}", stage=table_name)

        source_path = spark.conf.get(f"bundle.source_path_{dataset_name}") #These paths are defined in the databricks.yml deployment config

        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(source_path)
            .withColumn("ingestion_time", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("source_name", lit(dataset_name))
        )
