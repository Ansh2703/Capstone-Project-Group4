import dlt
from pyspark.sql.functions import col, current_timestamp, lit

# -------------------------------------------------------------------------
# NOTE: In DLT, we don't manually load YAML files from local paths using 'open'.
# We use dlt.get_parameter() or hardcoded lists for the table definitions.
# DLT manages the spark session and streaming triggers automatically.
# -------------------------------------------------------------------------

# List of datasets to create tables for
datasets = ["transactions", "regions", "stores", "returns", "calendar"]

# In DLT, we use a loop to generate functions that define our tables
def create_bronze_table(dataset_name):
    @dlt.table(
        name=f"bronze_{dataset_name}",
        comment=f"Raw ingestion of {dataset_name} data via Auto Loader."
    )
    def table_definition():
        # DLT reads the source path from your pipeline configuration
        # Make sure these keys exist in your dev_config.yaml/pipeline settings
        source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")

        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            # We do NOT provide a checkpointLocation here; DLT handles it.
            .load(source_path)
            .withColumn("ingestion_time", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("source_name", lit(dataset_name))
        )

# Loop through and register the tables with the DLT engine
for dataset in datasets:
    create_bronze_table(dataset)