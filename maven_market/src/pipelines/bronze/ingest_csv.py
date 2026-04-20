import dlt
from pyspark.sql.functions import col, current_timestamp, lit

# List of datasets to create tables for
datasets = ["transactions", "regions", "stores", "return", "calendar"]

def create_bronze_table(dataset_name):
    @dlt.table(
        name=f"{dataset_name}",
        comment=f"Raw ingestion of {dataset_name} data via Auto Loader."
    )
    def table_definition():
        # Source path is fetched from pipeline configuration
        source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")

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

for dataset in datasets:
    create_bronze_table(dataset)