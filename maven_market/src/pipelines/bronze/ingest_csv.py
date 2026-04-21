import dlt
from pyspark.sql.functions import col, current_timestamp, lit
from utils.logger import PipelineLogger

# INIT LOGGER (DLT-safe usage)
logger = PipelineLogger(spark, layer="bronze")

# List of datasets
datasets = ["transactions", "regions", "stores", "return", "calendar"]

# FUNCTION TO CREATE TABLES
def create_bronze_table(dataset_name):

    table_name = f"bronze_{dataset_name}"
    stage = table_name

    @dlt.table(
        name=table_name,
        comment=f"Raw ingestion of {dataset_name} data via Auto Loader."
    )
    def table_definition():

        # LOG START (lightweight)
        logger.log("INFO", "Starting ingestion", stage)

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

            # LOG SUCCESS (lightweight)
            logger.log(
                "INFO",
                "Ingestion transformation defined",
                stage,
                status="SUCCESS"
            )

            return df

        except Exception as e:
            logger.log(
                "ERROR",
                "Ingestion failed",
                stage,
                status="FAILED",
                error=str(e)
            )
            raise

# CREATE ALL TABLES
for dataset in datasets:
    create_bronze_table(dataset)

# GLOBAL PIPELINE LOG
logger.log("INFO", "All Bronze CSV tables registered", stage="bronze_pipeline")
