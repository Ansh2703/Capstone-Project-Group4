import dlt
from pyspark.sql.functions import current_timestamp, lit
from utils.logger import PipelineLogger

# INIT LOGGER
logger = PipelineLogger(spark, layer="bronze")

datasets = ["customers", "products"]

def create_mongo_bronze(dataset_name):

    table_name = f"bronze_{dataset_name}"
    stage = table_name

    @dlt.table(
        name=table_name,
        comment=f"Ingesting MongoDB data for {dataset_name}"
    )
    def mongo_definition():

        logger.log("INFO", "Starting Mongo ingestion", stage)

        try:
            # Fetch path from config
            source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")

            df = (
                spark.readStream.format("delta")
                .load(source_path)
                .withColumn("ingestion_time", current_timestamp())
                .withColumn("source", lit(dataset_name))
            )

            logger.log(
                "INFO",
                "Mongo stream transformation defined",
                stage,
                status="SUCCESS"
            )

            return df

        except Exception as e:
            logger.log(
                "ERROR",
                "Mongo ingestion failed",
                stage,
                status="FAILED",
                error=str(e)
            )
            raise

# CREATE TABLES
for dataset in datasets:
    create_mongo_bronze(dataset)

# Optional pipeline-level log
logger.log("INFO", "All Mongo Bronze tables registered", stage="bronze_mongo_pipeline")