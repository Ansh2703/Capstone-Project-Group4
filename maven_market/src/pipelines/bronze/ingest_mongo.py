import dlt
from pyspark.sql.functions import current_timestamp, lit

datasets = ["customers", "products"]

def create_mongo_bronze(dataset_name):
    @dlt.table(
        name=f"{dataset_name}",
        comment=f"Ingesting MongoDB data for {dataset_name}"
    )
    def mongo_definition():
        # Fetching path from pipeline config
        source_path = spark.conf.get(f"bundle.source_path_{dataset_name}")
        
        return (
            spark.readStream.format("delta")
            .load(source_path)
            .withColumn("ingestion_time", current_timestamp())
            .withColumn("source", lit(dataset_name))
        )

for dataset in datasets:
    create_mongo_bronze(dataset)