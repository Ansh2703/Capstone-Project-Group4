# ==========================================
# PARQUET INGESTION PIPELINE (BRONZE LAYER)
# ==========================================

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType

# ------------------------------------------
# STEP 1: START SPARK SESSION
# ------------------------------------------
spark = SparkSession.builder.appName("Parquet Ingestion").getOrCreate()

# ------------------------------------------
# STEP 2: LOAD CONFIG FILE
# ------------------------------------------
config_path = "/Workspace/Capstone-Project-Group4/maven_market/config/dev_config.yaml"

with open(config_path, "r") as file:
    config = yaml.safe_load(file)

# ------------------------------------------
# STEP 3: DEFINE DATASETS
# ------------------------------------------
datasets = ["customers", "products"]

# ------------------------------------------
# STEP 4: DEFINE STRICT SCHEMAS
# ------------------------------------------

common_schema = StructType([
    StructField("_id", StringType(), False),
    StructField("_fivetran_synced", TimestampType(), True),
    StructField("data", StringType(), True),  # JSON string from MongoDB
    StructField("_fivetran_deleted", BooleanType(), True)
])

schemas = {
    "customers": common_schema,
    "products": common_schema
}

# ------------------------------------------
# STEP 5: INGESTION FUNCTION
# ------------------------------------------

def ingest_parquet_dataset(dataset_name):

    print(f"Starting ingestion for: {dataset_name}")

    path = config["paths"][dataset_name]
    checkpoint = config["checkpoints"][dataset_name]

    catalog = config["catalog"]
    schema_name = config["schemas"]["bronze"]
    table = f"{catalog}.{schema_name}.{dataset_name}"

    schema = schemas[dataset_name]

    # READ USING DELTA STREAMING (schema inferred from Delta log)
    df = spark.readStream.format("delta").load(path)

    # ADD METADATA
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("source", lit(dataset_name))

    # WRITE TO DELTA (BRONZE)
    query = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint) \
        .outputMode("append") \
        .trigger(availableNow=True) \
        .toTable(table)

    return query

# ------------------------------------------
# STEP 6: RUN INGESTION
# ------------------------------------------

queries = []

for dataset in datasets:
    q = ingest_parquet_dataset(dataset)
    queries.append(q)

print("Parquet ingestion completed for all datasets (batch via Auto Loader).")
