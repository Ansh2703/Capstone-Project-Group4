# ==========================================
# CSV INGESTION PIPELINE (BRONZE LAYER)
# ==========================================

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

# ------------------------------------------
# STEP 1: START SPARK SESSION
# ------------------------------------------
spark = SparkSession.builder.appName("CSV Ingestion").getOrCreate()

# ------------------------------------------
# STEP 2: LOAD CONFIG FILE
# ------------------------------------------
config_path = "/Workspace/Capstone-Project-Group4/maven_market/config/dev_config.yaml"

with open(config_path, "r") as file:
    config = yaml.safe_load(file)

# ------------------------------------------
# STEP 3: DEFINE DATASETS
# ------------------------------------------
datasets = ["transactions", "regions", "stores", "return", "calendar"]

# ------------------------------------------
# STEP 4: INGESTION FUNCTION
# ------------------------------------------

def ingest_dataset(dataset_name):

    print(f" Starting ingestion for: {dataset_name}")

    path = config["paths"][dataset_name]
    checkpoint = config["checkpoints"][dataset_name]

    # Build table name from catalog and schema
    catalog = config["catalog"]
    schema = config["schemas"]["bronze"]
    table = f"{catalog}.{schema}.{dataset_name}"

    # READ USING AUTO LOADER
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", checkpoint + "/schema") \
        .load(path)

    # ADD METADATA
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("source_file", col("_metadata.file_path")) \
           .withColumn("source", lit(dataset_name))

    # WRITE TO DELTA
    query = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint) \
        .option("mergeSchema", "true") \
        .outputMode("append") \
        .trigger(availableNow=True) \
        .toTable(table)

    return query

# ------------------------------------------
# STEP 5: RUN INGESTION
# ------------------------------------------
queries = []

for dataset in datasets:
    q = ingest_dataset(dataset)
    queries.append(q)

print(" Ingestion started for all datasets (will stop automatically).")
