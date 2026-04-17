# ==========================================
# CSV INGESTION PIPELINE (BRONZE LAYER)
# ==========================================

# Import required libraries
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
# 🔴 Replace with your actual path
config_path = "/Workspace/maven_market/config/dev_config.yaml"

with open(config_path, "r") as file:
    config = yaml.safe_load(file)

# ------------------------------------------
# STEP 3: DEFINE DATASETS
# ------------------------------------------
# All CSV datasets in ADLS

datasets = ["transactions", "regions", "stores", "return", "calendar"]

# ------------------------------------------
# STEP 4: INGESTION FUNCTION
# ------------------------------------------

def ingest_dataset(dataset_name):

    print(f"Starting ingestion for: {dataset_name}")

    # Get values from config
    path = config["paths"][dataset_name]
    checkpoint = config["checkpoints"][dataset_name]
    table = config["tables"][dataset_name]

    # --------------------------------------
    # READ DATA USING AUTO LOADER
    # --------------------------------------
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", checkpoint + "/schema") \
        .load(path)

    # --------------------------------------
    # ADD METADATA COLUMNS
    # --------------------------------------
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("source_file", col("_metadata.file_path")) \
           .withColumn("source", lit(dataset_name))

    # --------------------------------------
    # WRITE TO BRONZE TABLE
    # --------------------------------------
    query = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint) \
        .outputMode("append") \
        .toTable(table)

    return query

# ------------------------------------------
# STEP 5: RUN INGESTION FOR ALL DATASETS
# ------------------------------------------

queries = []

for dataset in datasets:
    q = ingest_dataset(dataset)
    queries.append(q)

# ------------------------------------------
# STEP 6: KEEP STREAMS RUNNING
# ------------------------------------------

for q in queries:
    q.awaitTermination()
