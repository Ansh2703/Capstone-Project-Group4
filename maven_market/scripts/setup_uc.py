# scripts/setup_uc.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Ensure both Production and Development catalogs exist
catalogs = ["maven_market_uc", "maven_market_dev"]

for catalog in catalogs:
    print(f"Checking Catalog: {catalog}")
    # Create the Catalog
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    
    # Create the Medallion schemas inside each catalog
    schemas = ["bronze", "silver", "gold", "audit"]
    for schema_name in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
        print(f" - Verified: {catalog}.{schema_name}")

print("Infrastructure Setup Complete!")