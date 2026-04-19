# scripts/setup_uc.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Ensure all required catalogs exist
# We create 'maven_market_dev' specifically to fix your 404 error
catalogs = ["maven_market_uc", "maven_market_dev"]

for catalog in catalogs:
    print(f"Checking Catalog: {catalog}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    
    # Standard Medallion layers
    for schema in ["bronze", "silver", "gold", "audit"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        print(f" - Verified: {catalog}.{schema}")

print("Infrastructure Setup Complete!")