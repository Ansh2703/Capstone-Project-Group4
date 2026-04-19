# scripts/setup_uc.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

catalogs = ["maven_market_uc", "maven_market_dev"]
human_admin = "tansh4147@gmail.com"

for catalog in catalogs:
    print(f"--- Processing Catalog: {catalog} ---")
    
    # 1. Ensure Catalog exists
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    
    # 2. Use GRANT instead of ALTER OWNER
    # This gives you full power without violating "Manage" restrictions
    print(f"Granting privileges on {catalog} to {human_admin}...")
    spark.sql(f"GRANT USE CATALOG, BROWSE ON CATALOG {catalog} TO `{human_admin}`")
    spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `{human_admin}`")
    
    # 3. Create and Grant on Schemas
    schemas = ["bronze", "silver", "gold", "audit"]
    for schema_name in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
        spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {catalog}.{schema_name} TO `{human_admin}`")
        print(f" - Granted access to {catalog}.{schema_name}")

print("Infrastructure Setup Complete! You should now see the catalogs.")