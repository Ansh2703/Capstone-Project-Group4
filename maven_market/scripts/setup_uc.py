# scripts/setup_uc.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# The catalogs we need to claim
catalogs = ["maven_market_uc", "maven_market_dev"]
# YOUR email - the human who needs to see the data
human_admin = "tansh4147@gmail.com"

for catalog in catalogs:
    print(f"--- Processing Catalog: {catalog} ---")
    
    # 1. Ensure it exists (The Service Principal does this)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    
    # 2. TRANSFER OWNERSHIP (The Service Principal gives the keys to YOU)
    print(f"Transferring ownership of {catalog} to {human_admin}...")
    spark.sql(f"ALTER CATALOG {catalog} OWNER TO `{human_admin}`")
    
    # 3. Grant full permissions back to yourself just to be safe
    spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `{human_admin}`")
    
    # 4. Fix Schemas inside the catalog
    schemas = ["bronze", "silver", "gold", "audit"]
    for schema_name in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
        # Transfer ownership of every schema too
        spark.sql(f"ALTER SCHEMA {catalog}.{schema_name} OWNER TO `{human_admin}`")
        print(f" - Ownership of {catalog}.{schema_name} transferred to {human_admin}")

print("Mission Accomplished: You are now the owner of all infrastructure!")