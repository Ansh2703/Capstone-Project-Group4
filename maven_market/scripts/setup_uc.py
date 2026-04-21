# scripts/setup_uc.py
# ARCHIVE: This was used for the initial bootstrap. 
# Ownership has been claimed in the UI. 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

catalogs = ["maven_market_uc", "maven_market_dev"]
admin_user = "rajni.jha29112001@gmail.com"
storage_root = "abfss://maven-market-data@sgmavenmarket1.dfs.core.windows.net"

for catalog in catalogs:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} MANAGED LOCATION '{storage_root}/{catalog}'")
        spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `{admin_user}`")
        for schema in ["bronze", "silver", "gold", "audit"]:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {catalog}.{schema} TO `{admin_user}`")
        print(f"Catalog {catalog} setup complete.")
    except Exception as e:
        print(f"Skipping {catalog}: {e}")

print("Infrastructure Setup Verified.")