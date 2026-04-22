# scripts/setup_uc.py
# ARCHIVE: One-time bootstrap script — already executed.
# Kept for reference only. Do NOT re-run without reviewing.

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Read current user dynamically instead of hardcoding
admin_user = spark.sql("SELECT current_user()").collect()[0][0]
storage_root = spark.conf.get(
    "bundle.storage_root",
    "abfss://maven-market-data@sgmavenmarket1.dfs.core.windows.net"
)

catalogs = ["maven_market_uc", "maven_market_prod"]

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
