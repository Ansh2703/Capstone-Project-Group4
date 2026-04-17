# 1. Create the Catalog (The top-level container)
spark.sql('''CREATE CATALOG maven_market_uc
MANAGED LOCATION 'abfss://maven-market-data@mavengrp4.dfs.core.windows.net/maven_market_uc';''')

# 2. Create the Schemas (The Medallion layers)
# These represent the different stages of your data processing 
schemas = ["bronze", "silver", "gold", "audit"]

for schema_name in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS maven_market_uc.{schema_name}")

print("Catalog and Schemas (Bronze, Silver, Gold, Audit) created successfully!")

