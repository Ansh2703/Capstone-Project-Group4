# seed_user_region_map.py — Seeds user-to-region mapping for RLS enforcement
# Catalog name is read from pipeline configuration (not hardcoded)

CATALOG = spark.conf.get("bundle.target_catalog", "maven_market_uc")

# Create the table in Gold
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.user_region_map 
(user_id STRING, assigned_region STRING)
""")

# Clear and insert your specific team assignments
spark.sql(f"TRUNCATE TABLE {CATALOG}.gold.user_region_map")

# Update these emails to match your exact team member IDs
spark.sql(f"""
INSERT INTO {CATALOG}.gold.user_region_map VALUES 
('snigdha@rajnijha29112001gmail.onmicrosoft.com', 'North America'),
('devjit@rajnijha29112001gmail.onmicrosoft.com', 'Europe')
""")
