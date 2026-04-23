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

# Snigdha (analyst) → US regions ("North America") mapped to actual sales_region values
# Devjit  (executive) → South West region only
spark.sql(f"""
INSERT INTO {CATALOG}.gold.user_region_map VALUES 
('snigdha@rajnijha29112001gmail.onmicrosoft.com', 'North West'),
('snigdha@rajnijha29112001gmail.onmicrosoft.com', 'Central West'),
('snigdha@rajnijha29112001gmail.onmicrosoft.com', 'South West'),
('devjit@rajnijha29112001gmail.onmicrosoft.com',  'South West')
""")
