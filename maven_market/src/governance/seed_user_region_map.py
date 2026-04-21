# Create the table in Gold
spark.sql("""
CREATE TABLE IF NOT EXISTS maven_market_uc.gold.user_region_map 
(user_id STRING, assigned_region STRING)
""")

# Clear and insert your specific team assignments
spark.sql("TRUNCATE TABLE maven_market_uc.gold.user_region_map")

# Update these emails to match your exact IDs from image_bd9f2c.png
spark.sql("""
INSERT INTO maven_market_uc.gold.user_region_map VALUES 
('snigdha@rajnijha29112001gmail.onmicrosoft.com', 'North America'),
('devjit@rajnijha29112001gmail.onmicrosoft.com', 'Europe')
""")