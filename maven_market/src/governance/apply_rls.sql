-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  TEMPORARILY DISABLED — All members need full access during dev.  ║
-- ║  Uncomment all lines below to re-enable row-level security.       ║
-- ╚══════════════════════════════════════════════════════════════════════╝

-- -- Row-Level Security: region-based access control
-- -- Admins and executives see all regions; regional managers see only their assigned regions
-- -- This function is referenced by the agg_regional_sales table row filter

-- CREATE OR REPLACE FUNCTION maven_market_uc.gold.region_filter(sales_region STRING)
-- RETURN 
--   is_account_group_member('maven_admins') OR 
--   is_account_group_member('maven_executives') OR 
--   EXISTS (
--     SELECT 1 FROM maven_market_uc.gold.user_region_map 
--     WHERE user_id = current_user() AND assigned_region = sales_region
--   );

-- -- Apply the row filter to the regional sales aggregation (gold layer)
-- -- Table: maven_market_uc.gold.agg_regional_sales
-- -- Filter column: sales_region (STRING)
-- ALTER TABLE maven_market_uc.gold.agg_regional_sales
--   SET ROW FILTER maven_market_uc.gold.region_filter ON (sales_region);
