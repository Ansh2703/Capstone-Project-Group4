-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  Row-Level Security: region-based access control                  ║
-- ║                                                                    ║
-- ║  Admins & engineers bypass all filters (see all regions).          ║
-- ║  Analysts & executives are filtered via user_region_map.           ║
-- ║                                                                    ║
-- ║  Deployed mappings:                                                ║
-- ║    Snigdha (analyst)   → North West, Central West, South West     ║
-- ║    Devjit  (executive) → South West                               ║
-- ║                                                                    ║
-- ║  NOTE: dim_store must NOT have a row filter — the gold pipeline   ║
-- ║  reads it via dlt.read("dim_store") and the service principal     ║
-- ║  is not in maven_admins/maven_engineers, so the filter would      ║
-- ║  block all rows and produce empty agg_regional_sales and          ║
-- ║  agg_store_space_utilization. The filter on agg_regional_sales    ║
-- ║  alone is sufficient for regional dashboard filtering.            ║
-- ╚══════════════════════════════════════════════════════════════════════╝

-- Row-filter function: returns TRUE to keep the row
CREATE OR REPLACE FUNCTION identifier('${var.target_catalog}' || '.gold.region_filter')(region_name STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('maven_admins')
  OR is_account_group_member('maven_engineers')
  OR EXISTS (
    SELECT 1 FROM identifier('${var.target_catalog}' || '.gold.user_region_map')
    WHERE user_id = current_user() AND assigned_region = region_name
  );

-- Apply the row filter to end-user-facing tables only
-- (dim_store intentionally excluded — see header note)
ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_region')
  SET ROW FILTER identifier('${var.target_catalog}' || '.gold.region_filter') ON (sales_region);

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.agg_regional_sales')
  SET ROW FILTER identifier('${var.target_catalog}' || '.gold.region_filter') ON (sales_region);
