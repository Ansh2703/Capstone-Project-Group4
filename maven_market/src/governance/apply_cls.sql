-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  Column-Level Security: PII masking functions                     ║
-- ║                                                                    ║
-- ║  Admins & engineers see raw values; all others see masked data.    ║
-- ║  Applied to dim_customer, dim_store, and agg_customer_ltv.         ║
-- ╚══════════════════════════════════════════════════════════════════════╝

-- ── Masking functions ──────────────────────────────────────────────────

-- String PII mask (email, address, name, phone, postal code)
CREATE OR REPLACE FUNCTION identifier('${var.target_catalog}' || '.gold.mask_pii_string')(pii_value STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('maven_admins') OR is_account_group_member('maven_engineers') THEN pii_value
  ELSE '### MASKED ###'
END;

-- Date PII mask (birthdate)
CREATE OR REPLACE FUNCTION identifier('${var.target_catalog}' || '.gold.mask_pii_date')(pii_value DATE)
RETURNS DATE
RETURN CASE
  WHEN is_account_group_member('maven_admins') OR is_account_group_member('maven_engineers') THEN pii_value
  ELSE NULL
END;

-- ── dim_customer masks (7 columns) ─────────────────────────────────────

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_customer')
  ALTER COLUMN first_name SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_customer')
  ALTER COLUMN last_name SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_customer')
  ALTER COLUMN full_name SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_customer')
  ALTER COLUMN email_address SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_customer')
  ALTER COLUMN customer_address SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_customer')
  ALTER COLUMN customer_postal_code SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_customer')
  ALTER COLUMN birthdate SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_date');

-- ── dim_store masks (2 columns) ────────────────────────────────────────

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_store')
  ALTER COLUMN store_street_address SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.dim_store')
  ALTER COLUMN store_phone SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

-- ── agg_customer_ltv masks (2 columns) ─────────────────────────────────

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.agg_customer_ltv')
  ALTER COLUMN first_name SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');

ALTER MATERIALIZED VIEW identifier('${var.target_catalog}' || '.gold.agg_customer_ltv')
  ALTER COLUMN last_name SET MASK identifier('${var.target_catalog}' || '.gold.mask_pii_string');
