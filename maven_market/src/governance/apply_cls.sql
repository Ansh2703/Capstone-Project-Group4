-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  TEMPORARILY DISABLED — All members need full access during dev.  ║
-- ║  Uncomment all lines below to re-enable column-level security.    ║
-- ╚══════════════════════════════════════════════════════════════════════╝

-- -- Column-Level Security: PII masking function
-- -- Only maven_admins and maven_engineers can see raw PII values
-- CREATE OR REPLACE FUNCTION maven_market_uc.gold.pii_mask(column_value STRING)
-- RETURN CASE
--   WHEN is_account_group_member('maven_admins') OR is_account_group_member('maven_engineers') THEN column_value
--   ELSE '### MASKED ###'
-- END;

-- -- Birthdate mask (returns NULL for non-privileged users)
-- CREATE OR REPLACE FUNCTION maven_market_uc.gold.pii_mask_date(column_value DATE)
-- RETURN CASE
--   WHEN is_account_group_member('maven_admins') OR is_account_group_member('maven_engineers') THEN column_value
--   ELSE NULL
-- END;

-- -- Apply masks to PII columns on the gold dim_customer table
-- ALTER TABLE maven_market_uc.gold.dim_customer ALTER COLUMN email_address SET MASK maven_market_uc.gold.pii_mask;
-- ALTER TABLE maven_market_uc.gold.dim_customer ALTER COLUMN full_name SET MASK maven_market_uc.gold.pii_mask;
-- ALTER TABLE maven_market_uc.gold.dim_customer ALTER COLUMN customer_address SET MASK maven_market_uc.gold.pii_mask;
-- ALTER TABLE maven_market_uc.gold.dim_customer ALTER COLUMN birthdate SET MASK maven_market_uc.gold.pii_mask_date;
