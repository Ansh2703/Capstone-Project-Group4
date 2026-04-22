-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  TEMPORARILY DISABLED — All members need full access during dev.  ║
-- ║  Uncomment all lines below to re-enable tiered permissions.       ║
-- ╚══════════════════════════════════════════════════════════════════════╝

-- -- Let everyone use the catalog
-- GRANT USAGE ON CATALOG identifier('${var.target_catalog}') TO `maven_engineers`;
-- GRANT USAGE ON CATALOG identifier('${var.target_catalog}') TO `maven_executives`;
-- GRANT USAGE ON CATALOG identifier('${var.target_catalog}') TO `maven_analysts`;


-- -- Engineers (Rajni/Devjit) build the data
-- GRANT ALL PRIVILEGES ON SCHEMA identifier('${var.target_catalog}' || '.bronze') TO `maven_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA identifier('${var.target_catalog}' || '.silver') TO `maven_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA identifier('${var.target_catalog}' || '.gold') TO `maven_engineers`;

-- -- Business users (Ojasvi/Snigdha) only read the final Gold data
-- GRANT USAGE, SELECT ON SCHEMA identifier('${var.target_catalog}' || '.gold')  TO `maven_executives`;
-- GRANT USAGE, SELECT ON SCHEMA identifier('${var.target_catalog}' || '.gold')  TO `maven_analysts`;
