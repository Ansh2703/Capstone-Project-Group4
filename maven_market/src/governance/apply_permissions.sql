-- Let everyone use the catalog
GRANT USAGE ON CATALOG maven_market_uc TO `maven_engineers`;
GRANT USAGE ON CATALOG maven_market_uc TO `maven_executives`;
GRANT USAGE ON CATALOG maven_market_uc TO `maven_analysts`;


-- Engineers (Rajni/Devjit) build the data
GRANT ALL PRIVILEGES ON SCHEMA maven_market_uc.bronze TO `maven_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA maven_market_uc.silver TO `maven_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA maven_market_uc.gold TO `maven_engineers`;

-- Business users (Ojasvi/Snigdha) only read the final Gold data
GRANT USAGE, SELECT ON SCHEMA maven_market_uc.gold TO `maven_executives`;
GRANT USAGE, SELECT ON SCHEMA maven_market_uc.gold TO `maven_analysts`;