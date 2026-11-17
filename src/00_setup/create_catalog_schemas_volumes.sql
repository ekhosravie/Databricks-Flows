-- Catalog
CREATE CATALOG IF NOT EXISTS main;

-- Bronze
CREATE SCHEMA IF NOT EXISTS main.retail_bronze;

-- Silver
CREATE SCHEMA IF NOT EXISTS main.retail_silver;

-- Gold
CREATE SCHEMA IF NOT EXISTS main.retail_gold;

-- Landing Volume
CREATE VOLUME IF NOT EXISTS
main.retail_bronze.raw_orders;
