-- ================================================
-- Create Dictionaries
-- ================================================
-- Dictionaries are in-memory key-value stores
-- They provide O(1) lookups - much faster than JOINs
-- ================================================

USE mv_demo_dictionaries;

-- ================================================
-- Dictionary: Products
-- ================================================
-- LAYOUT(HASHED) - stores all data in a hash table
-- LIFETIME - how often to refresh from source table

DROP DICTIONARY IF EXISTS products_dict;
CREATE DICTIONARY products_dict (
    product_id UInt32,
    product_name String,
    category String,
    brand String,
    unit_price Decimal64(2),
    unit_cost Decimal64(2)
)
PRIMARY KEY product_id
SOURCE(CLICKHOUSE(
    TABLE 'dim_products'
    DB 'mv_demo_dictionaries'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);

-- ================================================
-- Dictionary: Customers
-- ================================================
DROP DICTIONARY IF EXISTS customers_dict;
CREATE DICTIONARY customers_dict (
    customer_id UInt32,
    customer_name String,
    tier String,
    country String,
    city String,
    signup_date Date
)
PRIMARY KEY customer_id
SOURCE(CLICKHOUSE(
    TABLE 'dim_customers'
    DB 'mv_demo_dictionaries'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);

-- Force load dictionaries into memory
SYSTEM RELOAD DICTIONARY products_dict;
SYSTEM RELOAD DICTIONARY customers_dict;

SELECT '[OK] Dictionaries created and loaded' AS status;

-- ================================================
-- Verify Dictionary Status
-- ================================================
SELECT
    name,
    status,
    element_count,
    formatReadableSize(bytes_allocated) AS memory_used,
    loading_duration
FROM system.dictionaries
WHERE database = 'mv_demo_dictionaries'
ORDER BY name;
