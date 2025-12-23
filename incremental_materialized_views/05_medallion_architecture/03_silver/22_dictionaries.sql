-- Step 6: Dictionaries and Enrichment MV
-- In-memory lookups replace JOINs, MV enriches orders automatically

USE fastmart_demo;

-- Dictionary: Products (O(1) lookup by product_id)
DROP DICTIONARY IF EXISTS products_dict;
CREATE DICTIONARY products_dict (
    product_id UInt64,
    product_name String,
    category String,
    brand String,
    price Decimal64(2),
    cost Decimal64(2)
)
PRIMARY KEY product_id
SOURCE(CLICKHOUSE(TABLE 'products' DB 'fastmart_demo'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);

-- Dictionary: Customers (O(1) lookup by customer_id)
DROP DICTIONARY IF EXISTS customers_dict;
CREATE DICTIONARY customers_dict (
    customer_id UInt64,
    customer_name String,
    customer_tier String,
    country String,
    city String
)
PRIMARY KEY customer_id
SOURCE(CLICKHOUSE(TABLE 'customers' DB 'fastmart_demo'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);

-- Dictionary: Suppliers
DROP DICTIONARY IF EXISTS suppliers_dict;
CREATE DICTIONARY suppliers_dict (
    supplier_id UInt32,
    supplier_name String,
    country String,
    rating Float32
)
PRIMARY KEY supplier_id
SOURCE(CLICKHOUSE(TABLE 'suppliers' DB 'fastmart_demo'))
LAYOUT(HASHED())
LIFETIME(MIN 600 MAX 1200);

-- Load dictionaries
SYSTEM RELOAD DICTIONARY products_dict;
SYSTEM RELOAD DICTIONARY customers_dict;
SYSTEM RELOAD DICTIONARY suppliers_dict;

-- MV: Enrich orders with product/customer data via dictGet
DROP VIEW IF EXISTS orders_enrichment_mv;
CREATE MATERIALIZED VIEW orders_enrichment_mv
TO orders_enriched
AS
SELECT
    order_id,
    customer_id,
    dictGet('customers_dict', 'customer_name', customer_id) AS customer_name,
    dictGet('customers_dict', 'customer_tier', customer_id) AS customer_tier,
    product_id,
    dictGet('products_dict', 'product_name', product_id) AS product_name,
    dictGet('products_dict', 'category', product_id) AS category,
    dictGet('products_dict', 'brand', product_id) AS brand,
    quantity,
    price AS unit_price,
    total_amount,
    total_amount - (quantity * dictGet('products_dict', 'cost', product_id)) AS profit_margin,
    payment_method,
    order_time,
    source_system
FROM orders_silver;

-- Validation: Dictionary status
SELECT '[OK] Dictionaries' AS step;
SELECT name, status, element_count, formatReadableSize(bytes_allocated) AS memory
FROM system.dictionaries
WHERE database = 'fastmart_demo'
ORDER BY name;

-- Validation: Test lookups
SELECT '[OK] Lookup test' AS step;
SELECT
    dictGet('products_dict', 'product_name', toUInt64(1)) AS product,
    dictGet('customers_dict', 'customer_name', toUInt64(1001)) AS customer;

-- NOTE: Do NOT insert data here. Gold MVs must be created first.
-- Sample data will be inserted in 31_gold_hourly.sql after ALL MVs exist.
