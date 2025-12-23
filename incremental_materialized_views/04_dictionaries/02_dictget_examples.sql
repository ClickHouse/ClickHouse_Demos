-- ================================================
-- Using dictGet() for Fast Lookups
-- ================================================
-- dictGet(dict_name, attribute, key) - retrieves a single attribute
-- dictGetOrDefault() - returns default if key not found
-- ================================================

USE mv_demo_dictionaries;

-- ================================================
-- Basic dictGet() Usage
-- ================================================

SELECT '-- Basic dictGet() examples:' AS info;

-- Single attribute lookup
SELECT
    dictGet('products_dict', 'product_name', toUInt32(1)) AS product_1_name,
    dictGet('products_dict', 'category', toUInt32(1)) AS product_1_category,
    dictGet('products_dict', 'unit_price', toUInt32(1)) AS product_1_price;

-- Multiple products
SELECT
    product_id,
    dictGet('products_dict', 'product_name', product_id) AS name,
    dictGet('products_dict', 'brand', product_id) AS brand,
    dictGet('products_dict', 'unit_price', product_id) AS price
FROM (SELECT arrayJoin([1, 2, 3, 4, 5]) AS product_id);

-- ================================================
-- Customer Lookups
-- ================================================

SELECT '-- Customer lookups:' AS info;

SELECT
    customer_id,
    dictGet('customers_dict', 'customer_name', customer_id) AS name,
    dictGet('customers_dict', 'tier', customer_id) AS tier,
    dictGet('customers_dict', 'country', customer_id) AS country
FROM (SELECT arrayJoin([1001, 1002, 1003, 1004, 1005]) AS customer_id);

-- ================================================
-- dictGetOrDefault() - Handle Missing Keys
-- ================================================

SELECT '-- dictGetOrDefault() for missing keys:' AS info;

SELECT
    product_id,
    dictGetOrDefault('products_dict', 'product_name', product_id, 'UNKNOWN') AS name,
    dictGetOrDefault('products_dict', 'unit_price', product_id, toDecimal64(0, 2)) AS price
FROM (SELECT arrayJoin([1, 2, 9999]) AS product_id);  -- 9999 doesn't exist

-- ================================================
-- Calculated Fields with dictGet()
-- ================================================

SELECT '-- Calculated profit margin:' AS info;

SELECT
    product_id,
    dictGet('products_dict', 'product_name', product_id) AS name,
    dictGet('products_dict', 'unit_price', product_id) AS price,
    dictGet('products_dict', 'unit_cost', product_id) AS cost,
    dictGet('products_dict', 'unit_price', product_id) -
        dictGet('products_dict', 'unit_cost', product_id) AS profit,
    round((dictGet('products_dict', 'unit_price', product_id) -
        dictGet('products_dict', 'unit_cost', product_id)) /
        dictGet('products_dict', 'unit_price', product_id) * 100, 1) AS margin_pct
FROM (SELECT arrayJoin([1, 2, 5, 6, 9]) AS product_id);

SELECT '[OK] dictGet() examples complete' AS status;
