-- Step 2: Dimension Tables
-- Creates products, customers, suppliers tables for dictionary lookups

USE fastmart_demo;

-- Products dimension
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id UInt64,
    product_name String,
    category String,
    brand String,
    price Decimal64(2),
    cost Decimal64(2),
    supplier_id UInt32,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY product_id;

-- Customers dimension
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id UInt64,
    customer_name String,
    email String,
    customer_tier String,  -- Bronze, Silver, Gold, Platinum
    country String,
    state String,
    city String,
    signup_date Date,
    lifetime_value Decimal64(2) DEFAULT 0,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY customer_id;

-- Suppliers dimension
DROP TABLE IF EXISTS suppliers;
CREATE TABLE suppliers (
    supplier_id UInt32,
    supplier_name String,
    country String,
    rating Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY supplier_id;

-- Sample data (run generate_dimensions.py for full dataset)
INSERT INTO products (product_id, product_name, category, brand, price, cost, supplier_id) VALUES
    (1, 'Wireless Mouse', 'Electronics', 'TechBrand', 29.99, 15.00, 1),
    (2, 'USB-C Cable', 'Electronics', 'TechBrand', 12.99, 5.00, 1),
    (3, 'Coffee Mug', 'Home', 'HomeBrand', 9.99, 3.50, 2),
    (4, 'Notebook Pack', 'Office', 'PaperCo', 14.99, 6.00, 3),
    (5, 'Desk Lamp', 'Home', 'LightPro', 39.99, 20.00, 2);

INSERT INTO customers (customer_id, customer_name, email, customer_tier, country, state, city, signup_date) VALUES
    (1001, 'Alice Johnson', 'alice@example.com', 'Gold', 'USA', 'CA', 'San Francisco', '2023-01-15'),
    (1002, 'Bob Smith', 'bob@example.com', 'Silver', 'USA', 'NY', 'New York', '2023-03-20'),
    (1003, 'Carol White', 'carol@example.com', 'Platinum', 'USA', 'TX', 'Austin', '2022-11-10'),
    (1004, 'David Brown', 'david@example.com', 'Bronze', 'Canada', 'ON', 'Toronto', '2024-01-05'),
    (1005, 'Emma Davis', 'emma@example.com', 'Gold', 'UK', 'England', 'London', '2023-06-18');

INSERT INTO suppliers (supplier_id, supplier_name, country, rating) VALUES
    (1, 'TechSupply Inc', 'China', 4.5),
    (2, 'HomeGoods Co', 'USA', 4.8),
    (3, 'Paper Masters', 'Canada', 4.2);

-- Validation
SELECT '[OK] Dimensions' AS step;
SELECT
    (SELECT count() FROM products) AS products,
    (SELECT count() FROM customers) AS customers,
    (SELECT count() FROM suppliers) AS suppliers;

SELECT '[OK] Sample products' AS step;
SELECT product_id, product_name, category, price FROM products LIMIT 3;

SELECT '[OK] Sample customers' AS step;
SELECT customer_id, customer_name, customer_tier FROM customers LIMIT 3;

SELECT '[OK] Sample suppliers' AS step;
SELECT supplier_id, supplier_name, country FROM suppliers LIMIT 3;
