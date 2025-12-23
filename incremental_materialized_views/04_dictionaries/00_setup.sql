-- ================================================
-- Example 4: Dictionaries for Fast Lookups
-- ================================================
-- Scenario: E-commerce order enrichment
-- Key Concept: Dictionaries provide O(1) lookups, replacing expensive JOINs
-- ================================================

-- Create database
CREATE DATABASE IF NOT EXISTS mv_demo_dictionaries;
USE mv_demo_dictionaries;

-- ================================================
-- Dimension Tables: Products and Customers
-- ================================================
-- These are the source tables for our dictionaries

DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products (
    product_id UInt32,
    product_name String,
    category String,
    brand String,
    unit_price Decimal64(2),
    unit_cost Decimal64(2)
)
ENGINE = MergeTree()
ORDER BY product_id;

DROP TABLE IF EXISTS dim_customers;
CREATE TABLE dim_customers (
    customer_id UInt32,
    customer_name String,
    tier String,           -- 'Bronze', 'Silver', 'Gold', 'Platinum'
    country String,
    city String,
    signup_date Date
)
ENGINE = MergeTree()
ORDER BY customer_id;

-- ================================================
-- Fact Table: Raw Orders
-- ================================================
DROP TABLE IF EXISTS orders_raw;
CREATE TABLE orders_raw (
    order_id UUID DEFAULT generateUUIDv4(),
    customer_id UInt32,
    product_id UInt32,
    quantity UInt32,
    order_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (order_time, order_id);

-- ================================================
-- Insert Sample Dimension Data
-- ================================================

-- Products
INSERT INTO dim_products (product_id, product_name, category, brand, unit_price, unit_cost) VALUES
    (1, 'Laptop Pro 15', 'Electronics', 'TechBrand', 1299.99, 850.00),
    (2, 'Wireless Mouse', 'Electronics', 'TechBrand', 49.99, 20.00),
    (3, 'USB-C Hub', 'Electronics', 'ConnectPro', 79.99, 35.00),
    (4, 'Mechanical Keyboard', 'Electronics', 'KeyMaster', 149.99, 75.00),
    (5, 'Monitor 27"', 'Electronics', 'ViewMax', 399.99, 250.00),
    (6, 'Desk Chair', 'Furniture', 'ComfortPlus', 299.99, 150.00),
    (7, 'Standing Desk', 'Furniture', 'ErgoDesk', 599.99, 350.00),
    (8, 'Webcam HD', 'Electronics', 'StreamPro', 89.99, 40.00),
    (9, 'Headphones Pro', 'Electronics', 'AudioMax', 249.99, 120.00),
    (10, 'Laptop Stand', 'Accessories', 'ErgoDesk', 59.99, 25.00);

-- Customers
INSERT INTO dim_customers (customer_id, customer_name, tier, country, city, signup_date) VALUES
    (1001, 'Alice Johnson', 'Gold', 'USA', 'New York', '2022-01-15'),
    (1002, 'Bob Smith', 'Silver', 'USA', 'Los Angeles', '2022-03-20'),
    (1003, 'Carol White', 'Platinum', 'UK', 'London', '2021-06-10'),
    (1004, 'David Brown', 'Bronze', 'Canada', 'Toronto', '2023-02-28'),
    (1005, 'Eva Martinez', 'Gold', 'Spain', 'Madrid', '2022-08-05'),
    (1006, 'Frank Lee', 'Silver', 'USA', 'Chicago', '2023-01-12'),
    (1007, 'Grace Kim', 'Platinum', 'South Korea', 'Seoul', '2021-11-30'),
    (1008, 'Henry Chen', 'Bronze', 'China', 'Shanghai', '2023-04-18'),
    (1009, 'Iris Patel', 'Gold', 'India', 'Mumbai', '2022-05-22'),
    (1010, 'Jack Wilson', 'Silver', 'Australia', 'Sydney', '2022-09-14');

SELECT '[OK] Database and tables created' AS status;
SELECT '[OK] Dimension data inserted' AS status;

-- Verify
SELECT 'Products:' AS info, count() AS count FROM dim_products;
SELECT 'Customers:' AS info, count() AS count FROM dim_customers;
