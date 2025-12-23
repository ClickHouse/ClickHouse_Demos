#!/usr/bin/env python3
"""
FastMart Demo: Generate Dimension Data
Purpose: Create realistic products, customers, and suppliers
Usage: python generate_dimensions.py [--products 1000] [--customers 10000]
"""

import os
import sys
import clickhouse_connect
from faker import Faker
from tqdm import tqdm
import random

# Configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'fastmart_demo')
CLICKHOUSE_SECURE = os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true'

fake = Faker()

# Product catalog data
CATEGORIES = ['Electronics', 'Home', 'Office', 'Sports', 'Books', 'Clothing', 'Toys', 'Garden']
BRANDS = {
    'Electronics': ['TechBrand', 'ElectroMax', 'GadgetPro', 'DigitalLife'],
    'Home': ['HomeBrand', 'LivingSpace', 'ComfortZone', 'CozyHome'],
    'Office': ['PaperCo', 'DeskMaster', 'OfficeEssentials', 'WorkPro'],
    'Sports': ['SportFit', 'ActiveGear', 'ProAthlete', 'FitLife'],
    'Books': ['ReadMore', 'BookWorm', 'PageTurner', 'LitHub'],
    'Clothing': ['FashionWear', 'StylePoint', 'TrendyLook', 'UrbanFit'],
    'Toys': ['PlayTime', 'KidJoy', 'FunFactory', 'HappyToys'],
    'Garden': ['GreenThumb', 'GardenPro', 'NatureLife', 'PlantCare']
}

PRODUCT_NAMES = {
    'Electronics': ['Wireless Mouse', 'USB-C Cable', 'Keyboard', 'Monitor', 'Webcam', 'Headphones'],
    'Home': ['Coffee Mug', 'Desk Lamp', 'Throw Pillow', 'Wall Clock', 'Photo Frame'],
    'Office': ['Notebook Pack', 'Pen Set', 'Stapler', 'File Folder', 'Desk Organizer'],
    'Sports': ['Yoga Mat', 'Water Bottle', 'Resistance Bands', 'Jump Rope', 'Dumbbell Set'],
    'Books': ['Fiction Novel', 'Business Guide', 'Cookbook', 'Self-Help Book', 'Biography'],
    'Clothing': ['T-Shirt', 'Jeans', 'Hoodie', 'Sneakers', 'Baseball Cap'],
    'Toys': ['Building Blocks', 'Puzzle Set', 'Action Figure', 'Board Game', 'Plush Toy'],
    'Garden': ['Plant Pot', 'Garden Tools', 'Seeds Pack', 'Watering Can', 'Fertilizer']
}

CUSTOMER_TIERS = ['Bronze', 'Silver', 'Gold', 'Platinum']
COUNTRIES = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'Japan']

def connect_clickhouse():
    """Connect to ClickHouse"""
    protocol = "https" if CLICKHOUSE_SECURE else "http"
    print(f"Connecting to ClickHouse at {protocol}://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        secure=CLICKHOUSE_SECURE
    )
    print("Connected successfully!")
    return client

def generate_products(client, count=1000):
    """Generate product catalog"""
    print(f"\nGenerating {count} products...")

    products = []
    product_id = 1

    for category in CATEGORIES:
        products_per_category = count // len(CATEGORIES)
        brands = BRANDS[category]
        product_templates = PRODUCT_NAMES[category]

        for i in range(products_per_category):
            brand = random.choice(brands)
            template = random.choice(product_templates)

            # Generate price and cost
            base_price = random.uniform(9.99, 199.99)
            cost = base_price * random.uniform(0.4, 0.7)  # 30-60% margin

            product_name = f"{template} {fake.color_name()}" if random.random() > 0.5 else template

            products.append([
                product_id,
                product_name,
                category,
                brand,
                round(base_price, 2),
                round(cost, 2),
                random.randint(1, 20)  # supplier_id
            ])
            product_id += 1

    # Insert in batches
    batch_size = 1000
    for i in tqdm(range(0, len(products), batch_size), desc="Inserting products"):
        batch = products[i:i+batch_size]
        client.insert('products', batch,
                     column_names=['product_id', 'product_name', 'category', 'brand',
                                  'price', 'cost', 'supplier_id'])

    print(f"Inserted {len(products)} products")

def generate_customers(client, count=10000):
    """Generate customer profiles"""
    print(f"\nGenerating {count} customers...")

    customers = []

    for i in tqdm(range(1, count + 1), desc="Generating customers"):
        country = random.choice(COUNTRIES)
        tier_weights = [40, 30, 20, 10]  # Bronze, Silver, Gold, Platinum distribution
        tier = random.choices(CUSTOMER_TIERS, weights=tier_weights)[0]

        # Lifetime value correlates with tier
        ltv_base = {'Bronze': 100, 'Silver': 500, 'Gold': 2000, 'Platinum': 10000}
        lifetime_value = ltv_base[tier] * random.uniform(0.5, 2.0)

        customers.append([
            i + 1000,  # Start from 1001
            fake.name(),
            fake.email(),
            tier,
            country,
            fake.state() if country in ['USA', 'Canada'] else '',
            fake.city(),
            fake.date_between(start_date='-3y', end_date='today'),
            round(lifetime_value, 2)
        ])

    # Insert in batches
    batch_size = 1000
    for i in tqdm(range(0, len(customers), batch_size), desc="Inserting customers"):
        batch = customers[i:i+batch_size]
        client.insert('customers', batch,
                     column_names=['customer_id', 'customer_name', 'email', 'customer_tier',
                                  'country', 'state', 'city', 'signup_date', 'lifetime_value'])

    print(f"Inserted {len(customers)} customers")

def generate_suppliers(client, count=20):
    """Generate supplier data"""
    print(f"\nGenerating {count} suppliers...")

    suppliers = []
    supplier_countries = ['USA', 'China', 'Germany', 'Japan', 'Taiwan', 'Korea', 'Mexico']

    for i in range(1, count + 1):
        suppliers.append([
            i,
            fake.company(),
            random.choice(supplier_countries),
            round(random.uniform(3.5, 5.0), 1)  # rating
        ])

    client.insert('suppliers', suppliers,
                 column_names=['supplier_id', 'supplier_name', 'country', 'rating'])

    print(f"Inserted {count} suppliers")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Generate dimension data for FastMart demo')
    parser.add_argument('--products', type=int, default=1000, help='Number of products')
    parser.add_argument('--customers', type=int, default=10000, help='Number of customers')
    parser.add_argument('--suppliers', type=int, default=20, help='Number of suppliers')

    args = parser.parse_args()

    try:
        client = connect_clickhouse()

        generate_suppliers(client, args.suppliers)
        generate_products(client, args.products)
        generate_customers(client, args.customers)

        print("\n" + "="*60)
        print("Dimension data generation complete!")
        print("="*60)

        # Verify counts
        product_count = client.query("SELECT count() FROM products").result_rows[0][0]
        customer_count = client.query("SELECT count() FROM customers").result_rows[0][0]
        supplier_count = client.query("SELECT count() FROM suppliers").result_rows[0][0]

        print(f"Products: {product_count}")
        print(f"Customers: {customer_count}")
        print(f"Suppliers: {supplier_count}")
        print("\nReady to generate events!")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
