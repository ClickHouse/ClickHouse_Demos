#!/usr/bin/env python3
"""
FastMart Demo: Generate Historical Events
Purpose: Create batch of historical events for testing
Usage: python generate_events.py --count 1000000
"""

import os
import sys
import json
import clickhouse_connect
from faker import Faker
from tqdm import tqdm
import random
from datetime import datetime, timedelta
import uuid

# Configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'fastmart_demo')
CLICKHOUSE_SECURE = os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true'

fake = Faker()

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

def get_dimensions(client):
    """Fetch dimension data for realistic references"""
    print("Fetching dimension data...")

    products = client.query("SELECT product_id, price FROM products").result_rows
    customers = client.query("SELECT customer_id FROM customers").result_rows

    print(f"Loaded {len(products)} products and {len(customers)} customers")

    return {
        'products': [(p[0], p[1]) for p in products],
        'customers': [c[0] for c in customers]
    }

def generate_order_event(dimensions, event_time):
    """Generate a realistic order event"""
    product_id, price = random.choice(dimensions['products'])
    customer_id = random.choice(dimensions['customers'])

    # Quantity follows realistic distribution
    quantity = random.choices(
        [1, 2, 3, 4, 5, 10, 20],
        weights=[50, 20, 10, 8, 5, 4, 3]
    )[0]

    # Payment methods
    payment_method = random.choices(
        ['credit_card', 'paypal', 'debit_card', 'apple_pay'],
        weights=[50, 25, 20, 5]
    )[0]

    payload = {
        'order_id': str(uuid.uuid4()),
        'customer_id': customer_id,
        'product_id': product_id,
        'quantity': quantity,
        'price': float(price),
        'payment_method': payment_method
    }

    return [
        str(uuid.uuid4()),
        event_time,
        'order',
        random.choice(['web', 'mobile', 'api']),
        json.dumps(payload)
    ]

def generate_click_event(dimensions, event_time):
    """Generate a clickstream event"""
    customer_id = random.choice(dimensions['customers'])
    product_id, _ = random.choice(dimensions['products'])

    actions = ['view', 'add_to_cart', 'remove', 'checkout']
    pages = ['/products', '/cart', '/checkout', '/account']

    payload = {
        'session_id': f"sess_{fake.uuid4()[:8]}",
        'customer_id': customer_id,
        'page': random.choice(pages),
        'action': random.choice(actions),
        'product_id': product_id,
        'duration_seconds': random.randint(5, 300)
    }

    return [
        str(uuid.uuid4()),
        event_time,
        'click',
        random.choice(['web', 'mobile']),
        json.dumps(payload)
    ]

def generate_inventory_event(dimensions, event_time):
    """Generate an inventory update event"""
    product_id, _ = random.choice(dimensions['products'])

    reasons = ['order_fulfillment', 'restock', 'adjustment', 'return']
    quantity_change = random.choice([-1, -2, -3, -5, 50, 100, 200])

    payload = {
        'product_id': product_id,
        'warehouse_id': random.randint(100, 110),
        'quantity_change': quantity_change,
        'new_stock_level': random.randint(0, 500),
        'reason': random.choice(reasons)
    }

    return [
        str(uuid.uuid4()),
        event_time,
        'inventory_update',
        'batch',
        json.dumps(payload)
    ]

def generate_events(client, dimensions, count=100000, days_back=30):
    """Generate historical events"""
    print(f"\nGenerating {count:,} events over {days_back} days...")

    # Event type distribution: 60% orders, 35% clicks, 5% inventory
    event_types = ['order', 'click', 'inventory']
    event_weights = [60, 35, 5]

    events = []
    batch_size = 10000

    start_time = datetime.now() - timedelta(days=days_back)

    for i in tqdm(range(count), desc="Generating events"):
        # Random time within the period
        event_time = start_time + timedelta(
            seconds=random.randint(0, days_back * 24 * 3600)
        )

        event_type = random.choices(event_types, weights=event_weights)[0]

        if event_type == 'order':
            event = generate_order_event(dimensions, event_time)
        elif event_type == 'click':
            event = generate_click_event(dimensions, event_time)
        else:
            event = generate_inventory_event(dimensions, event_time)

        events.append(event)

        # Insert batch
        if len(events) >= batch_size:
            client.insert('events_raw', events,
                         column_names=['event_id', 'event_time', 'event_type',
                                      'source_system', 'payload'])
            events = []

    # Insert remaining events
    if events:
        client.insert('events_raw', events,
                     column_names=['event_id', 'event_time', 'event_type',
                                  'source_system', 'payload'])

    print(f"\nInserted {count:,} events")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Generate historical events for FastMart demo')
    parser.add_argument('--count', type=int, default=100000,
                       help='Number of events to generate')
    parser.add_argument('--days', type=int, default=30,
                       help='Spread events over this many days')

    args = parser.parse_args()

    try:
        client = connect_clickhouse()
        dimensions = get_dimensions(client)

        generate_events(client, dimensions, args.count, args.days)

        print("\n" + "="*60)
        print("Event generation complete!")
        print("="*60)

        # Verify counts
        event_count = client.query("SELECT count() FROM events_raw").result_rows[0][0]
        order_count = client.query(
            "SELECT count() FROM events_raw WHERE event_type = 'order'"
        ).result_rows[0][0]

        print(f"Total events: {event_count:,}")
        print(f"Order events: {order_count:,}")
        print("\nRun sql/queries/40_validation.sql to verify pipeline!")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
