#!/usr/bin/env python3
"""
FastMart Demo: Real-Time Event Streaming Simulator
Purpose: Continuously generate events for live demo
Usage: python stream_simulator.py --rate 1000
"""

import os
import sys
import json
import time
import signal
import clickhouse_connect
from faker import Faker
import random
from datetime import datetime
import uuid

# Configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'fastmart_demo')
CLICKHOUSE_SECURE = os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true'

fake = Faker()
running = True

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global running
    print("\n\nStopping simulator...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

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
    """Fetch dimension data"""
    products = client.query("SELECT product_id, price FROM products LIMIT 100").result_rows
    customers = client.query("SELECT customer_id FROM customers LIMIT 1000").result_rows

    return {
        'products': [(p[0], p[1]) for p in products],
        'customers': [c[0] for c in customers]
    }

def generate_event_batch(dimensions, batch_size=100):
    """Generate a batch of events"""
    events = []
    now = datetime.now()

    for _ in range(batch_size):
        event_type = random.choices(
            ['order', 'click', 'inventory'],
            weights=[60, 35, 5]
        )[0]

        if event_type == 'order':
            product_id, price = random.choice(dimensions['products'])
            customer_id = random.choice(dimensions['customers'])

            payload = {
                'order_id': str(uuid.uuid4()),
                'customer_id': customer_id,
                'product_id': product_id,
                'quantity': random.randint(1, 5),
                'price': float(price),
                'payment_method': random.choice(['credit_card', 'paypal', 'debit_card'])
            }

        elif event_type == 'click':
            customer_id = random.choice(dimensions['customers'])
            product_id, _ = random.choice(dimensions['products'])

            payload = {
                'session_id': f"sess_{fake.uuid4()[:8]}",
                'customer_id': customer_id,
                'page': random.choice(['/products', '/cart', '/checkout']),
                'action': random.choice(['view', 'add_to_cart']),
                'product_id': product_id,
                'duration_seconds': random.randint(5, 120)
            }

        else:  # inventory
            product_id, _ = random.choice(dimensions['products'])

            payload = {
                'product_id': product_id,
                'warehouse_id': random.randint(100, 105),
                'quantity_change': random.choice([-1, -2, 50, 100]),
                'new_stock_level': random.randint(0, 500),
                'reason': random.choice(['order_fulfillment', 'restock'])
            }

        events.append([
            str(uuid.uuid4()),
            now,
            event_type,
            random.choice(['web', 'mobile', 'api']),
            json.dumps(payload)
        ])

    return events

def simulate_stream(client, dimensions, events_per_second=100):
    """Simulate continuous event stream"""
    print(f"\nStarting event stream: {events_per_second} events/second")
    print("Press Ctrl+C to stop\n")

    batch_size = max(10, events_per_second // 10)  # 10 batches per second
    sleep_time = 1.0 / 10  # 100ms between batches

    total_events = 0
    start_time = time.time()

    try:
        while running:
            loop_start = time.time()

            # Generate and insert batch
            events = generate_event_batch(dimensions, batch_size)

            client.insert('events_raw', events,
                         column_names=['event_id', 'event_time', 'event_type',
                                      'source_system', 'payload'])

            total_events += len(events)

            # Calculate metrics
            elapsed = time.time() - start_time
            rate = total_events / elapsed if elapsed > 0 else 0

            # Print status
            print(f"\rTotal events: {total_events:,} | "
                  f"Rate: {rate:.1f}/sec | "
                  f"Elapsed: {elapsed:.1f}s", end='', flush=True)

            # Sleep to maintain rate
            loop_time = time.time() - loop_start
            if loop_time < sleep_time:
                time.sleep(sleep_time - loop_time)

    except Exception as e:
        print(f"\nError: {e}")

    print(f"\n\nStopped. Total events generated: {total_events:,}")

def generate_anomalies(client, dimensions, count=5):
    """Generate anomalous events for testing detection"""
    print(f"\nGenerating {count} anomalous events...")

    events = []
    now = datetime.now()

    for _ in range(count):
        # High-value order
        product_id, price = random.choice(dimensions['products'])
        customer_id = random.choice(dimensions['customers'])

        payload = {
            'order_id': str(uuid.uuid4()),
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': random.randint(50, 200),  # Unusually high
            'price': float(price),
            'payment_method': 'credit_card'
        }

        events.append([
            str(uuid.uuid4()),
            now,
            'order',
            'web',
            json.dumps(payload)
        ])

    client.insert('events_raw', events,
                 column_names=['event_id', 'event_time', 'event_type',
                              'source_system', 'payload'])

    print(f"Inserted {count} anomalous orders")
    print("Check order_anomalies table to see detections!")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Real-time event stream simulator')
    parser.add_argument('--rate', type=int, default=100,
                       help='Events per second')
    parser.add_argument('--anomalies', action='store_true',
                       help='Generate anomalous events instead of stream')
    parser.add_argument('--count', type=int, default=5,
                       help='Number of anomalies to generate')

    args = parser.parse_args()

    try:
        client = connect_clickhouse()
        dimensions = get_dimensions(client)

        if args.anomalies:
            generate_anomalies(client, dimensions, args.count)
        else:
            simulate_stream(client, dimensions, args.rate)

    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
