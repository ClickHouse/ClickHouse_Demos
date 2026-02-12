"""
Telco Data Generator
Generates realistic telecommunications data for marketing analytics and network observability.

Memory-efficient: generates and inserts data in streaming batches to avoid OOM with large datasets.
"""

import os
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
import time

import clickhouse_connect
from faker import Faker
import numpy as np


class TelcoDataGenerator:
    """Generates realistic telco data patterns."""

    def __init__(self, seed: int = 42):
        """Initialize the data generator with a seed for reproducibility."""
        random.seed(seed)
        np.random.seed(seed)
        self.faker = Faker()
        Faker.seed(seed)

        # Customer segments
        self.segments = [
            "heavy_data_streamer",
            "voice_centric",
            "night_surfer",
            "low_usage",
            "hybrid_power_user"
        ]

        # Network event types
        self.event_types = [
            "call_drop",
            "data_session_start",
            "data_session_end",
            "sms_sent",
            "sms_received",
            "network_handover",
            "bandwidth_spike",
            "latency_increase",
            "packet_loss"
        ]

        # Device types
        self.device_types = [
            "iPhone 15 Pro",
            "Samsung Galaxy S24",
            "Google Pixel 8",
            "OnePlus 12",
            "Xiaomi 14",
            "iPhone 14",
            "Samsung Galaxy A54"
        ]

        # Plan types
        self.plan_types = [
            "prepaid_basic",
            "prepaid_unlimited",
            "postpaid_5gb",
            "postpaid_20gb",
            "postpaid_unlimited",
            "enterprise"
        ]

        # Base stations (cell towers)
        self.base_stations = self._generate_base_stations(100)

    def _generate_base_stations(self, count: int) -> List[Dict[str, Any]]:
        """Generate base station metadata."""
        stations = []
        for i in range(count):
            stations.append({
                "station_id": f"BS{i:05d}",
                "latitude": round(random.uniform(25.0, 49.0), 6),
                "longitude": round(random.uniform(-125.0, -65.0), 6),
                "capacity_mbps": random.choice([100, 500, 1000, 5000]),
                "technology": random.choice(["4G", "5G"]),
                "region": random.choice(["north", "south", "east", "west", "central"])
            })
        return stations

    def generate_customers(self, count: int) -> List[Dict[str, Any]]:
        """Generate customer profiles."""
        customers = []
        for i in range(count):
            segment = random.choice(self.segments)
            signup_date = self.faker.date_between(start_date="-3y", end_date="-1d")

            # Churn probability varies by segment
            churn_probability = {
                "heavy_data_streamer": 0.05,
                "voice_centric": 0.15,
                "night_surfer": 0.10,
                "low_usage": 0.30,
                "hybrid_power_user": 0.03
            }[segment]

            customer = {
                "customer_id": str(uuid.uuid4()),
                "email": self.faker.email(),
                "phone_number": self.faker.phone_number(),
                "first_name": self.faker.first_name(),
                "last_name": self.faker.last_name(),
                "age": random.randint(18, 75),
                "gender": random.choice(["M", "F", "Other"]),
                "address": self.faker.address().replace("\n", ", "),
                "city": self.faker.city(),
                "state": self.faker.state_abbr(),
                "zip_code": self.faker.zipcode(),
                "signup_date": signup_date,
                "plan_type": random.choice(self.plan_types),
                "device_type": random.choice(self.device_types),
                "segment": segment,
                "monthly_spend": round(random.uniform(20, 200), 2),
                "lifetime_value": round(random.uniform(500, 10000), 2),
                "churn_probability": round(churn_probability + random.uniform(-0.05, 0.05), 3),
                "is_churned": False,
                "created_at": datetime.utcnow()
            }
            customers.append(customer)
        return customers

    def generate_cdrs_for_customers(
        self,
        customers: List[Dict[str, Any]],
        days: int = 30,
    ) -> List[Dict[str, Any]]:
        """Generate call detail records for a batch of customers.

        This is called with small customer batches (e.g. 500) to keep memory bounded.
        Returns the CDR list for immediate insertion.
        """
        cdrs = []
        start_date = datetime.utcnow() - timedelta(days=days)

        for customer in customers:
            segment = customer["segment"]

            # Usage patterns vary by segment
            if segment == "heavy_data_streamer":
                data_sessions_per_day = random.randint(20, 50)
                voice_calls_per_day = random.randint(1, 5)
            elif segment == "voice_centric":
                data_sessions_per_day = random.randint(2, 10)
                voice_calls_per_day = random.randint(10, 30)
            elif segment == "night_surfer":
                data_sessions_per_day = random.randint(5, 15)
                voice_calls_per_day = random.randint(2, 8)
            elif segment == "low_usage":
                data_sessions_per_day = random.randint(1, 5)
                voice_calls_per_day = random.randint(1, 5)
            else:  # hybrid_power_user
                data_sessions_per_day = random.randint(15, 40)
                voice_calls_per_day = random.randint(10, 25)

            for day in range(days):
                current_date = start_date + timedelta(days=day)

                # Generate data sessions
                for _ in range(data_sessions_per_day):
                    if segment == "night_surfer":
                        hour = random.choice(list(range(22, 24)) + list(range(0, 6)))
                    else:
                        hour = random.randint(6, 23)

                    timestamp = current_date.replace(
                        hour=hour,
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    )

                    duration_minutes = random.randint(1, 120)
                    data_mb = round(random.uniform(10, 500), 2)

                    cdr = {
                        "cdr_id": str(uuid.uuid4()),
                        "customer_id": customer["customer_id"],
                        "timestamp": timestamp,
                        "event_type": "data_session",
                        "duration_seconds": duration_minutes * 60,
                        "data_mb": data_mb,
                        "base_station_id": random.choice(self.base_stations)["station_id"],
                        "cost": round(data_mb * 0.01, 2),
                        "created_at": datetime.utcnow()
                    }
                    cdrs.append(cdr)

                # Generate voice calls
                for _ in range(voice_calls_per_day):
                    hour = random.randint(8, 22)
                    timestamp = current_date.replace(
                        hour=hour,
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    )

                    duration_minutes = random.randint(1, 45)

                    cdr = {
                        "cdr_id": str(uuid.uuid4()),
                        "customer_id": customer["customer_id"],
                        "timestamp": timestamp,
                        "event_type": "voice_call",
                        "duration_seconds": duration_minutes * 60,
                        "data_mb": 0,
                        "base_station_id": random.choice(self.base_stations)["station_id"],
                        "cost": round(duration_minutes * 0.05, 2),
                        "created_at": datetime.utcnow()
                    }
                    cdrs.append(cdr)

        return cdrs

    # Keep the old signature for test compatibility
    def generate_call_detail_records(
        self,
        customers: List[Dict[str, Any]],
        days: int = 30,
        records_per_customer_per_day: int = 10
    ) -> List[Dict[str, Any]]:
        """Generate call detail records (CDRs). Thin wrapper for test compatibility."""
        return self.generate_cdrs_for_customers(customers, days=days)

    def generate_network_events_for_day(
        self,
        day_offset: int,
        total_days: int,
        events_per_day: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Generate network events for a single day.

        Called in a loop to keep memory bounded.
        """
        events = []
        start_date = datetime.utcnow() - timedelta(days=total_days)
        current_date = start_date + timedelta(days=day_offset)

        for _ in range(events_per_day):
            hour = random.randint(0, 23)
            timestamp = current_date.replace(
                hour=hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )

            event_type = random.choice(self.event_types)
            base_station = random.choice(self.base_stations)

            is_peak_hour = 9 <= hour <= 17
            is_anomaly = False

            if is_peak_hour and random.random() < 0.05:
                is_anomaly = True

            if event_type == "bandwidth_spike":
                bandwidth_mbps = random.uniform(800, 1200) if is_anomaly else random.uniform(50, 400)
                latency_ms = random.uniform(10, 100)
                packet_loss_pct = random.uniform(0, 2)
                severity = "high" if is_anomaly else "low"
            elif event_type == "latency_increase":
                bandwidth_mbps = random.uniform(50, 400)
                latency_ms = random.uniform(200, 500) if is_anomaly else random.uniform(10, 100)
                packet_loss_pct = random.uniform(0, 2)
                severity = "high" if is_anomaly else "low"
            elif event_type == "packet_loss":
                bandwidth_mbps = random.uniform(50, 400)
                latency_ms = random.uniform(10, 100)
                packet_loss_pct = random.uniform(5, 20) if is_anomaly else random.uniform(0, 2)
                severity = "high" if is_anomaly else "low"
            else:
                bandwidth_mbps = random.uniform(50, 400)
                latency_ms = random.uniform(10, 100)
                packet_loss_pct = random.uniform(0, 2)
                severity = "low"

            event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": timestamp,
                "event_type": event_type,
                "base_station_id": base_station["station_id"],
                "region": base_station["region"],
                "technology": base_station["technology"],
                "bandwidth_mbps": round(bandwidth_mbps, 2),
                "latency_ms": round(latency_ms, 2),
                "packet_loss_pct": round(packet_loss_pct, 3),
                "severity": severity,
                "is_anomaly": is_anomaly,
                "created_at": datetime.utcnow()
            }
            events.append(event)

        return events

    # Keep old signature for test compatibility
    def generate_network_events(
        self,
        days: int = 30,
        events_per_day: int = 1000
    ) -> List[Dict[str, Any]]:
        """Generate network events. Thin wrapper for test compatibility."""
        all_events = []
        for day in range(days):
            all_events.extend(
                self.generate_network_events_for_day(day, days, events_per_day)
            )
        return all_events

    def generate_marketing_campaigns(self, count: int = 10) -> List[Dict[str, Any]]:
        """Generate marketing campaign data."""
        campaigns = []
        campaign_types = [
            "churn_prevention",
            "upsell_data_plan",
            "device_upgrade",
            "seasonal_promotion",
            "referral_bonus"
        ]

        for i in range(count):
            start_date = self.faker.date_between(start_date="-90d", end_date="-30d")
            end_date = start_date + timedelta(days=random.randint(7, 30))

            campaign = {
                "campaign_id": str(uuid.uuid4()),
                "campaign_name": f"{random.choice(campaign_types).replace('_', ' ').title()} Q{random.randint(1,4)} 2024",
                "campaign_type": random.choice(campaign_types),
                "start_date": start_date,
                "end_date": end_date,
                "target_segment": random.choice(self.segments),
                "channel": random.choice(["email", "sms", "app_notification", "call"]),
                "budget": round(random.uniform(10000, 100000), 2),
                "impressions": random.randint(10000, 100000),
                "clicks": random.randint(500, 10000),
                "conversions": random.randint(50, 1000),
                "revenue_generated": round(random.uniform(5000, 50000), 2),
                "created_at": datetime.utcnow()
            }
            campaigns.append(campaign)

        return campaigns


def insert_to_clickhouse(client, table: str, data: List[Dict[str, Any]], batch_size: int = 1000):
    """Insert data into ClickHouse in batches using pandas DataFrame."""
    import pandas as pd

    if not data:
        return

    df = pd.DataFrame(data)

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        client.insert_df(table, batch)

    return len(data)


def get_data_size_profile(size: str) -> Dict[str, int]:
    """Return preset data volume profiles for t-shirt sizing."""
    profiles = {
        "small": {
            "num_customers": 1000,
            "num_days": 7,
            "num_campaigns": 10,
            "events_per_day": 500,
        },
        "medium": {
            "num_customers": 10000,
            "num_days": 30,
            "num_campaigns": 100,
            "events_per_day": 10000,
        },
        "large": {
            "num_customers": 50000,
            "num_days": 60,
            "num_campaigns": 500,
            "events_per_day": 25000,
        },
        "2xl": {
            "num_customers": 100000,
            "num_days": 90,
            "num_campaigns": 1000,
            "events_per_day": 50000,
        },
    }
    if size not in profiles:
        valid = ", ".join(profiles.keys())
        raise ValueError(f"Invalid DATA_SIZE '{size}'. Valid options: {valid}")
    return profiles[size]


def main():
    """Main execution function.

    Uses streaming inserts: generates data in bounded chunks and inserts immediately,
    so memory usage stays constant regardless of total data volume.
    """
    # Load configuration from environment
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_http_port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8443"))
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    clickhouse_secure = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"

    # Data volume configuration: DATA_SIZE overrides individual variables
    data_size = os.getenv("DATA_SIZE", "").strip().lower()
    if data_size:
        profile = get_data_size_profile(data_size)
        num_customers = profile["num_customers"]
        num_days = profile["num_days"]
        num_campaigns = profile["num_campaigns"]
        events_per_day = profile["events_per_day"]
    else:
        num_customers = int(os.getenv("NUM_CUSTOMERS", "10000"))
        num_days = int(os.getenv("NUM_DAYS", "30"))
        num_campaigns = int(os.getenv("NUM_CAMPAIGNS", "100"))
        events_per_day = int(os.getenv("EVENTS_PER_DAY", "10000"))

    # Configurable seed for reproducibility
    seed = int(os.getenv("DATA_SEED", "42"))

    # Dataset selection: all, network, or marketing
    generate_datasets = os.getenv("GENERATE_DATASETS", "all").strip().lower()
    valid_datasets = ("all", "network", "marketing")
    if generate_datasets not in valid_datasets:
        raise ValueError(
            f"Invalid GENERATE_DATASETS '{generate_datasets}'. "
            f"Valid options: {', '.join(valid_datasets)}"
        )

    # Adaptive batch size for ClickHouse inserts
    total_estimated_rows = (num_customers * num_days * 10) + (num_days * events_per_day)
    if total_estimated_rows > 1_000_000:
        batch_size = 10000
    else:
        batch_size = 1000

    # Customer chunk size: how many customers to generate CDRs for at a time.
    # Each customer generates ~40 CDRs/day, so 500 customers * 30 days = ~600K records per chunk.
    customer_chunk_size = 500

    print("=" * 60)
    print("Telco Data Generator")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  ClickHouse Host: {clickhouse_host}:{clickhouse_http_port}")
    if data_size:
        print(f"  Data Size Preset: {data_size}")
    print(f"  Number of Customers: {num_customers}")
    print(f"  Days of Data: {num_days}")
    print(f"  Marketing Campaigns: {num_campaigns}")
    print(f"  Network Events per Day: {events_per_day}")
    print(f"  Datasets: {generate_datasets}")
    print(f"  Seed: {seed}")
    print(f"  Batch Size: {batch_size}")
    print(f"  Customer Chunk Size: {customer_chunk_size}")
    print("=" * 60)

    # Connect to ClickHouse
    print("\nConnecting to ClickHouse...")
    connect_kwargs = dict(
        host=clickhouse_host,
        port=clickhouse_http_port,
        username=clickhouse_user,
        password=clickhouse_password,
    )
    if clickhouse_secure:
        connect_kwargs["secure"] = True
    client = clickhouse_connect.get_client(**connect_kwargs)
    print("[OK] Connected to ClickHouse")

    # Initialize generator
    print("\nInitializing data generator...")
    generator = TelcoDataGenerator(seed=seed)
    print("[OK] Generator initialized")

    total_customers = 0
    total_cdrs = 0
    total_events = 0
    total_campaigns = 0

    # -- Customers + CDRs + Campaigns --
    if generate_datasets in ("all", "marketing"):
        # Generate all customers first (10K customers = ~5MB, fits in memory)
        print("\nGenerating customer data...")
        customers = generator.generate_customers(num_customers)
        insert_to_clickhouse(client, "telco.customers", customers, batch_size=batch_size)
        total_customers = len(customers)
        print(f"[OK] Inserted {total_customers} customers")

        # Stream CDRs in customer chunks to avoid OOM
        print(f"\nGenerating call detail records (streaming in chunks of {customer_chunk_size} customers)...")
        num_chunks = (num_customers + customer_chunk_size - 1) // customer_chunk_size
        for chunk_idx in range(0, num_customers, customer_chunk_size):
            chunk_end = min(chunk_idx + customer_chunk_size, num_customers)
            customer_chunk = customers[chunk_idx:chunk_end]
            chunk_num = chunk_idx // customer_chunk_size + 1

            cdrs = generator.generate_cdrs_for_customers(customer_chunk, days=num_days)
            inserted = insert_to_clickhouse(client, "telco.call_detail_records", cdrs, batch_size=batch_size)
            total_cdrs += len(cdrs)
            print(f"  Chunk {chunk_num}/{num_chunks}: {len(cdrs)} CDRs inserted ({chunk_end - chunk_idx} customers)")

            # Free memory immediately
            del cdrs

        # Free customer list (no longer needed for CDRs)
        del customers
        print(f"[OK] Inserted {total_cdrs} CDRs total")

        # Marketing campaigns (always small, no streaming needed)
        print("\nGenerating marketing campaigns...")
        campaigns = generator.generate_marketing_campaigns(num_campaigns)
        insert_to_clickhouse(client, "telco.marketing_campaigns", campaigns, batch_size=batch_size)
        total_campaigns = len(campaigns)
        del campaigns
        print(f"[OK] Inserted {total_campaigns} campaigns")

    # -- Network Events --
    if generate_datasets in ("all", "network"):
        # Stream network events day-by-day to avoid OOM
        print(f"\nGenerating network events (streaming day-by-day, {events_per_day}/day)...")
        for day in range(num_days):
            events = generator.generate_network_events_for_day(day, num_days, events_per_day)
            insert_to_clickhouse(client, "telco.network_events", events, batch_size=batch_size)
            total_events += len(events)
            if (day + 1) % 10 == 0 or day == num_days - 1:
                print(f"  Day {day + 1}/{num_days}: {total_events} events total")
            del events

        print(f"[OK] Inserted {total_events} network events total")

    # Print summary
    print("\n" + "=" * 60)
    print("Data Generation Complete!")
    print("=" * 60)
    if total_customers:
        print(f"Total Customers: {total_customers}")
    if total_cdrs:
        print(f"Total CDRs: {total_cdrs}")
    if total_events:
        print(f"Total Network Events: {total_events}")
    if total_campaigns:
        print(f"Total Marketing Campaigns: {total_campaigns}")
    print("=" * 60)

    # Close connection
    client.close()


if __name__ == "__main__":
    main()
