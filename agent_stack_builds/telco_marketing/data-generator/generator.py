"""
Telco Data Generator (vectorized, column-oriented).

Generates realistic telecommunications data for marketing analytics and
network observability. Uses numpy column-oriented arrays end-to-end and
inserts directly via clickhouse-connect's column-oriented insert (no pandas).
Peak memory is bounded by the per-chunk row count, which is small enough to
fit the 2GB data-generator container limit even at the 2xl preset.
"""

import os
import random
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import clickhouse_connect
import numpy as np
from faker import Faker


# ---------------------------------------------------------------------------
# Static enumerations
# ---------------------------------------------------------------------------
SEGMENTS = ["heavy_data_streamer", "voice_centric", "night_surfer",
            "low_usage", "hybrid_power_user"]

EVENT_TYPES = ["call_drop", "data_session_start", "data_session_end",
               "sms_sent", "sms_received", "network_handover",
               "bandwidth_spike", "latency_increase", "packet_loss"]

DEVICE_TYPES = ["iPhone 15 Pro", "Samsung Galaxy S24", "Google Pixel 8",
                "OnePlus 12", "Xiaomi 14", "iPhone 14", "Samsung Galaxy A54"]

PLAN_TYPES = ["prepaid_basic", "prepaid_unlimited", "postpaid_5gb",
              "postpaid_20gb", "postpaid_unlimited", "enterprise"]

GENDERS = ["M", "F", "Other"]
REGIONS = ["north", "south", "east", "west", "central"]
TECHNOLOGIES = ["4G", "5G"]

CAMPAIGN_TYPES = ["churn_prevention", "upsell_data_plan", "device_upgrade",
                  "seasonal_promotion", "referral_bonus"]
CHANNELS = ["email", "sms", "app_notification", "call"]

SEGMENT_CHURN = {
    "heavy_data_streamer": 0.05,
    "voice_centric": 0.15,
    "night_surfer": 0.10,
    "low_usage": 0.30,
    "hybrid_power_user": 0.03,
}

# (data_sessions_per_day_min, _max_excl, voice_calls_per_day_min, _max_excl)
SEGMENT_USAGE = {
    "heavy_data_streamer": (20, 51, 1, 6),
    "voice_centric": (2, 11, 10, 31),
    "night_surfer": (5, 16, 2, 9),
    "low_usage": (1, 6, 1, 6),
    "hybrid_power_user": (15, 41, 10, 26),
}

NIGHT_HOURS = np.array([22, 23, 0, 1, 2, 3, 4, 5], dtype=np.int32)


class TelcoDataGenerator:
    """Vectorized telco data generator."""

    def __init__(self, seed: int = 42, station_count: int = 100):
        random.seed(seed)
        np.random.seed(seed)
        self.rng = np.random.default_rng(seed)

        faker = Faker()
        Faker.seed(seed)

        # Pre-generate Faker pools once -- subsequent rows sample from these
        # arrays, avoiding ~8 slow Faker calls per customer.
        self.first_names = np.array([faker.first_name() for _ in range(500)])
        self.last_names = np.array([faker.last_name() for _ in range(500)])
        self.cities = np.array([faker.city() for _ in range(300)])
        self.state_abbrs = np.array([faker.state_abbr() for _ in range(60)])
        self.zips = np.array([faker.zipcode() for _ in range(200)])
        self.streets = np.array(
            [faker.street_address().replace("\n", ", ") for _ in range(500)]
        )

        # Base station metadata (small, kept in memory throughout).
        self.station_ids = np.array(
            [f"BS{i:05d}" for i in range(station_count)]
        )
        self.station_regions = self.rng.choice(REGIONS, station_count)
        self.station_techs = self.rng.choice(TECHNOLOGIES, station_count)

        # Rolling sequence counters give each row a unique ID without uuid4().
        self._customer_seq = 0
        self._cdr_seq = 0
        self._event_seq = 0

    # ---- ID helpers ----
    def _next_cdr_ids(self, n: int) -> np.ndarray:
        ids = np.array([f"cdr_{i:012d}" for i in range(self._cdr_seq, self._cdr_seq + n)])
        self._cdr_seq += n
        return ids

    def _next_event_ids(self, n: int) -> np.ndarray:
        ids = np.array([f"evt_{i:012d}" for i in range(self._event_seq, self._event_seq + n)])
        self._event_seq += n
        return ids

    # ---- Customers ----
    def generate_customers_columns(self, count: int) -> Tuple[List[str], List]:
        """Generate customers in column-oriented form. Returns (column_names, columns)."""
        rng = self.rng
        seq = np.arange(self._customer_seq, self._customer_seq + count)
        self._customer_seq += count

        customer_ids = np.array([f"cust_{i:08d}" for i in seq])
        first_names = rng.choice(self.first_names, count)
        last_names = rng.choice(self.last_names, count)
        emails = np.array([
            f"{f.lower()}.{l.lower()}{int(i) % 1000}@example.com"
            for f, l, i in zip(first_names, last_names, seq)
        ])
        # Phone parts vectorized, format string assembled per row.
        a = rng.integers(200, 1000, count)
        b = rng.integers(200, 1000, count)
        c = rng.integers(0, 10000, count)
        phones = np.array([
            f"({int(ai)}) {int(bi):03d}-{int(ci):04d}"
            for ai, bi, ci in zip(a, b, c)
        ])
        ages = rng.integers(18, 76, count).astype(np.uint8)
        genders = rng.choice(GENDERS, count)
        streets = rng.choice(self.streets, count)
        cities = rng.choice(self.cities, count)
        states = rng.choice(self.state_abbrs, count)
        zips = rng.choice(self.zips, count)
        addresses = np.array([
            f"{s}, {ct}, {st} {z}"
            for s, ct, st, z in zip(streets, cities, states, zips)
        ])

        today = datetime.utcnow().date()
        signup_offsets = rng.integers(1, 365 * 3 + 1, count)
        signup_dates = np.array(
            [today - timedelta(days=int(o)) for o in signup_offsets], dtype=object
        )

        plan_types = rng.choice(PLAN_TYPES, count)
        device_types = rng.choice(DEVICE_TYPES, count)
        segments = rng.choice(SEGMENTS, count)

        monthly_spend = np.round(rng.uniform(20, 200, count), 2)
        lifetime_value = np.round(rng.uniform(500, 10000, count), 2)
        churn_base = np.array([SEGMENT_CHURN[s] for s in segments])
        churn_probability = np.round(
            churn_base + rng.uniform(-0.05, 0.05, count), 3
        )
        is_churned = np.zeros(count, dtype=bool)

        now = datetime.utcnow().replace(microsecond=0)
        created_at = np.full(count, now, dtype=object)

        cols = [
            "customer_id", "email", "phone_number", "first_name", "last_name",
            "age", "gender", "address", "city", "state", "zip_code",
            "signup_date", "plan_type", "device_type", "segment",
            "monthly_spend", "lifetime_value", "churn_probability",
            "is_churned", "created_at",
        ]
        data = [
            customer_ids, emails, phones, first_names, last_names,
            ages, genders, addresses, cities, states, zips,
            signup_dates, plan_types, device_types, segments,
            monthly_spend, lifetime_value, churn_probability,
            is_churned, created_at,
        ]
        return cols, data

    # ---- Call detail records ----
    def generate_cdrs_columns(
        self,
        customer_ids: np.ndarray,
        customer_segments: np.ndarray,
        days: int,
    ) -> Tuple[List[str], List]:
        """Generate CDRs for a customer chunk in column-oriented form."""
        rng = self.rng
        base_date = datetime.utcnow().replace(microsecond=0) - timedelta(days=days)
        base_unix = int(base_date.timestamp())

        n_customers = len(customer_ids)
        data_per_day = np.empty(n_customers, dtype=np.int32)
        voice_per_day = np.empty(n_customers, dtype=np.int32)
        for i, seg in enumerate(customer_segments):
            dmin, dmax, vmin, vmax = SEGMENT_USAGE[seg]
            data_per_day[i] = int(rng.integers(dmin, dmax))
            voice_per_day[i] = int(rng.integers(vmin, vmax))

        data_rows_per_cust = data_per_day * days
        voice_rows_per_cust = voice_per_day * days
        total_data = int(data_rows_per_cust.sum())
        total_voice = int(voice_rows_per_cust.sum())
        total_rows = total_data + total_voice

        # Data sessions.
        data_cust_ids = np.repeat(customer_ids, data_rows_per_cust)
        data_segs = np.repeat(customer_segments, data_rows_per_cust)
        is_night = data_segs == "night_surfer"
        hours_normal = rng.integers(6, 24, total_data)
        hours_night = rng.choice(NIGHT_HOURS, total_data)
        data_hours = np.where(is_night, hours_night, hours_normal)
        day_off_d = rng.integers(0, days, total_data)
        min_d = rng.integers(0, 60, total_data)
        sec_d = rng.integers(0, 60, total_data)
        ts_data = (base_unix + day_off_d * 86400
                   + data_hours * 3600 + min_d * 60 + sec_d).astype(np.int64)

        dur_min_d = rng.integers(1, 121, total_data)
        duration_sec_d = (dur_min_d * 60).astype(np.uint32)
        data_mb = np.round(rng.uniform(10, 500, total_data), 2)
        station_d = rng.choice(self.station_ids, total_data)
        cost_d = np.round(data_mb * 0.01, 2)

        # Voice calls.
        voice_cust_ids = np.repeat(customer_ids, voice_rows_per_cust)
        hours_v = rng.integers(8, 23, total_voice)
        day_off_v = rng.integers(0, days, total_voice)
        min_v = rng.integers(0, 60, total_voice)
        sec_v = rng.integers(0, 60, total_voice)
        ts_voice = (base_unix + day_off_v * 86400
                    + hours_v * 3600 + min_v * 60 + sec_v).astype(np.int64)

        dur_min_v = rng.integers(1, 46, total_voice)
        duration_sec_v = (dur_min_v * 60).astype(np.uint32)
        data_mb_v = np.zeros(total_voice, dtype=np.float64)
        station_v = rng.choice(self.station_ids, total_voice)
        cost_v = np.round(dur_min_v * 0.05, 2)

        # Concatenate.
        cdr_ids = self._next_cdr_ids(total_rows)
        cust_ids = np.concatenate([data_cust_ids, voice_cust_ids])
        ts_unix = np.concatenate([ts_data, ts_voice])
        # clickhouse-connect's DateTime writer expects Python datetime objects,
        # not numpy.datetime64 scalars. tolist() does the conversion in C.
        timestamps = ts_unix.astype("datetime64[s]").tolist()
        event_types = np.concatenate([
            np.full(total_data, "data_session"),
            np.full(total_voice, "voice_call"),
        ])
        durations = np.concatenate([duration_sec_d, duration_sec_v])
        data_mbs = np.concatenate([data_mb, data_mb_v])
        stations = np.concatenate([station_d, station_v])
        costs = np.concatenate([cost_d, cost_v])
        now = datetime.utcnow().replace(microsecond=0)
        created_at = np.full(total_rows, now, dtype=object)

        cols = ["cdr_id", "customer_id", "timestamp", "event_type",
                "duration_seconds", "data_mb", "base_station_id", "cost",
                "created_at"]
        data = [cdr_ids, cust_ids, timestamps, event_types,
                durations, data_mbs, stations, costs, created_at]
        return cols, data

    # ---- Network events ----
    def generate_network_events_day_columns(
        self,
        day_offset: int,
        total_days: int,
        events_per_day: int,
    ) -> Tuple[List[str], List]:
        rng = self.rng
        base_date = datetime.utcnow().replace(microsecond=0) - timedelta(days=total_days)
        day_base = int(base_date.timestamp()) + day_offset * 86400

        n = events_per_day
        event_ids = self._next_event_ids(n)
        hours = rng.integers(0, 24, n)
        minutes = rng.integers(0, 60, n)
        secs = rng.integers(0, 60, n)
        ts_unix = (day_base + hours * 3600 + minutes * 60 + secs).astype(np.int64)
        timestamps = ts_unix.astype("datetime64[s]").tolist()

        event_type_idx = rng.integers(0, len(EVENT_TYPES), n)
        event_types = np.array(EVENT_TYPES)[event_type_idx]

        station_idx = rng.integers(0, len(self.station_ids), n)
        base_station = self.station_ids[station_idx]
        regions = self.station_regions[station_idx]
        techs = self.station_techs[station_idx]

        is_peak = (hours >= 9) & (hours <= 17)
        is_anomaly = is_peak & (rng.uniform(0, 1, n) < 0.05)

        # Default ranges; anomaly-specific event types overwrite per-row.
        bandwidth = rng.uniform(50, 400, n)
        latency = rng.uniform(10, 100, n)
        packet_loss = rng.uniform(0, 2, n)

        mask_bw = (event_types == "bandwidth_spike") & is_anomaly
        if mask_bw.any():
            bandwidth[mask_bw] = rng.uniform(800, 1200, int(mask_bw.sum()))
        mask_lat = (event_types == "latency_increase") & is_anomaly
        if mask_lat.any():
            latency[mask_lat] = rng.uniform(200, 500, int(mask_lat.sum()))
        mask_pl = (event_types == "packet_loss") & is_anomaly
        if mask_pl.any():
            packet_loss[mask_pl] = rng.uniform(5, 20, int(mask_pl.sum()))

        severity = np.where(is_anomaly, "high", "low")

        bandwidth = np.round(bandwidth, 2)
        latency = np.round(latency, 2)
        packet_loss = np.round(packet_loss, 3)

        now = datetime.utcnow().replace(microsecond=0)
        created_at = np.full(n, now, dtype=object)

        cols = ["event_id", "timestamp", "event_type", "base_station_id",
                "region", "technology", "bandwidth_mbps", "latency_ms",
                "packet_loss_pct", "severity", "is_anomaly", "created_at"]
        data = [event_ids, timestamps, event_types, base_station,
                regions, techs, bandwidth, latency, packet_loss,
                severity, is_anomaly, created_at]
        return cols, data

    # ---- Marketing campaigns ----
    def generate_campaigns_columns(self, count: int) -> Tuple[List[str], List]:
        rng = self.rng
        seq = np.arange(count)
        campaign_ids = np.array([f"camp_{i:06d}" for i in seq])
        types = rng.choice(CAMPAIGN_TYPES, count)
        quarters = rng.integers(1, 5, count)
        names = np.array([
            f"{t.replace('_', ' ').title()} Q{int(q)} 2024"
            for t, q in zip(types, quarters)
        ])

        today = datetime.utcnow().date()
        start_offsets = rng.integers(30, 91, count)
        start_dates = [today - timedelta(days=int(o)) for o in start_offsets]
        durations = rng.integers(7, 31, count)
        end_dates = [s + timedelta(days=int(d))
                     for s, d in zip(start_dates, durations)]
        start_dates_arr = np.array(start_dates, dtype=object)
        end_dates_arr = np.array(end_dates, dtype=object)

        target_segments = rng.choice(SEGMENTS, count)
        channels = rng.choice(CHANNELS, count)
        budgets = np.round(rng.uniform(10000, 100000, count), 2)
        impressions = rng.integers(10000, 100001, count).astype(np.uint32)
        clicks = rng.integers(500, 10001, count).astype(np.uint32)
        conversions = rng.integers(50, 1001, count).astype(np.uint32)
        revenue = np.round(rng.uniform(5000, 50000, count), 2)

        now = datetime.utcnow().replace(microsecond=0)
        created_at = np.full(count, now, dtype=object)

        cols = ["campaign_id", "campaign_name", "campaign_type",
                "start_date", "end_date", "target_segment", "channel",
                "budget", "impressions", "clicks", "conversions",
                "revenue_generated", "created_at"]
        data = [campaign_ids, names, types, start_dates_arr, end_dates_arr,
                target_segments, channels, budgets, impressions, clicks,
                conversions, revenue, created_at]
        return cols, data


def insert_columns(client, table: str, cols: List[str], data: List) -> int:
    """Insert column-oriented data directly via clickhouse-connect."""
    n = len(data[0]) if data else 0
    if n == 0:
        return 0
    client.insert(table, data, column_names=cols, column_oriented=True)
    return n


def get_data_size_profile(size: str) -> dict:
    """Return preset data volume profiles for t-shirt sizing."""
    profiles = {
        "small":  {"num_customers": 1000,   "num_days": 7,  "num_campaigns": 10,   "events_per_day": 500},
        "medium": {"num_customers": 10000,  "num_days": 30, "num_campaigns": 100,  "events_per_day": 10000},
        "large":  {"num_customers": 50000,  "num_days": 60, "num_campaigns": 500,  "events_per_day": 25000},
        "2xl":    {"num_customers": 100000, "num_days": 90, "num_campaigns": 1000, "events_per_day": 50000},
    }
    if size not in profiles:
        valid = ", ".join(profiles.keys())
        raise ValueError(f"Invalid DATA_SIZE '{size}'. Valid options: {valid}")
    return profiles[size]


def main():
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_http_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    clickhouse_secure = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"

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

    seed = int(os.getenv("DATA_SEED", "42"))
    generate_datasets = os.getenv("GENERATE_DATASETS", "all").strip().lower()
    valid_datasets = ("all", "network", "marketing")
    if generate_datasets not in valid_datasets:
        raise ValueError(
            f"Invalid GENERATE_DATASETS '{generate_datasets}'. "
            f"Valid options: {', '.join(valid_datasets)}"
        )

    # Customer chunk: how many customers' worth of CDRs to materialize at once.
    # 500 customers x ~70 CDRs/day x 30 days ~= 1M rows per chunk, ~100MB peak.
    customer_chunk_size = 500

    print("=" * 60)
    print("Telco Data Generator (vectorized)")
    print("=" * 60)
    print("Configuration:")
    print(f"  ClickHouse Host: {clickhouse_host}:{clickhouse_http_port}")
    if data_size:
        print(f"  Data Size Preset: {data_size}")
    print(f"  Number of Customers: {num_customers}")
    print(f"  Days of Data: {num_days}")
    print(f"  Marketing Campaigns: {num_campaigns}")
    print(f"  Network Events per Day: {events_per_day}")
    print(f"  Datasets: {generate_datasets}")
    print(f"  Seed: {seed}")
    print(f"  Customer Chunk Size: {customer_chunk_size}")
    print("=" * 60)

    print("\nConnecting to ClickHouse...")
    connect_kwargs = dict(
        host=clickhouse_host, port=clickhouse_http_port,
        username=clickhouse_user, password=clickhouse_password,
    )
    if clickhouse_secure:
        connect_kwargs["secure"] = True
    client = clickhouse_connect.get_client(**connect_kwargs)
    print("[OK] Connected to ClickHouse")

    print("\nInitializing generator (warming Faker pools)...")
    t0 = time.time()
    generator = TelcoDataGenerator(seed=seed)
    print(f"[OK] Initialized in {time.time() - t0:.2f}s")

    total_customers = 0
    total_cdrs = 0
    total_events = 0
    total_campaigns = 0

    if generate_datasets in ("all", "marketing"):
        print("\nGenerating customer data...")
        t0 = time.time()
        cust_cols, cust_data = generator.generate_customers_columns(num_customers)
        insert_columns(client, "telco.customers", cust_cols, cust_data)
        total_customers = num_customers
        print(f"[OK] Inserted {total_customers} customers in {time.time() - t0:.2f}s")

        cust_id_idx = cust_cols.index("customer_id")
        seg_idx = cust_cols.index("segment")
        customer_ids = cust_data[cust_id_idx]
        customer_segments = cust_data[seg_idx]
        del cust_data  # Free all customer columns except IDs+segments.

        print(f"\nGenerating CDRs (chunks of {customer_chunk_size} customers)...")
        t0 = time.time()
        num_chunks = (num_customers + customer_chunk_size - 1) // customer_chunk_size
        for chunk_start in range(0, num_customers, customer_chunk_size):
            chunk_end = min(chunk_start + customer_chunk_size, num_customers)
            chunk_num = chunk_start // customer_chunk_size + 1
            cdr_cols, cdr_data = generator.generate_cdrs_columns(
                customer_ids[chunk_start:chunk_end],
                customer_segments[chunk_start:chunk_end],
                days=num_days,
            )
            inserted = insert_columns(client, "telco.call_detail_records",
                                      cdr_cols, cdr_data)
            total_cdrs += inserted
            print(f"  Chunk {chunk_num}/{num_chunks}: {inserted} CDRs")
            del cdr_data
        print(f"[OK] Inserted {total_cdrs} CDRs in {time.time() - t0:.2f}s")

        del customer_ids, customer_segments

        print("\nGenerating marketing campaigns...")
        t0 = time.time()
        camp_cols, camp_data = generator.generate_campaigns_columns(num_campaigns)
        insert_columns(client, "telco.marketing_campaigns", camp_cols, camp_data)
        total_campaigns = num_campaigns
        print(f"[OK] Inserted {total_campaigns} campaigns in {time.time() - t0:.2f}s")

    if generate_datasets in ("all", "network"):
        print(f"\nGenerating network events ({events_per_day}/day)...")
        t0 = time.time()
        for day in range(num_days):
            cols, data = generator.generate_network_events_day_columns(
                day, num_days, events_per_day
            )
            inserted = insert_columns(client, "telco.network_events", cols, data)
            total_events += inserted
            if (day + 1) % 10 == 0 or day == num_days - 1:
                print(f"  Day {day + 1}/{num_days}: {total_events} events total")
            del data
        print(f"[OK] Inserted {total_events} events in {time.time() - t0:.2f}s")

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

    client.close()


if __name__ == "__main__":
    main()
