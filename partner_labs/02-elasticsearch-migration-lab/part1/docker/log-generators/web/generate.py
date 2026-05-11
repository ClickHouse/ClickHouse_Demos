#!/usr/bin/env python3
"""
Web Access Log Generator
Generates Nginx-style structured JSON access logs at ~500 events/sec.
Writes to /var/log/generators/web-access-{service}.log (one file per service).
"""

import json
import math
import os
import random
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR = "/var/log/generators"
TARGET_RATE = 500          # events per second (across all services)
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB

SERVICES = [
    "web-frontend",
    "api-gateway",
    "order-service",
    "payment-service",
    "inventory-service",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
]

# Pool of ~50 realistic-ish IPs
IP_POOL = [
    f"{a}.{b}.{c}.{d}"
    for a, b, c, d in [
        (203, 45, 67, 89), (198, 51, 100, 14), (192, 0, 2, 42), (10, 20, 30, 40),
        (172, 16, 0, 5), (185, 220, 101, 7), (91, 108, 4, 12), (77, 88, 55, 88),
        (8, 8, 8, 8), (1, 1, 1, 1), (104, 16, 132, 229), (151, 101, 65, 140),
        (13, 107, 42, 16), (20, 112, 52, 29), (52, 84, 151, 97), (54, 230, 200, 170),
        (67, 199, 248, 100), (69, 171, 250, 10), (74, 125, 21, 147), (93, 184, 216, 34),
        (95, 101, 64, 1), (103, 21, 244, 0), (108, 162, 192, 0), (109, 94, 218, 3),
        (114, 119, 0, 0), (117, 103, 64, 0), (118, 27, 22, 0), (120, 253, 48, 0),
        (122, 248, 64, 0), (125, 212, 4, 0), (128, 199, 0, 96), (130, 41, 128, 0),
        (134, 122, 0, 0), (136, 243, 154, 0), (138, 68, 0, 0), (139, 59, 64, 0),
        (141, 101, 64, 0), (142, 250, 64, 0), (143, 204, 0, 0), (146, 75, 0, 0),
        (147, 75, 0, 0), (149, 154, 160, 0), (151, 101, 0, 0), (153, 92, 0, 0),
        (155, 138, 0, 0), (157, 240, 0, 0), (160, 153, 0, 0), (162, 158, 0, 0),
        (163, 172, 0, 0), (165, 227, 0, 0),
    ]
]

# ~200 paths with Zipfian weights
_BASE_PATHS = [
    "/", "/api/orders", "/api/orders/{id}", "/api/products", "/api/products/{id}",
    "/api/cart", "/api/cart/add", "/api/cart/remove", "/api/checkout",
    "/api/payments", "/api/payments/{id}", "/api/inventory", "/api/inventory/{id}",
    "/api/users", "/api/users/{id}", "/api/users/profile", "/api/auth/login",
    "/api/auth/logout", "/api/auth/refresh", "/api/search",
    "/api/recommendations", "/api/reviews", "/api/reviews/{id}",
    "/api/categories", "/api/categories/{id}", "/api/tags", "/api/promotions",
    "/api/coupons", "/api/wishlist", "/api/addresses", "/api/shipping",
    "/api/tracking/{id}", "/api/returns", "/api/notifications",
    "/health", "/metrics", "/ready", "/live",
    "/static/js/main.js", "/static/css/main.css", "/static/images/logo.png",
    "/favicon.ico", "/robots.txt", "/sitemap.xml",
]

# Expand to ~200 paths with unique IDs injected
REQUEST_PATHS = []
for path in _BASE_PATHS:
    REQUEST_PATHS.append(path.replace("{id}", str(random.randint(1, 9999))))
    for _ in range(4):
        REQUEST_PATHS.append(path.replace("{id}", str(random.randint(1, 9999))))

# Zipfian weights: rank-based
_N = len(REQUEST_PATHS)
ZIPF_WEIGHTS = [1.0 / (i + 1) ** 0.8 for i in range(_N)]

# Status code pool with realistic distribution
STATUS_POOL = (
    ["200"] * 70 +
    ["204"] * 5 +
    ["301"] * 5 + ["302"] * 10 +
    ["400"] * 3 + ["401"] * 2 + ["403"] * 2 + ["404"] * 3 +
    ["500"] * 2 + ["502"] * 2 + ["503"] * 1
)

METHODS = ["GET"] * 60 + ["POST"] * 20 + ["PUT"] * 10 + ["DELETE"] * 5 + ["PATCH"] * 5

REFERERS = ["-", "https://example.com/", "https://google.com/", "-", "-", "-"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
           f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def time_local() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%d/%b/%Y:%H:%M:%S +0000")


def lognormal_run_time() -> float:
    """Log-normal with median ~0.1s, long tail up to a few seconds."""
    raw = math.exp(random.gauss(-2.3, 0.8))
    return round(max(0.001, min(raw, 30.0)), 6)


def random_size(status: str) -> int:
    if status.startswith("2"):
        return random.randint(128, 65536)
    if status.startswith("3"):
        return random.randint(64, 512)
    return random.randint(64, 4096)


# ---------------------------------------------------------------------------
# File handles with rotation
# ---------------------------------------------------------------------------

class RotatingFile:
    def __init__(self, path: str, max_size: int):
        self.path = path
        self.max_size = max_size
        self._size = 0
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._f = open(path, "a", buffering=8192)
        self._f.seek(0, 2)
        self._size = self._f.tell()

    def write(self, line: str):
        encoded = (line + "\n").encode("utf-8")
        if self._size + len(encoded) > self.max_size:
            self._rotate()
        self._f.write(line + "\n")
        self._size += len(encoded)

    def _rotate(self):
        self._f.close()
        rotated = self.path + ".1"
        if os.path.exists(rotated):
            os.remove(rotated)
        os.rename(self.path, rotated)
        self._f = open(self.path, "a", buffering=8192)
        self._size = 0

    def flush(self):
        self._f.flush()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    files = {
        svc: RotatingFile(
            os.path.join(OUTPUT_DIR, f"web-access-{svc}.log"),
            MAX_FILE_SIZE,
        )
        for svc in SERVICES
    }

    interval = 1.0 / TARGET_RATE  # seconds per event
    flush_every = 500
    count = 0

    print(f"Web access log generator started. Target rate: {TARGET_RATE} events/sec", flush=True)

    while True:
        start = time.monotonic()

        for _ in range(TARGET_RATE):
            svc = random.choice(SERVICES)
            status = random.choice(STATUS_POOL)
            path = random.choices(REQUEST_PATHS, weights=ZIPF_WEIGHTS, k=1)[0]
            record = {
                "@timestamp": now_iso(),
                "remote_addr": random.choice(IP_POOL),
                "request_type": random.choice(METHODS),
                "request_path": path,
                "status": status,
                "size": random_size(status),
                "user_agent": random.choice(USER_AGENTS),
                "referer": random.choice(REFERERS),
                "run_time": lognormal_run_time(),
                "time_local": time_local(),
                "service": svc,
            }
            files[svc].write(json.dumps(record, separators=(",", ":")))

            count += 1
            if count % flush_every == 0:
                for f in files.values():
                    f.flush()

        # Pace: sleep remaining time in the 1-second window
        elapsed = time.monotonic() - start
        sleep_for = 1.0 - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
