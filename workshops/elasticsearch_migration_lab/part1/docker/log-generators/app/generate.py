#!/usr/bin/env python3
"""
Application Log Generator
Generates semi-structured JSON application logs at ~200 events/sec.
Writes to /var/log/generators/app-{service}.log (one file per service).
5% of lines are ERROR entries with a multi-line stack trace embedded in JSON.
"""

import json
import os
import random
import string
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR = "/var/log/generators"
TARGET_RATE = 200          # events per second (across all services)
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB

SERVICES = [
    "web-frontend",
    "api-gateway",
    "order-service",
    "payment-service",
    "inventory-service",
]

# Level distribution: 60% INFO, 20% DEBUG, 15% WARN, 5% ERROR
LEVEL_POOL = (
    ["INFO"] * 60 +
    ["DEBUG"] * 20 +
    ["WARN"] * 15 +
    ["ERROR"] * 5
)

# Realistic log messages per level
INFO_MESSAGES = [
    "Order processed successfully for customer {cid}",
    "Payment authorised for order {oid} amount ${amt}",
    "Inventory updated for SKU {sku} quantity {qty}",
    "User session started for account {cid}",
    "Cache hit for key products:{oid}",
    "Request completed in {ms}ms path=/api/orders/{oid}",
    "Scheduled job completed: daily-report rows={qty}",
    "Health check passed for dependency db latency={ms}ms",
    "Connected to message broker successfully",
    "Configuration reloaded from remote source",
    "Rate limit bucket reset for client {cid}",
    "Webhook delivered to endpoint https://hooks.example.com/notify status=200",
    "Background worker picked up task id={oid}",
    "Email notification queued for user {cid}",
    "Feature flag evaluated: new-checkout=true user={cid}",
]

DEBUG_MESSAGES = [
    "Entering function processOrder with args order_id={oid}",
    "SQL query executed: SELECT * FROM orders WHERE id={oid} (took {ms}ms)",
    "Redis GET key=session:{cid} hit=True",
    "Deserialized payload 1024 bytes",
    "Retrying HTTP request attempt=2 url=https://api.partner.com/v2/verify",
    "Circuit breaker state=CLOSED failure_rate=0.02",
    "gRPC call to inventory-svc method=CheckStock latency={ms}ms",
    "Token validation passed sub={cid} exp=+3600s",
    "Pagination applied offset={qty} limit=25",
    "Trace context propagated trace_id={{trace}} span_id={{span}}",
]

WARN_MESSAGES = [
    "Slow query detected: {ms}ms threshold=500ms query=SELECT orders",
    "Deprecated API version v1 used by client {cid}",
    "Connection pool near capacity: {qty}/100 connections in use",
    "Disk usage at 78% on volume /data",
    "Cache miss rate elevated: 35% last 5min",
    "Retried request succeeded after 3 attempts endpoint=/api/payments",
    "JWT expiry imminent for user {cid} exp_in=120s",
    "Config value PAYMENT_TIMEOUT not set, using default 30s",
    "Batch size {qty} exceeds recommended limit of 500",
    "External service latency elevated: partner-api p99={ms}ms",
]

ERROR_MESSAGES = [
    "Failed to process order {oid} for customer {cid}",
    "Database connection refused host=postgres port=5432",
    "Unhandled exception in request handler path=/api/payments",
    "Payment gateway timeout after 30s order={oid}",
    "Inventory reservation failed SKU {sku} insufficient stock",
    "Message broker connection lost, attempting reconnect",
    "Authentication failed invalid token for user {cid}",
    "Service unavailable: downstream-api returned 503",
]

# Stack trace templates
STACK_TRACES = [
    (
        "Traceback (most recent call last):\n"
        "  File \"/app/services/order_service.py\", line 142, in process_order\n"
        "    result = db.execute(query, params)\n"
        "  File \"/app/db/connection.py\", line 87, in execute\n"
        "    return self.conn.execute(sql, args)\n"
        "psycopg2.OperationalError: could not connect to server: Connection refused\n"
        "\tIs the server running on host \"postgres\" (10.0.0.5) and accepting\n"
        "\tTCP/IP connections on port 5432?"
    ),
    (
        "Traceback (most recent call last):\n"
        "  File \"/app/handlers/payment.py\", line 63, in post\n"
        "    response = payment_client.charge(amount, card_token)\n"
        "  File \"/app/clients/stripe.py\", line 29, in charge\n"
        "    raise TimeoutError(f'Gateway timeout after {timeout}s')\n"
        "TimeoutError: Gateway timeout after 30s"
    ),
    (
        "Traceback (most recent call last):\n"
        "  File \"/app/workers/inventory.py\", line 201, in reserve_stock\n"
        "    qty = int(redis_client.get(f'stock:{sku}'))\n"
        "TypeError: int() argument must be a string, a bytes-like object or a number, not 'NoneType'\n"
        "During handling of the above exception, another exception occurred:\n"
        "  File \"/app/workers/inventory.py\", line 208, in reserve_stock\n"
        "    raise InventoryError(f'SKU {sku} not found in cache')\n"
        "app.exceptions.InventoryError: SKU B07XJ8C8F5 not found in cache"
    ),
    (
        "Traceback (most recent call last):\n"
        "  File \"/usr/local/lib/python3.12/site-packages/flask/app.py\", line 1455, in wsgi_app\n"
        "    response = self.full_dispatch_request()\n"
        "  File \"/app/api/routes.py\", line 88, in get_order\n"
        "    order = Order.query.get_or_404(order_id)\n"
        "werkzeug.exceptions.NotFound: 404 Not Found: No row was found when one was required"
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def random_hex(n: int) -> str:
    return "".join(random.choices(string.hexdigits[:16], k=n))


def random_id() -> str:
    return random_hex(32)


def random_span() -> str:
    return random_hex(16)


def fill_template(template: str) -> str:
    return (
        template
        .replace("{cid}", str(random.randint(1000, 99999)))
        .replace("{oid}", str(random.randint(10000, 999999)))
        .replace("{sku}", "B0" + random_hex(8).upper())
        .replace("{qty}", str(random.randint(1, 9999)))
        .replace("{amt}", f"{random.uniform(5.0, 5000.0):.2f}")
        .replace("{ms}", str(random.randint(1, 2000)))
    )


class RotatingFile:
    def __init__(self, path: str, max_size: int):
        self.path = path
        self.max_size = max_size
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

def build_record(service: str) -> dict:
    level = random.choice(LEVEL_POOL)
    trace_id = random_id()
    span_id = random_span()

    if level == "INFO":
        msg = fill_template(random.choice(INFO_MESSAGES))
    elif level == "DEBUG":
        msg = fill_template(random.choice(DEBUG_MESSAGES))
    elif level == "WARN":
        msg = fill_template(random.choice(WARN_MESSAGES))
    else:
        msg = fill_template(random.choice(ERROR_MESSAGES))

    record: dict = {
        "@timestamp": now_iso(),
        "level": level,
        "service": service,
        "message": msg,
        "trace_id": trace_id,
        "span_id": span_id,
    }

    if level == "ERROR":
        record["error"] = {
            "type": random.choice(["OperationalError", "TimeoutError", "ValueError", "RuntimeError"]),
            "message": msg,
            "stack": random.choice(STACK_TRACES),
        }

    return record


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    files = {
        svc: RotatingFile(
            os.path.join(OUTPUT_DIR, f"app-{svc}.log"),
            MAX_FILE_SIZE,
        )
        for svc in SERVICES
    }

    flush_every = 200
    count = 0

    print(f"App log generator started. Target rate: {TARGET_RATE} events/sec", flush=True)

    while True:
        start = time.monotonic()

        for _ in range(TARGET_RATE):
            svc = random.choice(SERVICES)
            record = build_record(svc)
            files[svc].write(json.dumps(record, separators=(",", ":")))

            count += 1
            if count % flush_every == 0:
                for f in files.values():
                    f.flush()

        elapsed = time.monotonic() - start
        sleep_for = 1.0 - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
