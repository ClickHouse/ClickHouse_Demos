#!/usr/bin/env python3
"""
Sample Order-Processing Flask App
Instrumented with OpenTelemetry (manual spans) exporting traces via OTLP to Elastic APM Server.

Endpoints:
  GET  /orders         - list orders
  POST /orders         - create order
  GET  /orders/<id>    - get order by ID
  GET  /health         - health check

A background thread hits these endpoints every 1-2 seconds to generate
continuous trace data without requiring external traffic.
"""

import random
import threading
import time
import uuid
from datetime import datetime, timezone

import requests
from flask import Flask, g, jsonify, request

# ---------------------------------------------------------------------------
# OpenTelemetry setup (core SDK only — no instrumentation package needed)
# ---------------------------------------------------------------------------
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, StatusCode

OTEL_SERVICE_NAME = "sample-order-app"
# Elastic APM Server 8.x accepts OTLP traces on /v1/traces
OTLP_ENDPOINT = "http://elastic-apm-server:8200/v1/traces"

resource = Resource.create({"service.name": OTEL_SERVICE_NAME})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint=OTLP_ENDPOINT)
provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(OTEL_SERVICE_NAME)

# ---------------------------------------------------------------------------
# Flask app with manual request tracing via before/after hooks
# ---------------------------------------------------------------------------
app = Flask(__name__)


@app.before_request
def before_request():
    """Start a server span for every incoming request."""
    span = tracer.start_span(
        name=f"{request.method} {request.path}",
        kind=SpanKind.SERVER,
    )
    span.set_attribute("http.method", request.method)
    span.set_attribute("http.url", request.url)
    span.set_attribute("http.host", request.host)
    span.set_attribute("http.scheme", request.scheme)
    g.span = span
    g.span_ctx = trace.use_span(span, end_on_exit=False).__enter__()


@app.after_request
def after_request(response):
    """Finish the server span with the HTTP status code."""
    span = getattr(g, "span", None)
    if span is not None:
        span.set_attribute("http.status_code", response.status_code)
        if response.status_code >= 500:
            span.set_status(StatusCode.ERROR)
        span.end()
    return response


@app.errorhandler(Exception)
def handle_exception(e):
    span = getattr(g, "span", None)
    if span is not None:
        span.record_exception(e)
        span.set_status(StatusCode.ERROR, str(e))
        span.end()
    return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# In-memory order store
# ---------------------------------------------------------------------------
ORDERS: dict[str, dict] = {}

PRODUCTS = [
    "Widget A", "Widget B", "Gadget X", "Gadget Y",
    "Component Z", "Module Q", "Part 99", "Assembly R",
]

CUSTOMERS = [f"customer-{i:04d}" for i in range(1, 201)]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": OTEL_SERVICE_NAME})


@app.route("/orders", methods=["GET"])
def list_orders():
    with tracer.start_as_current_span("db.query.orders") as span:
        time.sleep(random.lognormvariate(-3, 0.5))  # ~50ms median
        span.set_attribute("db.system", "in-memory")
        span.set_attribute("db.operation", "SELECT")
        result = list(ORDERS.values())[-50:]  # return last 50
        span.set_attribute("db.rows_returned", len(result))
    return jsonify({"orders": result, "total": len(ORDERS)})


@app.route("/orders", methods=["POST"])
def create_order():
    order_id = str(uuid.uuid4())[:8]
    product = random.choice(PRODUCTS)
    customer = random.choice(CUSTOMERS)
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(9.99, 299.99), 2)

    # Simulate inventory check
    with tracer.start_as_current_span("inventory.check") as span:
        time.sleep(random.lognormvariate(-3.5, 0.4))  # ~30ms median
        span.set_attribute("inventory.product", product)
        span.set_attribute("inventory.quantity_requested", quantity)
        in_stock = random.random() > 0.05  # 95% in stock
        span.set_attribute("inventory.in_stock", in_stock)

    if not in_stock:
        return jsonify({"error": "out of stock", "product": product}), 409

    # Simulate payment authorization
    with tracer.start_as_current_span("payment.authorize") as span:
        time.sleep(random.lognormvariate(-2.5, 0.6))  # ~80ms median
        span.set_attribute("payment.amount", unit_price * quantity)
        span.set_attribute("payment.customer", customer)
        auth_ok = random.random() > 0.02  # 98% success
        span.set_attribute("payment.authorized", auth_ok)

    if not auth_ok:
        return jsonify({"error": "payment declined", "customer": customer}), 402

    order = {
        "id": order_id,
        "customer": customer,
        "product": product,
        "quantity": quantity,
        "unit_price": unit_price,
        "total": round(unit_price * quantity, 2),
        "status": "confirmed",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    ORDERS[order_id] = order
    return jsonify(order), 201


@app.route("/orders/<order_id>", methods=["GET"])
def get_order(order_id):
    with tracer.start_as_current_span("db.query.order_by_id") as span:
        time.sleep(random.lognormvariate(-4, 0.3))  # ~18ms median
        span.set_attribute("db.system", "in-memory")
        span.set_attribute("db.operation", "SELECT")
        span.set_attribute("order.id", order_id)

    order = ORDERS.get(order_id)
    if order is None:
        return jsonify({"error": "not found", "id": order_id}), 404
    return jsonify(order)


# ---------------------------------------------------------------------------
# Background traffic generator
# ---------------------------------------------------------------------------

def _generate_traffic():
    """Hit own endpoints continuously to produce trace data."""
    base = "http://localhost:5000"
    time.sleep(5)  # wait for Flask to start
    while True:
        try:
            roll = random.random()
            if roll < 0.40:
                requests.post(f"{base}/orders", timeout=5)
            elif roll < 0.70:
                requests.get(f"{base}/orders", timeout=5)
            else:
                order_id = random.choice(list(ORDERS.keys())) if ORDERS else "unknown"
                requests.get(f"{base}/orders/{order_id}", timeout=5)
        except Exception:
            pass
        time.sleep(random.uniform(1.0, 2.0))


if __name__ == "__main__":
    t = threading.Thread(target=_generate_traffic, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)
