"""
ClickHouse Forex Live Dashboard — FastAPI backend.

Serves a small single-page dashboard (static/) and a JSON API that queries the
`forex` table you loaded during the workshop. Every API response carries the
ClickHouse query timing and rows-scanned count, so the front end can show how
little work the database does when a filter hits the sort key.

Runs on Python 3.9+ (the Docker image pins 3.12, so your laptop's Python
version doesn't matter).
"""

import os
import time
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Load a local .env if present (Docker passes these via env_file instead).
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # python-dotenv is optional at runtime
    pass

import clickhouse_connect

app = FastAPI(title="ClickHouse Forex Live Dashboard")

_client = None


def get_client():
    """Create (once) and reuse a ClickHouse Cloud client from env vars."""
    global _client
    if _client is None:
        host = os.getenv("CLICKHOUSE_HOST")
        if not host:
            raise HTTPException(
                status_code=500,
                detail="CLICKHOUSE_HOST is not set. Copy .env.example to .env "
                "and fill in your ClickHouse Cloud connection details.",
            )
        _client = clickhouse_connect.get_client(
            host=host,
            port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "default"),
            secure=os.getenv("CLICKHOUSE_SECURE", "true").lower() in ("1", "true", "yes"),
            connect_timeout=15,
            query_limit=0,
        )
    return _client


def _run(sql, params=None):
    """Run a query and return (result, wall_ms, rows_read, server_ms)."""
    client = get_client()
    t0 = time.perf_counter()
    res = client.query(sql, parameters=params or {})
    wall_ms = (time.perf_counter() - t0) * 1000.0
    summary = res.summary or {}
    rows_read = int(summary.get("read_rows", 0) or 0)
    elapsed_ns = summary.get("elapsed_ns")
    server_ms = (int(elapsed_ns) / 1_000_000.0) if elapsed_ns else None
    return res, wall_ms, rows_read, server_ms


@app.get("/api/health")
def health():
    try:
        _run("SELECT 1")
        return {"ok": True}
    except Exception as exc:  # surface config/connection problems to the UI
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


@app.get("/api/meta")
def meta():
    """Currency pairs and the available date range, for the filter controls."""
    res, _, _, _ = _run(
        "SELECT DISTINCT concat(base, '/', quote) AS pair FROM forex ORDER BY pair"
    )
    pairs = [row[0] for row in res.result_rows]
    res2, _, _, _ = _run(
        "SELECT toDate(min(datetime)) AS a, toDate(max(datetime)) AS b FROM forex"
    )
    dmin, dmax = res2.result_rows[0]
    return {"pairs": pairs, "date_min": str(dmin), "date_max": str(dmax)}


@app.get("/api/dashboard")
def dashboard(
    pair: str = Query(..., description="e.g. XAU/USD"),
    start: str = Query(..., description="YYYY-MM-DD (inclusive)"),
    end: str = Query(..., description="YYYY-MM-DD (inclusive)"),
    bucket: str = Query("day", pattern="^(day|hour)$"),
):
    """OHLC candles + volume + KPIs for one pair over a date range."""
    if "/" not in pair:
        raise HTTPException(400, "pair must look like BASE/QUOTE, e.g. XAU/USD")
    base, quote = pair.split("/", 1)

    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")
        # end is inclusive, so we scan up to the start of the following day
        end_dt = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        raise HTTPException(400, "start and end must be YYYY-MM-DD")

    bucket_expr = "toStartOfHour(datetime)" if bucket == "hour" else "toStartOfDay(datetime)"
    params = {"base": base, "quote": quote, "start": start_dt, "end": end_dt}
    where = (
        "base = {base:String} AND quote = {quote:String} "
        "AND datetime >= {start:DateTime} AND datetime < {end:DateTime}"
    )

    ohlc_sql = f"""
        SELECT {bucket_expr}        AS t,
               argMin(bid, datetime) AS open,
               max(bid)              AS high,
               min(bid)              AS low,
               argMax(bid, datetime) AS close,
               count()               AS volume,
               round(avg(ask - bid), 6) AS avg_spread
        FROM forex
        WHERE {where}
        GROUP BY t
        ORDER BY t
    """
    kpi_sql = f"""
        SELECT count()                             AS ticks,
               round(quantile(0.5)(ask - bid), 6)  AS median_spread,
               round(quantile(0.99)(ask - bid), 6) AS p99_spread,
               argMax(bid, datetime)               AS last_bid,
               min(bid)                            AS low,
               max(bid)                            AS high
        FROM forex
        WHERE {where}
    """

    r1, w1, rr1, s1 = _run(ohlc_sql, params)
    r2, w2, rr2, s2 = _run(kpi_sql, params)

    ohlc = [
        {
            "t": str(row[0]),
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": int(row[5]),
            "spread": row[6],
        }
        for row in r1.result_rows
    ]

    if r2.result_rows and r2.result_rows[0][0]:
        k = r2.result_rows[0]
        kpis = {
            "ticks": int(k[0]),
            "median_spread": k[1],
            "p99_spread": k[2],
            "last_bid": k[3],
            "low": k[4],
            "high": k[5],
        }
    else:
        kpis = {"ticks": 0, "median_spread": None, "p99_spread": None,
                "last_bid": None, "low": None, "high": None}

    server_ms = round(s1 + s2, 2) if (s1 is not None and s2 is not None) else None
    timing = {
        "server_ms": server_ms,
        "wall_ms": round(w1 + w2, 2),
        "rows_read": rr1 + rr2,
        "queries": 2,
    }
    return {"ohlc": ohlc, "kpis": kpis, "timing": timing, "bucket": bucket, "pair": pair}


# Static single-page front end. Mount last so /api/* routes win.
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")
