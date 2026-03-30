# Clickstack Walkthrough — 5-10 Minute Guide

**URL:** https://play-clickstack.clickhouse.com

Clickstack is ClickHouse's built-in observability UI — search, explore, and visualize your OpenTelemetry data without writing SQL. This guide walks through every section in under 10 minutes.

---

## What is Clickstack?

Clickstack sits directly on top of ClickHouse and reads OpenTelemetry data (traces, logs, metrics) natively. Unlike the AI agent in LibreChat, Clickstack gives you purpose-built observability UI — pre-built service maps, session timelines, and chart explorers — without needing to prompt a model or write SQL.

| | **LibreChat AI Agent** | **Clickstack** |
|---|---|---|
| Interface | Conversational chat | Purpose-built observability UI |
| Query method | Natural language → SQL | Pre-built views + search |
| Custom logic | Full SQL via MCP | Structured filters |
| Traces to Langfuse | Yes | No |
| Service map | Generated on demand | Built-in, always on |

---

## Section 1 — Search (2 min)

**Navigate to:** https://play-clickstack.clickhouse.com/search

Search is the entry point. It runs full-text and structured queries across logs, traces, and spans simultaneously.

### Try these searches in order:

**1. Find errors across all services**
```
level:error
```
> Look at the results panel — notice how results show service name, severity, timestamp, and the full log body. Click any result to expand it.

**2. Search for a specific service**
```
service:checkout
```
> Filter to a single service. Notice the timeline histogram at the top — it shows event density over time. Zoom into a spike.

**3. Find slow requests by keyword**
```
timeout
```
> Full-text search across log bodies. Find "timeout" messages and note which services and trace IDs they belong to.

**4. Combine filters**
```
service:payment level:error
```
> Narrow to payment service errors only. Use the time range picker (top right) to change from the last hour to the last 24 hours. How does the volume change?

**5. Trace ID lookup**
> Copy a trace ID from any result → paste it directly in the search box → see all events belonging to that single request.

### Key observations:
- The histogram updates as you type — use it to spot error spikes visually
- Click column headers to sort by time, level, or service
- The sidebar filters let you narrow by severity, service, or attribute without retyping

---

## Section 2 — Service Map (2 min)

**Navigate to:** Service Map (left sidebar)

The service map visualizes which services call which other services and the health of each connection — automatically derived from trace data.

### What to look for:

**1. Identify the entry points**
> Look for nodes with no incoming arrows — these are the services users hit directly (e.g., `frontend-proxy`, `api-gateway`). They are the start of every trace.

**2. Find the most connected service**
> Which node has the most incoming AND outgoing arrows? This is your highest-blast-radius service — if it goes down, everything downstream fails.

**3. Spot unhealthy edges**
> Edges (arrows between services) may be colored by error rate. Red or orange edges indicate connections with elevated errors.

**4. Click a service node**
> Click any node to see its metrics: request rate, error rate, p50/p95/p99 latency. Compare the numbers to what the AI agent returned earlier for the same service.

**5. Trace a request path**
> Start at `frontend-proxy` → follow the arrows downstream → how many services does a typical checkout request touch?

### Discussion question:
*"If the `payment` service starts failing, which other services would be immediately affected? How does the map help you prioritize an incident response?"*

---

## Section 3 — Chart Explorer (2 min)

**Navigate to:** Chart Explorer (left sidebar)

Chart Explorer lets you build custom time-series charts from any metric or trace field without writing SQL.

### Build a chart:

**1. Request volume over time**
- Metric: `span count` or `request rate`
- Group by: `service.name`
- Time range: last 3 hours
- Visualization: Line chart

> Compare the shape of different services. Does traffic rise and fall together (correlated load) or independently?

**2. Error rate trend**
- Metric: `error rate`
- Group by: `service.name`
- Visualization: Bar chart

> Look for services where the error rate is climbing. A steady upward slope is a leading indicator of a future incident.

**3. Latency percentiles**
- Metric: `duration p95` or `p99`
- Group by: `service.name`
- Visualization: Line chart

> Compare p95 vs p99. A large gap between the two means high variance — most requests are fast but some are extremely slow (tail latency problem).

### Key insight:
Chart Explorer uses ClickHouse materialized views under the hood — pre-aggregated data that makes even large time ranges respond instantly. This is the same `otel_service_health_hourly` view you queried directly in the SQL section.

---

## Section 4 — Dashboards (1 min)

**Navigate to:** Dashboards (left sidebar)

Dashboards are pre-built collections of charts pinned to a single page.

### Explore:

**1. Open the default Service Health dashboard**
> You should see error rate, request volume, and latency panels for all services on one page. This is what an on-call engineer would open first during an incident.

**2. Change the time range**
> Switch from "Last 1 hour" to "Last 7 days". Notice how the charts re-render without page reload — ClickHouse returns aggregated results in milliseconds even for week-long ranges.

**3. Note the difference from Grafana/Datadog**
> Traditional observability tools use a separate time-series database (InfluxDB, Prometheus). Clickstack stores everything in ClickHouse — traces, logs, metrics, and dashboards all in one place. No data duplication, no sync lag.

---

## Section 5 — Client Sessions (1 min)

**Navigate to:** Client Sessions (left sidebar)

Client Sessions groups trace activity by user session — useful for understanding the end-to-end experience of a specific user or browser session.

### Try:

**1. Browse recent sessions**
> Each row is a user session. Columns show session duration, number of requests, error count, and pages visited.

**2. Click a session with errors**
> Drill into the session timeline — see every request in chronological order, with errors highlighted. This is the same data as distributed traces but organized around the user's journey.

**3. Find the slowest session**
> Sort by duration descending. Open the session — is the slowness at the start (slow initial load) or at a specific user action (slow checkout, slow search)?

### Key insight:
Session-level view bridges observability and product analytics. You're not just seeing "the checkout service was slow at 3pm" — you're seeing "user session X had 3 failed payment attempts and then abandoned the session".

---

## Recap: Clickstack vs SQL vs AI Agent

You've now used three different interfaces to explore the same OpenTelemetry data:

| Task | Best tool |
|------|-----------|
| Unknown problem, exploring | **Clickstack Search** |
| Service health at a glance | **Clickstack Service Map / Dashboards** |
| Custom metric or ad-hoc question | **LibreChat AI Agent** |
| Precise aggregation or join | **Direct SQL at sql-clickhouse.clickhouse.com** |
| Understanding AI behavior (cost, latency) | **Langfuse** |

All four tools read the same `otel_v2` tables in ClickHouse. The data is identical — only the interface and query method differ.

---

## Key Takeaway

Clickstack demonstrates that ClickHouse isn't just a query engine — it's a complete observability backend. By storing traces, logs, and metrics in a single columnar store, you get:

- **Sub-second dashboards** on weeks of data (materialized views)
- **No data pipeline** between your app and your dashboards
- **SQL access** to every metric and log when you need it
- **AI-ready** data — the same tables power the LibreChat agent

---

*Part of the AI Observability Workshop — github.com/ClickHouse/ClickHouse_Demos/tree/main/agent_stack_builds/clickhouse_ai_obserability*
