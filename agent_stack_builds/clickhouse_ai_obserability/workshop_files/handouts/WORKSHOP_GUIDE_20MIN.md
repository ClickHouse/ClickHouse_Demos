# AI Observability Workshop — 20-30 Minute Participant Guide

**Tools used today:**

| Tool | URL | Purpose |
|------|-----|---------|
| **AgentHouse** | `https://llm.clickhouse.com` | AI agent — natural language → SQL → results |
| **Clickstack** | `https://play-clickstack.clickhouse.com` | Purpose-built observability UI |
| **ClickHouse SQL** | `https://sql.clickhouse.com` | Direct SQL console |

---

## Part 1 — Getting Started with AgentHouse (3 min)

AgentHouse is ClickHouse's hosted AI agent. It combines LibreChat's chat interface with Claude Sonnet and a ClickHouse MCP server pre-connected to public datasets including the OpenTelemetry (`otel_v2`) database you'll use throughout this workshop.

### Step 1: Sign in

1. Open **https://llm.clickhouse.com**
2. Click **Sign in with Google**
3. Use your Google account — no separate account creation needed
4. You land on the AgentHouse chat interface

> If you don't have a Google account, ask the facilitator for a shared workshop account.

### Step 2: Orient yourself

The interface has three key areas:
- **Left sidebar** — conversation history (your chats are saved per Google account)
- **Centre** — the chat window with the AI
- **Top** — model selector and tool toggles

### Step 3: Discover what's available

Start with this question to understand what data you have access to:

```
What databases and tables do you have access to? Give me a summary of each.
```

> AgentHouse has access to multiple public ClickHouse datasets. You'll explore the `otel_v2` observability dataset in this workshop, and briefly explore others at the end.

---

## Part 2 — Observability: Service Health (7 min)

The `otel_v2` database contains real OpenTelemetry data — distributed traces, metrics, and logs from a demo microservices application. Run these prompts in order.

### 2.1 — Service Discovery (2 min)

```
What services are in the otel_v2 database?
Show me their request volume and error rates in the last hour.
```

**What to observe:**
- The agent queries `otel_traces` and groups by `ServiceName`
- Look at which services have traffic and which don't
- Note the error rates — any service above 1% is worth investigating

```
Which service has the highest p99 latency? How does it compare to p50?
```

> A large gap between p50 and p99 means most requests are fast but some are extremely slow — a tail latency problem. This is different from a uniformly slow service.

### 2.2 — Error Investigation (3 min)

```
Find the top 5 services by error rate in the last hour.
For the worst one, show me the actual error messages from the logs.
```

**What to observe:**
- The agent runs two queries: one on `otel_traces` for error rates, then a JOIN with `otel_logs` to pull error messages
- Notice how it correlates traces and logs via `TraceId` — this cross-signal correlation is what makes OpenTelemetry powerful

```
Are any services violating their SLA targets?
Show me actual p95 latency vs the target for each service.
```

### 2.3 — Root Cause Analysis (2 min)

```
The checkout service seems slow. Find the slowest traces in the last hour
and tell me which downstream service is causing the delay.
```

**What to observe:**
- The agent queries `otel_service_dependencies` to map parent→child service calls
- It identifies which downstream hop contributes the most latency
- This is the core RCA workflow: start at symptoms, trace to cause

---

## Part 3 — Distributed Tracing & Dependencies (5 min)

### 3.1 — Service Dependency Map (2 min)

```
Build a service dependency map for the last hour.
Which services call which, and what's the error rate on each connection?
Show it as a mermaid diagram.
```

**What to observe:**
- A Mermaid `flowchart LR` diagram should render in the response
- Entry-point services (no incoming arrows) vs. internal services
- Highlighted edges show connections with elevated error rates

### 3.2 — Trace Deep Dive (2 min)

```
Find the 5 slowest end-to-end traces in the last hour.
For each one, show me which service took the longest and what operation it was running.
```

```
How many services does a typical checkout trace touch?
What's the average number of spans per trace?
```

### 3.3 — Time Comparison (1 min)

```
Compare the error rate for all services between the last hour and 24 hours ago.
Which services have gotten worse?
```

> This pattern — current vs. baseline — is the most common question in incident response. Is this a new problem or an ongoing one?

---

## Part 4 — Beyond Observability: Explore Other Datasets (5 min)

AgentHouse has access to other public ClickHouse datasets. Ask the agent to switch context.

### 4.1 — Discover other data

```
What other datasets do you have besides otel_v2?
What's interesting in each one?
```

### 4.2 — Try a dataset from the list

Pick one from what the agent shows and run an exploratory question. Some ideas:

**If there's a GitHub/OSS dataset:**
```
What are the most starred open source repositories in the last 6 months?
Show me the trend as a mermaid bar chart.
```

**If there's an e-commerce/sales dataset:**
```
What are the top 10 products by revenue?
Which category has the highest return rate?
```

**If there's a web analytics dataset:**
```
What are the top landing pages by sessions in the last 7 days?
What's the bounce rate trend?
```

### 4.3 — Cross-dataset question (advanced)

```
Is there any correlation between [something in dataset A] and [something in dataset B]?
```

> **The key point:** The same agent and the same MCP interface works for any ClickHouse dataset — observability, sales, product analytics, or your own data. The agent writes SQL for each one.

---

## Part 5 — Clickstack: Purpose-Built Observability UI (5 min)

While AgentHouse is conversational, Clickstack gives you always-on observability views without prompting.

**Open:** https://play-clickstack.clickhouse.com

### 5.1 — Search (1 min)

Go to the **Search** tab and try:

```
service:checkout level:error
```

Then try a trace ID — copy one from your AgentHouse conversation and paste it directly into the search box.

### 5.2 — Service Map (1 min)

Click **Service Map** in the sidebar.

- Find the same entry-point services the AI agent identified
- Click a node — compare the latency numbers to what AgentHouse told you
- **Discussion:** Which view is faster for spotting an incident mid-shift?

### 5.3 — Chart Explorer (2 min)

Click **Chart Explorer**.

Build this chart:
- Metric: `error rate`
- Group by: `service.name`
- Visualization: Line chart
- Time range: Last 6 hours

> Does the error rate chart match what AgentHouse told you? It should — they both read the same `otel_v2` tables in ClickHouse. The difference is the interface, not the data.

### 5.4 — Dashboards (1 min)

Open the default **Service Health** dashboard.

Change the time range to **Last 7 days** and watch the charts re-render. ClickHouse aggregates weeks of telemetry data in milliseconds using pre-built materialized views.

---

## Part 6 — Direct SQL (Optional, 5 min)

For engineers who want to go beyond what the AI suggests, the raw SQL console is always available.

**Open:** https://sql.clickhouse.com

Connect with: Host `sql-clickhouse.clickhouse.com` · User `demo` · Password *(blank)* · Database `otel_v2`

**Validate the AI's answer — run this manually:**

```sql
SELECT
    ServiceName,
    count()                                                    AS total_requests,
    countIf(StatusCode = 'ERROR')                             AS errors,
    round(countIf(StatusCode = 'ERROR') / count() * 100, 2)  AS error_rate_pct,
    round(quantile(0.95)(Duration) / 1e6, 1)                 AS p95_ms,
    round(quantile(0.99)(Duration) / 1e6, 1)                 AS p99_ms
FROM otel_v2.otel_traces
WHERE Timestamp >= now() - INTERVAL 1 HOUR
  AND SpanKind = 'SERVER'
GROUP BY ServiceName
ORDER BY error_rate_pct DESC
```

Compare the output to what AgentHouse returned. They should match exactly — because AgentHouse generated and ran this same SQL query against the same database.

> **The insight:** The AI doesn't invent answers. It writes SQL, runs it against ClickHouse, and formats the result. You can always see and validate the exact query it used.

---

## What You Just Learned

```
Natural Language
       ↓
  AgentHouse (Claude Sonnet + MCP)
       ↓
  SQL query generated + executed
       ↓
  ClickHouse Cloud (otel_v2)
       ↓
  Results + visualizations in chat
```

The same data powers:
- **AgentHouse** — conversational AI agent
- **Clickstack** — purpose-built observability dashboards
- **sql.clickhouse.com** — raw SQL console

All three tools read the same tables. ClickHouse is the single source of truth.

---

## Quick Reference

| Resource | URL |
|----------|-----|
| AgentHouse (AI Agent) | `https://llm.clickhouse.com` |
| Clickstack | `https://play-clickstack.clickhouse.com` |
| ClickHouse SQL Console | `https://sql.clickhouse.com` |

| Login | Method |
|-------|--------|
| AgentHouse | Sign in with Google |
| Clickstack | No login required |
| SQL Console | Host: `sql-clickhouse.clickhouse.com` · User: `demo` · Password: *(blank)* |

**Key ClickHouse facts:**
- Duration is in **nanoseconds** → divide by `1,000,000` for milliseconds
- Use `SpanKind = 'SERVER'` for user-facing request latency
- Always filter by `Timestamp` — ClickHouse partitions by time for performance
- Ask the AI: *"Show me the SQL you used"* to see and learn from every query

---

*Workshop materials: github.com/ClickHouse/ClickHouse_Demos/tree/main/agent_stack_builds/clickhouse_ai_obserability*
