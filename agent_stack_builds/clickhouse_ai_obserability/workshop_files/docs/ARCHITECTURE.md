# Architecture Deep Dive

This document provides technical details about the observability workshop architecture.

---

## System Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                         PARTICIPANT                              │
│                      (Web Browser)                               │
└───────────────────────────┬──────────────────────────────────────┘
                            │ HTTP
                            │ Port 3000
┌───────────────────────────▼──────────────────────────────────────┐
│                       LIBRECHAT                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  • Chat UI (React)                                       │   │
│  │  • User Authentication                                   │   │
│  │  • Conversation Management                               │   │
│  │  • Message Routing                                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│         Depends on: MongoDB (data), MeiliSearch (search)        │
└───────────────────────────┬──────────────────────────────────────┘
                            │ HTTP
                            │ Port 4000
┌───────────────────────────▼──────────────────────────────────────┐
│                        LITELLM                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  • LLM Proxy & Router                                    │   │
│  │  • Model Fallback & Retry Logic                          │   │
│  │  • Rate Limiting & Cost Tracking                         │   │
│  │  • Langfuse Integration (Tracing)                        │   │
│  │  • System Prompt Injection                               │   │
│  └──────────────────────────────────────────────────────────┘   │
│         Depends on: PostgreSQL (config & cache)                 │
└──────────────┬──────────────────────┬────────────────────────────┘
               │                      │
               │ HTTPS                │ HTTPS
               │                      │
   ┌───────────▼──────────┐     ┌────▼──────────────┐
   │   LLM PROVIDERS      │     │   LANGFUSE CLOUD  │
   │                      │     │                   │
   │  • OpenAI (GPT-4)    │     │  • Trace Storage  │
   │  • Anthropic (Claude)│     │  • Analytics      │
   │  • Model APIs        │     │  • Dashboards     │
   └───────────┬──────────┘     └───────────────────┘
               │
               │ Generates SQL
               │
   ┌───────────▼──────────────────────────────────────┐
   │          CLICKHOUSE CLOUD                        │
   │                                                   │
   │  ┌────────────────────────────────────────────┐  │
   │  │  TABLES:                                   │  │
   │  │  • otel_traces      (spans)                │  │
   │  │  • otel_metrics     (time-series data)     │  │
   │  │  • otel_logs        (structured logs)      │  │
   │  │  • otel_services    (service catalog)      │  │
   │  │                                            │  │
   │  │  MATERIALIZED VIEWS:                       │  │
   │  │  • otel_service_metrics_1m                 │  │
   │  │  • otel_error_logs_summary                 │  │
   │  │  • otel_service_dependencies               │  │
   │  │  • otel_http_endpoint_metrics              │  │
   │  │  • otel_service_health_hourly              │  │
   │  └────────────────────────────────────────────┘  │
   └──────────────────────────────────────────────────┘
```

---

## Component Details

### 1. LibreChat (Chat UI)

**Purpose:** Web-based chat interface for natural language queries

**Technology Stack:**
- Frontend: React, TypeScript
- Backend: Node.js, Express
- Database: MongoDB (conversations, users)
- Search: MeiliSearch (message search)

**Key Features:**
- Multi-user support with authentication
- Conversation history
- Model selection (Claude, GPT-4, etc.)
- Markdown rendering
- Code syntax highlighting

**Configuration:** `config/librechat.yaml`

**Environment Variables:**
```bash
HOST=0.0.0.0
PORT=3000
MONGO_URI=mongodb://mongodb:27017/LibreChat
OPENAI_API_KEY=sk-litellm  # Points to LiteLLM
OPENAI_REVERSE_PROXY=http://litellm:4000
```

**Data Flow:**
1. User enters question in UI
2. LibreChat authenticates request
3. Message sent to LiteLLM (appears as OpenAI request)
4. Response streamed back to UI
5. Conversation saved in MongoDB

---

### 2. LiteLLM (LLM Proxy)

**Purpose:** Unified interface to multiple LLM providers with observability

**Technology Stack:**
- Python 3.10+
- FastAPI (REST API)
- PostgreSQL (configuration storage)
- Redis (optional caching)

**Key Features:**
- **Multi-provider support:** OpenAI, Anthropic, Azure, Google, etc.
- **Intelligent routing:** Load balancing, fallbacks
- **Cost tracking:** Token usage per user/model
- **Rate limiting:** Prevent quota exhaustion
- **Observability:** Langfuse integration for full tracing
- **System prompt injection:** Add context to all requests

**Configuration:** `config/litellm_config.yaml`

**Critical Settings:**
```yaml
model_list:
  - model_name: claude-3-5-sonnet
    litellm_params:
      model: anthropic/claude-3-5-sonnet-20241022
      api_key: ${ANTHROPIC_API_KEY}
      metadata:
        tags: ["workshop", "observability"]

success_callback: ["langfuse"]  # Send traces to Langfuse
```

**Data Flow:**
1. Receive request from LibreChat
2. Inject system prompt (observability context)
3. Route to appropriate LLM provider
4. Stream response back
5. Log trace to Langfuse
6. Track costs in PostgreSQL

---

### 3. Langfuse Cloud (LLMOps Platform)

**Purpose:** Observability and analytics for AI agents

**Hosted Service:** https://cloud.langfuse.com

**Key Features:**
- **Distributed tracing:** Full request lifecycle
- **Token tracking:** Cost per query
- **Latency analysis:** Identify bottlenecks
- **Prompt versioning:** Track prompt changes
- **User analytics:** Usage patterns
- **Error monitoring:** Failed requests

**Data Model:**
```
Trace (end-to-end request)
  ├── Generation (LLM call)
  │   ├── Prompt (input)
  │   ├── Completion (output)
  │   ├── Tokens (input/output)
  │   └── Latency
  └── Metadata
      ├── User ID
      ├── Model
      ├── Tags
      └── Custom attributes
```

**Integration:**
- LiteLLM automatically sends traces
- No code changes required
- Uses `LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY`

---

### 4. ClickHouse Cloud (OLAP Database)

**Purpose:** Store and query OpenTelemetry observability data

**Hosted Service:** https://clickhouse.cloud or sql.clickhouse.com

**Key Features:**
- **Columnar storage:** Optimized for analytics
- **Real-time ingestion:** Handle high-throughput data
- **SQL interface:** Familiar query language
- **Materialized views:** Pre-aggregated metrics
- **Partitioning:** Time-based for efficient querying
- **Compression:** ZSTD for reduced storage costs

**Table Design:**

#### otel_traces
```sql
-- Stores distributed trace spans
MergeTree ENGINE
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, Timestamp)
TTL 30 days

Key Columns:
- TraceId, SpanId, ParentSpanId (trace tree)
- Duration (nanoseconds!)
- StatusCode (OK, ERROR, UNSET)
- SpanAttributes (Map with http.*, db.*, etc.)
```

#### otel_metrics
```sql
-- Time-series metrics
MergeTree ENGINE
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, MetricName, Timestamp)
TTL 90 days

Supports:
- Gauges (current value)
- Counters (cumulative)
- Histograms (distributions)
- Summaries (percentiles)
```

#### otel_logs
```sql
-- Structured application logs
MergeTree ENGINE
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, Timestamp)
TTL 30 days

Key Features:
- TraceId/SpanId correlation
- Full-text search on Body
- Severity filtering
```

#### otel_services
```sql
-- Service catalog with SLAs
ReplacingMergeTree ENGINE
ORDER BY (ServiceNamespace, ServiceName, ServiceVersion)

Defines:
- SLA targets (latency, error rate)
- Ownership information
- Service metadata
```

**Materialized Views:**

Pre-aggregate common queries for performance:

1. **otel_service_metrics_1m** - Per-minute rollups
2. **otel_error_logs_summary** - Error patterns
3. **otel_service_dependencies** - Call graphs
4. **otel_http_endpoint_metrics** - Endpoint performance
5. **otel_service_health_hourly** - Historical health

---

## Data Flow: End-to-End Query

### Example: "Show me error rates by service"

#### Step 1: User Input (Browser → LibreChat)
```
User types: "Show me error rates by service in the last hour"
↓
LibreChat UI sends POST to /api/messages
```

#### Step 2: LibreChat → LiteLLM
```
POST http://litellm:4000/v1/chat/completions
{
  "model": "claude-3-5-sonnet",
  "messages": [
    {
      "role": "system",
      "content": "<System prompt with schema info>"
    },
    {
      "role": "user",
      "content": "Show me error rates by service in the last hour"
    }
  ]
}
```

#### Step 3: LiteLLM Processing
```
1. Validate request
2. Inject system prompt (from config/system_prompt.txt)
3. Add observability metadata
4. Route to Anthropic API
5. Start Langfuse trace
```

#### Step 4: LLM Generation
```
Anthropic API (Claude 3.5 Sonnet)
↓
Generates SQL query:

SELECT
    ServiceName,
    count() as total_requests,
    countIf(StatusCode = 'ERROR') as errors,
    round(countIf(StatusCode = 'ERROR') / count() * 100, 2) as error_rate_pct
FROM otel_traces
WHERE Timestamp >= now() - INTERVAL 1 HOUR
  AND SpanKind = 'SERVER'
GROUP BY ServiceName
ORDER BY error_rate_pct DESC
```

#### Step 5: Query Execution (Hypothetical)
```
Note: Current implementation doesn't execute queries automatically.
In production version, LiteLLM would:

1. Extract SQL from LLM response
2. Connect to ClickHouse
3. Execute query
4. Format results
5. Return to user

For workshop, LLM explains the query and expected results.
```

#### Step 6: Response Streaming
```
LiteLLM → LibreChat → Browser

Response includes:
1. Explanation of approach
2. SQL query (formatted)
3. Expected results (or actual if executed)
4. Insights and recommendations
```

#### Step 7: Langfuse Tracing
```
Langfuse receives trace:
- Trace ID: unique identifier
- Generation:
  - Model: claude-3-5-sonnet
  - Input tokens: 1,234
  - Output tokens: 567
  - Latency: 2.3s
  - Cost: $0.03
- Metadata:
  - User: workshop@example.com
  - Tags: ["observability", "workshop"]
```

---

## Security Considerations

### Network Security

**Production:**
```
Internet → ALB (HTTPS) → LibreChat (internal)
                      → LiteLLM (internal)

- Use AWS ALB with SSL certificate
- Keep LiteLLM internal (no public access)
- ClickHouse: Use secure connections (port 8443)
- Langfuse: HTTPS only
```

**Workshop:**
```
Internet → EC2 (HTTP port 3000) → LibreChat
                                → LiteLLM (exposed for debugging)

- Temporary security group rules
- Short-lived credentials
- Read-only ClickHouse access
```

### Data Security

**Sensitive Data:**
- API keys in environment variables (not code)
- JWT secrets randomly generated
- Database passwords rotated

**Access Control:**
- LibreChat: User authentication required
- LiteLLM: Master key authentication
- ClickHouse: User-level permissions
- Langfuse: Project-level API keys

**Data Retention:**
- Traces: 30 days (configurable)
- Chat history: Configurable (default: unlimited)
- Logs: 7-30 days

---

## Performance Considerations

### Scaling LibreChat

**Horizontal Scaling:**
```yaml
# docker-compose.yml
services:
  librechat:
    deploy:
      replicas: 3
    depends_on:
      - mongodb  # Shared state
```

**Limits:**
- MongoDB is bottleneck
- Consider MongoDB Atlas for production

### Scaling LiteLLM

**Load Balancing:**
```yaml
model_list:
  - model_name: claude-3-5-sonnet
    litellm_params:
      api_key: ${ANTHROPIC_KEY_1}
  - model_name: claude-3-5-sonnet
    litellm_params:
      api_key: ${ANTHROPIC_KEY_2}  # Load balance across keys
```

**Caching:**
```yaml
cache_settings:
  type: redis
  ttl: 3600  # Cache identical queries for 1 hour
```

### ClickHouse Query Optimization

**Best Practices:**
1. **Always filter by Timestamp** (partition pruning)
2. **Use materialized views** for repeated queries
3. **Limit results** (avoid scanning entire dataset)
4. **Use appropriate data types** (LowCardinality for high cardinality strings)
5. **Leverage indexes** (bloom filters, minmax)

**Example Optimized Query:**
```sql
-- Good: Uses materialized view + time filter
SELECT *
FROM otel_service_metrics_1m
WHERE timestamp_minute >= now() - INTERVAL 1 HOUR
LIMIT 100

-- Bad: Full table scan
SELECT * FROM otel_traces
```

---

## Cost Analysis

### Per-Workshop Costs (75 minutes, 20 participants)

**Infrastructure:**
- EC2 (t3.medium): $0.05
- ClickHouse Cloud (Development tier): $0.50
- Data transfer: $0.10

**LLM API:**
- Avg queries per participant: 10-15
- Tokens per query: ~2,000 (input + output)
- OpenAI GPT-4: ~$0.20/query → $4/participant → $80 total
- Anthropic Claude: ~$0.10/query → $2/participant → $40 total

**Total:** $40-85 per workshop

**Cost Optimization:**
- Use Claude Haiku for simple queries (10x cheaper)
- Cache common queries
- Set token limits
- Use materialized views (faster = cheaper)

---

## Monitoring the System

### Health Checks

**LibreChat:**
```bash
curl http://localhost:3000/api/health
```

**LiteLLM:**
```bash
curl http://localhost:4000/health
```

**ClickHouse:**
```bash
clickhouse-client --query "SELECT 1"
```

**Langfuse:**
```bash
curl -H "Authorization: Bearer $LANGFUSE_PUBLIC_KEY" \
  https://cloud.langfuse.com/api/public/health
```

### Observability Stack

**For the observability system itself:**

1. **Prometheus** (metrics)
   - LiteLLM exposes /metrics endpoint
   - Track request rates, latency, errors

2. **Grafana** (dashboards)
   - Visualize system health
   - Alert on anomalies

3. **Langfuse** (LLM traces)
   - Already built-in!
   - Monitor token usage, costs

---

## Extending the System

### Adding Custom Functions

**Example: Create Alert from Query Results**

```python
# In LiteLLM config
functions:
  - name: create_alert
    description: Create a PagerDuty alert
    parameters:
      type: object
      properties:
        severity: {type: string}
        message: {type: string}
```

### Integrating with Other Tools

**Slack Bot:**
- Replace LibreChat with Slack interface
- Same backend (LiteLLM + ClickHouse)

**CLI Tool:**
- Direct LiteLLM API calls
- Pipe results to jq for processing

**Grafana Plugin:**
- Natural language query in dashboard
- Auto-refresh results

---

## Troubleshooting Guide

See [SETUP_GUIDE.md](SETUP_GUIDE.md#troubleshooting) for common issues.

---

## References

- [ClickHouse Architecture](https://clickhouse.com/docs/en/development/architecture)
- [OpenTelemetry Data Model](https://opentelemetry.io/docs/specs/otel/overview/)
- [LiteLLM Documentation](https://docs.litellm.ai/docs/)
- [LibreChat Architecture](https://docs.librechat.ai/features/advanced/architecture)
- [Langfuse Tracing](https://langfuse.com/docs/tracing)

---

**For questions about the architecture, open an issue on GitHub.**
