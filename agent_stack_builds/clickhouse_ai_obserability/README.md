# AI-Powered Observability Analytics Workshop

> Query OpenTelemetry data using natural language - A hands-on workshop using ClickHouse, LiteLLM, LibreChat, and Langfuse

[![Workshop Duration](https://img.shields.io/badge/Duration-75%20minutes-blue)]()
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Cloud-yellow)]()
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)]()

## 🎯 Overview

This workshop teaches engineers how to analyze observability data using natural language instead of writing complex SQL queries. Participants interact with a LibreChat UI that uses **LiteLLM as the intelligent proxy** to route requests to LLMs (Claude/GPT-4), which convert questions into ClickHouse SQL queries against OpenTelemetry v2 data.

**Example:**
```
You: "Show me error rates by service in the last hour"
AI:  Generates and explains SQL query
     Returns formatted results
     Provides actionable insights
```

**Key Learning:** LiteLLM automatically sends all traces to your Langfuse Cloud project for complete LLMOps observability - you'll see every request, token usage, costs, and latency in real-time.

---

## ✨ Key Features

- 🗣️ **Natural language queries** - No SQL required
- 📊 **OpenTelemetry v2 schema** - Production-ready data model
- 🚀 **Fast performance** - Materialized views for sub-second queries
- 🔄 **LiteLLM proxy** - Intelligent routing with automatic Langfuse tracing
- 📈 **Full observability** - Every LLM request traced to your Langfuse project
- 🎓 **Complete workshop materials** - 75-minute structured curriculum
- 🐳 **Docker-based deployment** - Easy setup on AWS EC2
- 💰 **Cost-effective** - ~$50 per workshop

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     User Browser                        │
└──────────────────────────┬──────────────────────────────┘
                           │ HTTP :3000
                           │
┌──────────────────────────▼──────────────────────────────┐
│                      LibreChat                          │
│                   (Chat Interface)                      │
│          Stores: Conversations, User Auth               │
└──────────────────────────┬──────────────────────────────┘
                           │ HTTP :4000
                           │ (Configured as OpenAI endpoint)
                           │
┌──────────────────────────▼──────────────────────────────┐
│                       LiteLLM                           │
│                   (Core Workshop Component)             │
│  ┌────────────────────────────────────────────────┐    │
│  │ • Receives requests from LibreChat             │    │
│  │ • Injects system prompt (observability schema) │    │
│  │ • Routes to Claude/GPT-4                       │    │
│  │ • AUTOMATICALLY sends traces to Langfuse       │    │
│  │ • Tracks tokens, costs, latency                │    │
│  └────────────────────────────────────────────────┘    │
└─────┬─────────────────────┬────────────────────────────┘
      │                     │
      │ HTTPS               │ HTTPS
      │                     │ (Every request traced!)
      ▼                     ▼
┌─────────────┐      ┌──────────────────┐
│   LLM APIs  │      │ Langfuse Cloud   │
│             │      │ (Your Project)   │
│ • Claude    │      │                  │
│ • GPT-4     │      │ Receives:        │
│ • GPT-4o    │      │ • Full traces    │
│             │      │ • Token counts   │
│             │      │ • Cost data      │
│             │      │ • Latency        │
└─────────────┘      └──────────────────┘
      │
      │ (Queries ClickHouse for data)
      │
      ▼
┌─────────────────────────────┐
│     ClickHouse Cloud        │
│   (OpenTelemetry Data)      │
│                             │
│ • otel_traces               │
│ • otel_metrics              │
│ • otel_logs                 │
│ • otel_services             │
└─────────────────────────────┘
```

**Critical Flow:** LibreChat → LiteLLM → LLM APIs + Langfuse (parallel traces)

---

## 🚀 Quick Start

### Prerequisites

- AWS EC2 instance (t3.medium, Ubuntu 22.04)
- ClickHouse Cloud account (or sql.clickhouse.com access)
- Langfuse Cloud account (free tier)
- LLM API key (OpenAI or Anthropic)

### Installation

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/clickhouse_ai_observability.git
cd clickhouse_ai_observability/workshop_files

# Configure environment
cp .env.example .env
nano .env  # Add your credentials

# Start services
make start

# Verify setup
make test-clickhouse
make test-langfuse
```

### Access

Open browser to: `http://your-ec2-ip:3000`

---

## 📚 Documentation

| Document | Description | Audience |
|----------|-------------|----------|
| [workshop_files/README.md](workshop_files/README.md) | Workshop overview | All |
| [workshop_files/WORKSHOP_INDEX.md](workshop_files/WORKSHOP_INDEX.md) | Complete file guide | All |
| [workshop_files/docs/SETUP_GUIDE.md](workshop_files/docs/SETUP_GUIDE.md) | Step-by-step setup | Facilitators |
| [workshop_files/docs/FACILITATOR_GUIDE.md](workshop_files/docs/FACILITATOR_GUIDE.md) | Teaching guide | Facilitators |
| [workshop_files/docs/ARCHITECTURE.md](workshop_files/docs/ARCHITECTURE.md) | Technical deep-dive | Developers |
| [workshop_files/questions/sample_questions.md](workshop_files/questions/sample_questions.md) | 60+ example questions | Participants |

---

## 📋 Workshop Agenda (75 Minutes)

| Time | Topic | Activities |
|------|-------|------------|
| 0-10 min | Setup & Introduction | Architecture overview, access verification |
| 10-25 min | Basic Observability | Service discovery, error analysis |
| 25-45 min | Advanced Analytics | Latency analysis, performance bottlenecks |
| 45-60 min | Distributed Tracing | Service dependencies, root cause analysis |
| 60-70 min | Production Best Practices | SLA compliance, query optimization |
| 70-75 min | Q&A | Open discussion |

---

## 💡 Sample Questions

### Beginner
- "What services are currently running?"
- "Show me error rates by service"
- "Which service has the highest latency?"

### Intermediate
- "Find the 10 slowest traces in the last hour"
- "Show me database queries slower than 1 second"
- "Compare latency today vs yesterday"

### Advanced
- "Build a service dependency map"
- "Why is the payment service slow today?"
- "Find traces where database calls took >80% of total time"
- "Are any services violating their SLA?"

See [60+ more examples](workshop_files/questions/sample_questions.md)

---

## 🗂️ Data Model

### Tables

- **otel_traces** - Distributed trace spans (TraceId, SpanId, Duration, Status)
- **otel_metrics** - Time-series metrics (Gauges, Counters, Histograms)
- **otel_logs** - Structured application logs (Severity, Body, Attributes)
- **otel_services** - Service catalog with SLA definitions

### Materialized Views

- **otel_service_metrics_1m** - Per-minute service metrics
- **otel_error_logs_summary** - Error pattern aggregations
- **otel_service_dependencies** - Service call graphs
- **otel_http_endpoint_metrics** - Endpoint performance
- **otel_service_health_hourly** - Historical health data

See [complete schema](workshop_files/sql/schema.sql)

---

## 🎓 Learning Objectives

Participants will learn to:

1. Query OpenTelemetry data using natural language
2. Analyze service performance, errors, and latency
3. Build service dependency graphs
4. Check SLA compliance
5. Understand AI agent architecture (LibreChat → LiteLLM → LLMs + Langfuse)
6. Configure LiteLLM to automatically send traces to Langfuse
7. Optimize ClickHouse queries for observability
8. Monitor LLM usage, costs, and latency in Langfuse dashboards

---

## 💰 Cost Estimate

**Per 75-minute workshop (20 participants):**

- EC2 (t3.medium): $0.05
- ClickHouse Cloud (Development): $0.50
- Langfuse Cloud: Free
- LLM API (Anthropic Claude): ~$40
- LLM API (OpenAI GPT-4): ~$80

**Total: $40-85**

---

## 🛠️ Tech Stack

- **LibreChat** - Web-based chat UI (React, Node.js)
- **LiteLLM** - LLM proxy with observability (Python, FastAPI)
- **ClickHouse** - OLAP database for analytics
- **Langfuse** - LLMOps tracing platform
- **MongoDB** - LibreChat data storage
- **PostgreSQL** - LiteLLM configuration
- **Docker Compose** - Service orchestration

---

## 🔧 Configuration

### Key Files

- `.env` - Environment variables (copy from `.env.example`)
- `config/system_prompt.txt` - AI agent instructions (10KB)
- `config/litellm_config.yaml` - LLM proxy settings
- `config/librechat.yaml` - Chat UI customization
- `sql/schema.sql` - Table definitions
- `sql/materialized_views.sql` - Pre-aggregated views

### Customization Points

1. **System Prompt** - Modify for your domain
2. **Sample Questions** - Match your services
3. **Model Selection** - Claude vs GPT-4
4. **Workshop Duration** - 60-120 minutes

---

## 📊 Example Query Flow

```
User: "Show me error rates by service in the last hour"
  ↓
LibreChat → LiteLLM
  ↓
System Prompt + User Question → Claude 3.5 Sonnet
  ↓
Generated SQL:
  SELECT
    ServiceName,
    countIf(StatusCode = 'ERROR') / count() as error_rate
  FROM otel_traces
  WHERE Timestamp >= now() - INTERVAL 1 HOUR
  GROUP BY ServiceName
  ORDER BY error_rate DESC
  ↓
Response with explanation + query + insights
  ↓
Trace logged to Langfuse (tokens, cost, latency)
```

---

## 🎯 Use Cases

### Workshop & Training
- Internal SRE training
- Customer education
- Conference workshops
- University courses

### Production Deployment
- Internal observability portal
- Customer-facing analytics
- On-call investigation tool
- Incident response assistant

---

## 🚦 Getting Started

### For Workshop Facilitators

1. **Read** [SETUP_GUIDE.md](workshop_files/docs/SETUP_GUIDE.md) (30-45 min)
2. **Deploy** infrastructure using `make start`
3. **Review** [FACILITATOR_GUIDE.md](workshop_files/docs/FACILITATOR_GUIDE.md)
4. **Test** with [sample questions](workshop_files/questions/sample_questions.md)
5. **Deliver** workshop following structured agenda

### For Developers

1. **Read** [ARCHITECTURE.md](workshop_files/docs/ARCHITECTURE.md)
2. **Customize** system prompt for your domain
3. **Extend** with custom functions
4. **Deploy** to production with HTTPS, auth, etc.

### For Participants

1. **Access** LibreChat URL from facilitator
2. **Create** account
3. **Start** with simple questions
4. **Experiment** and iterate

---

## 🎥 Workshop Recording

(Add link to recorded workshop if available)

---

## 🤝 Contributing

Contributions welcome! Areas for improvement:

- Additional sample questions
- New use cases (metrics, logs focus)
- Performance optimizations
- Documentation improvements
- Bug fixes

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📜 License

MIT License - see [LICENSE](LICENSE) file for details

---

## 🙏 Acknowledgments

- Inspired by ClickHouse workshop patterns for AI agent development
- Built on OpenTelemetry observability standards
- Powered by LibreChat, LiteLLM, and Langfuse

---

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/YOUR_USERNAME/clickhouse_ai_observability/issues)
- **Discussions**: [GitHub Discussions](https://github.com/YOUR_USERNAME/clickhouse_ai_observability/discussions)
- **Community**: [ClickHouse Slack](https://clickhouse.com/slack)

---

## 🌟 Star History

If you find this workshop useful, please star the repository!

---

## 📚 Related Projects

- [ClickHouse](https://github.com/ClickHouse/ClickHouse)
- [OpenTelemetry](https://github.com/open-telemetry)
- [LibreChat](https://github.com/danny-avila/LibreChat)
- [LiteLLM](https://github.com/BerriAI/litellm)
- [Langfuse](https://github.com/langfuse/langfuse)

---

**Ready to build AI-powered observability? Start with the [Setup Guide](workshop_files/docs/SETUP_GUIDE.md)!** 🚀
