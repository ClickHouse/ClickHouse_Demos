# AI-Powered Observability Analytics Workshop

A hands-on 75-minute workshop demonstrating how to build an AI agent using **LiteLLM** that queries OpenTelemetry data in ClickHouse using natural language, with automatic tracing to Langfuse.

## 🎯 Workshop Overview

Participants will learn how to:
- Deploy a production-ready AI chat interface (LibreChat) on EC2
- Configure **LiteLLM as the intelligent proxy** to route LLM requests
- Connect to ClickHouse Cloud with OpenTelemetry v2 schema
- Use LLMs (Claude/GPT-4) to convert natural language questions into SQL queries
- **Automatically trace all requests to Langfuse Cloud** (every token, cost, latency)
- Analyze service performance, errors, and latency using conversational queries

**Key Focus:** Understanding how LiteLLM handles LLM routing and automatic Langfuse tracing

**Duration:** 75 minutes
**Level:** Intermediate
**Prerequisites:** Basic SQL knowledge, familiarity with observability concepts

## 🏗️ Architecture

```mermaid
flowchart TD
    Browser["🌐 User Browser"]
    Nginx["🔒 Nginx Reverse Proxy\nTLS termination"]
    LibreChat["💬 LibreChat\nChat Interface\nStores: Conversations, User Auth"]
    LiteLLM["⚡ LiteLLM\nCore Workshop Component\n\n• Receives requests from LibreChat\n• Injects system prompt (observability schema)\n• Routes to Claude / GPT-4\n• AUTOMATICALLY sends traces to Langfuse\n• Tracks tokens, costs, latency"]
    LLMs["🤖 LLM APIs\nClaude Sonnet 4.6\nGemini 2.5 Pro/Flash"]
    Langfuse["📊 Langfuse Cloud\nTraces · Tokens\nCosts · Latency · Scores"]
    ClickHouse["🏠 ClickHouse Cloud\notel_traces\notel_metrics\notel_logs"]
    MCP["🔌 MCP Server\nClickHouse connector\nSSE transport"]

    Browser -->|"HTTPS :443"| Nginx
    Nginx -->|"HTTP :3000"| LibreChat
    LibreChat -->|"HTTP :4000\nConfigured as OpenAI endpoint"| LiteLLM
    LiteLLM -->|"HTTPS"| LLMs
    LiteLLM -->|"HTTPS traces"| Langfuse
    LLMs -->|"SQL via MCP"| MCP
    MCP -->|"HTTPS :8443"| ClickHouse

    style LiteLLM fill:#f5a623,color:#000
    style ClickHouse fill:#FAFF69,color:#000
    style Langfuse fill:#6366f1,color:#fff
    style Nginx fill:#009639,color:#fff
```

**Workshop Focus:** LibreChat → **LiteLLM** (automatic Langfuse tracing) → LLMs

## 📊 Data Model

The workshop uses OpenTelemetry v2 schema on `sql.clickhouse.com`:

- **otel_traces** - Distributed traces with span details
- **otel_metrics** - Service metrics (latency, throughput, errors)
- **otel_logs** - Structured application logs
- **otel_services** - Service catalog and metadata

## 🚀 Quick Start

### Prerequisites

1. **AWS EC2 Instance**
   - Instance type: t3.medium or larger (2 vCPU, 4GB RAM minimum)
   - Ubuntu 22.04 LTS
   - Security group: Allow inbound on ports 22, 3000, 4000
   - 20GB storage minimum

2. **ClickHouse Cloud Account**
   - Access to `sql.clickhouse.com` or your own ClickHouse Cloud instance
   - Connection details (host, port, username, password)

3. **Langfuse Cloud Account**
   - Sign up at https://cloud.langfuse.com
   - Create a project and get API keys

4. **LLM API Key**
   - OpenAI API key OR Anthropic API key

### Installation

#### Step 1: Connect to EC2

```bash
ssh -i your-key.pem ubuntu@your-ec2-ip
```

#### Step 2: Install Docker

```bash
# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

#### Step 3: Clone Workshop Repository

```bash
git clone https://github.com/ClickHouse/ClickHouse_Demos.git
cd ClickHouse_Demos/agent_stack_builds/clickhouse_ai_obserability/workshop_files
```

#### Step 4: Configure Environment

```bash
cp .env.example .env
nano .env
```

Update the following variables:
```bash
# ClickHouse Connection
CLICKHOUSE_HOST=your-clickhouse-host.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=default

# Langfuse Cloud
LANGFUSE_PUBLIC_KEY=pk-lf-xxx
LANGFUSE_SECRET_KEY=sk-lf-xxx
LANGFUSE_HOST=https://cloud.langfuse.com

# LLM Provider (choose one)
OPENAI_API_KEY=sk-xxx
# OR
ANTHROPIC_API_KEY=sk-ant-xxx

# LibreChat
LIBRECHAT_PORT=3000
```

#### Step 5: Start Services

```bash
make start
```

Wait 30-60 seconds for services to initialize.

#### Step 6: Access LibreChat

Open your browser to: `http://your-ec2-ip:3000`

**Workshop login credentials:**

| Field    | Value                        |
|----------|------------------------------|
| Email    | `workshop@clickhouse.com`    |
| Password | `observability2024`          |

## 📚 Workshop Agenda

### Part 1: Setup & Introduction (10 minutes)
- Architecture overview
- Environment setup verification
- OpenTelemetry data model introduction

### Part 2: Basic Observability Queries (15 minutes)
- Service discovery and health checks
- Error rate analysis
- Latency percentile queries
- Request volume patterns

### Part 3: Advanced Analytics (20 minutes)
- Distributed trace analysis
- Service dependency mapping
- Root cause analysis
- SLA compliance checking

### Part 4: Custom Metrics & Aggregations (15 minutes)
- Time-series aggregations
- Multi-dimensional analysis
- Anomaly detection patterns
- Custom materialized views

### Part 5: Production Best Practices (10 minutes)
- Query optimization tips
- Langfuse trace analysis
- Cost and performance considerations
- Next steps and resources

### Part 6: Q&A (5 minutes)

## 💡 Example Questions

Once LibreChat is running, try these questions:

**Service Health:**
- "What services are currently running?"
- "Show me error rates by service in the last hour"
- "Which service has the highest p99 latency?"

**Distributed Tracing:**
- "Find the slowest traces in the last 24 hours"
- "Show me all failed requests for the checkout service"
- "What's the average trace duration by endpoint?"

**Root Cause Analysis:**
- "Why is the payment service slow today?"
- "Find traces with database query duration > 5 seconds"
- "Show me error logs for failed authentication attempts"

**Performance Monitoring:**
- "Compare today's throughput vs last week for the API gateway"
- "What's the error rate trend over the past 7 days?"
- "Show me services violating SLA (p95 > 1000ms)"

See [questions/sample_questions.md](questions/sample_questions.md) for 50+ more examples.

## 🔍 How It Works

1. **User asks question** in natural language via LibreChat UI
2. **LiteLLM forwards** request to Claude/GPT with system prompt
3. **LLM generates SQL** query based on OpenTelemetry schema
4. **Query executes** against ClickHouse Cloud
5. **Results formatted** and returned to user
6. **Langfuse traces** entire pipeline for observability

The system prompt provides:
- OpenTelemetry schema definitions
- Query patterns and examples
- Visualization templates (tables, charts)
- Best practices for ClickHouse queries

## 📁 Project Structure

```
workshop_files/
├── README.md                 # This file
├── docker-compose.yml        # Service definitions
├── Makefile                  # Workshop commands
├── .env.example              # Environment template
├── config/
│   ├── librechat.yaml       # LibreChat configuration
│   ├── litellm_config.yaml  # LiteLLM proxy config
│   └── system_prompt.txt    # AI agent instructions
├── sql/
│   ├── schema.sql           # Table definitions
│   ├── materialized_views.sql
│   └── sample_data.sql      # Test data (optional)
├── questions/
│   ├── sample_questions.md  # Pre-built questions
│   └── exercises.md         # Workshop exercises
└── docs/
    ├── SETUP_GUIDE.md       # Detailed setup
    ├── TROUBLESHOOTING.md   # Common issues
    └── ARCHITECTURE.md      # Technical deep-dive
```

## 🎓 Learning Objectives

By the end of this workshop, participants will be able to:

1. **Deploy AI agent infrastructure** using Docker on cloud instances
2. **Connect multiple cloud services** (ClickHouse, Langfuse, LLM providers)
3. **Design effective system prompts** for SQL generation
4. **Query OpenTelemetry data** using natural language
5. **Optimize ClickHouse queries** for observability use cases
6. **Monitor AI agent behavior** using Langfuse tracing
7. **Build production-ready observability solutions** with AI

## 🛠️ Makefile Commands

```bash
make start          # Start all services
make stop           # Stop all services
make restart        # Restart all services
make logs           # View logs (all services)
make logs-librechat # View LibreChat logs
make logs-litellm   # View LiteLLM logs
make status         # Check service health
make clean          # Stop and remove all containers
make reset          # Clean and restart fresh
```

## 🔧 Troubleshooting

### LibreChat not accessible
```bash
# Check if container is running
docker ps | grep librechat

# View logs
make logs-librechat

# Verify port is open
sudo netstat -tlnp | grep 3000
```

### LiteLLM connection errors
```bash
# Test ClickHouse connection
docker exec -it litellm curl -v "https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"

# Check LiteLLM logs
make logs-litellm
```

### Langfuse not receiving traces
- Verify API keys in `.env`
- Check that `LANGFUSE_HOST=https://cloud.langfuse.com` (no trailing slash)
- View traces in Langfuse dashboard: https://cloud.langfuse.com

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for more details.

## 📖 Additional Resources

- [ClickHouse Documentation](https://clickhouse.com/docs)
- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)
- [LibreChat Documentation](https://docs.librechat.ai/)
- [Langfuse Documentation](https://langfuse.com/docs)
- [LiteLLM Documentation](https://docs.litellm.ai/)

## 🤝 Contributing

This workshop is open source. Contributions welcome!

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📝 License

MIT License - see LICENSE file for details

## 💬 Support

- GitHub Issues: https://github.com/ClickHouse/ClickHouse_Demos/issues
- ClickHouse Community Slack: https://clickhouse.com/slack

---

**Ready to start?** Head to [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) for detailed instructions.
