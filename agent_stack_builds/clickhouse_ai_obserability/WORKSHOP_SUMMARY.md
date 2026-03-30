# AI-Powered Observability Workshop - Complete Package

## 🎉 Workshop Created Successfully!

I've created a complete 75-minute workshop on AI-powered observability analytics using ClickHouse, LibreChat, and Langfuse Cloud.

---

## 📦 What's Included

### Complete Workshop Package (15 files, ~100KB)

```
workshop_files/
├── README.md                          # Main workshop overview
├── WORKSHOP_INDEX.md                  # Complete file guide
├── .env.example                       # Configuration template
├── docker-compose.yml                 # Service definitions
├── Makefile                           # Automation commands
│
├── config/                            # Configuration files
│   ├── librechat.yaml                # LibreChat UI setup
│   ├── litellm_config.yaml           # LLM proxy config
│   └── system_prompt.txt             # AI agent instructions (10KB)
│
├── sql/                               # Database schemas
│   ├── schema.sql                    # OpenTelemetry v2 tables
│   └── materialized_views.sql        # Pre-aggregated views
│
├── questions/                         # Workshop materials
│   └── sample_questions.md           # 60+ example questions
│
└── docs/                              # Complete documentation
    ├── SETUP_GUIDE.md                # Step-by-step setup (20KB)
    ├── FACILITATOR_GUIDE.md          # Teaching guide (18KB)
    └── ARCHITECTURE.md               # Technical deep-dive (16KB)
```

---

## 🎯 Workshop Overview

**Duration:** 75 minutes (configurable: 60-120 min)

**Audience:** Engineers, SREs, Platform teams

**Prerequisites:** Basic SQL, observability concepts

**Infrastructure:**
- AWS EC2 (t3.medium)
- ClickHouse Cloud (or sql.clickhouse.com)
- Langfuse Cloud (free tier)
- LLM Provider (OpenAI or Anthropic)

---

## 🏗️ Architecture

```
User Browser
    ↓ HTTP (port 3000)
LibreChat (Chat UI)
    ↓ HTTP (port 4000)
LiteLLM (LLM Proxy)
    ↓ HTTPS
┌───────────────────┐
│ Claude / GPT-4    │ (generates SQL)
│ Langfuse Cloud    │ (traces all requests)
│ ClickHouse Cloud  │ (stores OpenTelemetry data)
└───────────────────┘
```

**Tech Stack:**
- **LibreChat:** React chat UI
- **LiteLLM:** Python LLM proxy with Langfuse integration
- **ClickHouse:** OLAP database for OpenTelemetry data
- **Langfuse:** LLMOps observability platform
- **Docker Compose:** Local orchestration

---

## 🚀 Quick Start

### For Workshop Facilitators

1. **Setup (30-45 minutes)**
   ```bash
   cd workshop_files
   cp .env.example .env
   # Edit .env with your credentials
   make start
   make test-env
   ```

2. **Prepare (1 hour)**
   - Read `docs/FACILITATOR_GUIDE.md`
   - Review `questions/sample_questions.md`
   - Test 5-10 queries

3. **Deliver (75 minutes)**
   - Follow the structured agenda
   - Use sample questions
   - Encourage experimentation

4. **Teardown (10 minutes)**
   ```bash
   make stop
   # Terminate EC2 instance
   ```

### For Participants

1. Access LibreChat URL (provided by facilitator)
2. Create account
3. Start asking questions:
   - "What services are currently running?"
   - "Show me error rates by service"
   - "Find the slowest traces"

---

## 📚 Key Features

### 1. Complete OpenTelemetry Schema

**4 Core Tables:**
- `otel_traces` - Distributed trace spans
- `otel_metrics` - Time-series metrics
- `otel_logs` - Structured logs
- `otel_services` - Service catalog with SLAs

**6 Materialized Views:**
- Per-minute service metrics
- Error log aggregations
- Service dependency graphs
- HTTP endpoint performance
- Database query metrics
- Hourly health rollups

### 2. Comprehensive System Prompt

10KB system prompt teaching the AI agent:
- Complete schema documentation
- Query patterns and best practices
- Response formatting guidelines
- Visualization recommendations

### 3. 60+ Sample Questions

Organized by difficulty and topic:
- Service discovery (5 questions)
- Error analysis (12 questions)
- Latency & performance (15 questions)
- Distributed tracing (15 questions)
- SLA compliance (8 questions)
- Time-series analysis (9 questions)
- Advanced analytics (10 questions)

### 4. Full Observability Stack

- **Query tracing** via Langfuse
- **Token usage tracking**
- **Cost monitoring**
- **Latency analysis**
- **Error rate monitoring**

"We're using observability to observe observability!"

---

## 💡 Learning Objectives

Participants will learn to:

1. ✅ Query OpenTelemetry data using natural language
2. ✅ Analyze service performance and errors
3. ✅ Build service dependency graphs
4. ✅ Check SLA compliance
5. ✅ Understand AI agent architecture
6. ✅ Optimize ClickHouse queries
7. ✅ Trace LLM requests with Langfuse

---

## 📖 Documentation Highlights

### SETUP_GUIDE.md (20KB)
Complete step-by-step instructions:
- AWS EC2 provisioning
- Docker installation
- ClickHouse Cloud setup
- Langfuse integration
- Environment configuration
- Verification tests
- Troubleshooting guide

**Time to setup:** 30-45 minutes

### FACILITATOR_GUIDE.md (18KB)
Workshop delivery guide:
- Minute-by-minute agenda (75 min)
- Teaching points for each section
- Interactive exercises
- Engagement strategies
- Common pitfalls
- Technical issue handling
- Success metrics

**Essential reading for facilitators!**

### ARCHITECTURE.md (16KB)
Technical deep-dive:
- Component architecture
- Data flow diagrams
- Security considerations
- Performance optimization
- Cost analysis (~$50/workshop)
- Scaling strategies
- Extension patterns

**For developers and architects**

---

## 🎓 Workshop Agenda (75 Minutes)

### Part 1: Setup & Introduction (10 min)
- Welcome and context
- Architecture overview
- Access verification
- First query together

### Part 2: Basic Observability (15 min)
- Service discovery
- Error rate analysis
- Request volume patterns

### Part 3: Advanced Analytics (20 min)
- Latency distributions (p50, p95, p99)
- Performance bottlenecks
- Database query analysis

### Part 4: Distributed Tracing (15 min)
- Trace anatomy
- Service dependencies
- Root cause analysis

### Part 5: Production Best Practices (10 min)
- SLA compliance
- Query optimization
- Langfuse trace analysis
- Cost considerations

### Part 6: Q&A (5 min)

---

## 💰 Cost Estimate

**Per 75-minute workshop (20 participants):**

- EC2 (t3.medium): $0.05
- ClickHouse Cloud: $0.50
- Langfuse Cloud: Free
- LLM API (Claude): ~$40
- LLM API (GPT-4): ~$80

**Total: $40-85 per workshop**

**Cost optimization:**
- Use Claude Haiku for simple queries (10x cheaper)
- Cache common queries
- Set token limits
- Use materialized views

---

## 🔒 Security Considerations

### Workshop Mode
- Temporary EC2 instance
- Read-only ClickHouse access
- Short-lived credentials
- HTTP acceptable (demo only)

### Production Mode
- Use AWS ALB with HTTPS
- Rotate API keys regularly
- Implement proper authentication
- Use AWS Secrets Manager
- Restrict network access
- Enable audit logging

---

## 🛠️ Customization Points

### 1. System Prompt (High Impact)
Modify `config/system_prompt.txt`:
- Add domain terminology
- Include custom metrics
- Adjust query patterns
- Change visualization preferences

### 2. Sample Questions
Update `questions/sample_questions.md`:
- Match your services
- Use your metric names
- Reflect your use cases

### 3. Model Selection
Edit `config/litellm_config.yaml`:
- Claude 3.5 Sonnet (best quality)
- GPT-4 Turbo (balanced)
- Claude Haiku (cheapest)

### 4. Workshop Duration
Adjust in `docs/FACILITATOR_GUIDE.md`:
- 60 min (skip advanced topics)
- 75 min (standard)
- 90-120 min (deep dives)

---

## 📊 Sample Queries

### Basic
```
"What services are currently running?"
"Show me error rates by service in the last hour"
"Which service has the highest latency?"
```

### Intermediate
```
"Find the 10 slowest traces"
"Show me database queries slower than 1 second"
"Compare latency today vs yesterday"
```

### Advanced
```
"Build a service dependency map"
"Why is the payment service slow today?"
"Find traces where database calls took >80% of total time"
"Are any services violating their SLA?"
```

---

## 🎯 Success Metrics

### During Workshop
- [ ] 90%+ participants complete first query
- [ ] 5+ questions during Q&A
- [ ] No extended downtime (>5 min)
- [ ] Positive engagement

### Post-Workshop
- [ ] Survey response rate >50%
- [ ] Satisfaction score >4/5
- [ ] 30%+ express interest in implementation

---

## 🔧 Troubleshooting Quick Reference

### Container won't start
```bash
make logs-[service]
make restart
```

### ClickHouse connection failed
```bash
make test-clickhouse
# Check .env credentials
```

### Langfuse not receiving traces
```bash
make test-langfuse
docker logs litellm | grep langfuse
```

### Slow queries
- Reduce time window
- Use materialized views
- Add LIMIT clause

---

## 🌟 Next Steps

### After Workshop

**For Participants:**
1. Clone repository
2. Experiment with your own data
3. Customize system prompt
4. Join ClickHouse community

**For Facilitators:**
1. Export Langfuse traces
2. Gather feedback
3. Update materials
4. Schedule follow-up sessions

**For Developers:**
1. Review ARCHITECTURE.md
2. Plan production deployment
3. Implement custom functions
4. Integrate with existing tools

---

## 📚 Additional Resources

### Official Documentation
- [ClickHouse Docs](https://clickhouse.com/docs)
- [OpenTelemetry Spec](https://opentelemetry.io/docs/specs/otel/)
- [LibreChat Docs](https://docs.librechat.ai/)
- [LiteLLM Docs](https://docs.litellm.ai/)
- [Langfuse Docs](https://langfuse.com/docs)

### Community
- [ClickHouse Slack](https://clickhouse.com/slack)
- [OpenTelemetry Slack](https://cloud-native.slack.com)

---

## 🤝 Contributing

This workshop is open source. Contributions welcome!

**Areas for contribution:**
- Additional sample questions
- New use cases
- Performance improvements
- Documentation updates
- Bug fixes

---

## 📝 Files You Need to Edit

Before running the workshop, you MUST configure:

1. **`.env`** (copy from `.env.example`)
   - ClickHouse credentials
   - Langfuse API keys
   - LLM provider API key
   - Generate JWT secrets

2. **Optional customizations:**
   - `config/system_prompt.txt` (for your domain)
   - `questions/sample_questions.md` (for your services)
   - `config/litellm_config.yaml` (model preferences)

---

## ✅ Pre-Workshop Checklist

### 1 Week Before
- [ ] Complete setup (docs/SETUP_GUIDE.md)
- [ ] Test end-to-end queries
- [ ] Review facilitator guide
- [ ] Prepare slides

### 1 Day Before
- [ ] Verify all services running
- [ ] Test 5-10 sample questions
- [ ] Check Langfuse traces
- [ ] Confirm participant access

### Day Of
- [ ] Start services 30 min early
- [ ] Test one query
- [ ] Prepare browser tabs
- [ ] Have backup queries ready

---

## 🎊 You're Ready!

You now have everything needed to run a successful AI-powered observability workshop:

✅ Complete infrastructure setup
✅ Production-quality SQL schemas
✅ 60+ sample questions
✅ Comprehensive documentation
✅ Teaching materials
✅ Troubleshooting guides

**Time to workshop deployment:** 30-45 minutes
**Time to deliver workshop:** 75 minutes
**Participant satisfaction:** High (if you follow the guide!)

---

## 📞 Support

For questions or issues:
- GitHub Issues: (your repository URL)
- Email: (your support email)
- Community Slack channels

---

## 🏆 Workshop Goals Achieved

This workshop demonstrates:

1. **AI simplifies observability** - Natural language > complex SQL
2. **ClickHouse performance** - Fast analytics on massive datasets
3. **Production patterns** - Real-world architecture and best practices
4. **Full observability** - Even the AI agent is monitored!
5. **Practical skills** - Participants can implement this

---

**Ready to run your workshop? Start with `workshop_files/docs/SETUP_GUIDE.md`!** 🚀

---

*Created: March 30, 2026*
*Architecture: LibreChat → LiteLLM (with Langfuse tracing) → Claude/GPT-4 → ClickHouse*
*Use case: OpenTelemetry observability data analysis*
*Duration: 75 minutes*
*Audience: Engineers, SREs, Platform teams*
