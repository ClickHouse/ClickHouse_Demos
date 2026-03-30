# Observability Workshop - Complete File Index

This document provides a complete overview of all workshop materials and how to use them.

---

## Quick Navigation

### For Participants
- **Start here:** [README.md](README.md)
- **Sample questions:** [questions/sample_questions.md](questions/sample_questions.md)

### For Facilitators
- **Setup instructions:** [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md)
- **Teaching guide:** [docs/FACILITATOR_GUIDE.md](docs/FACILITATOR_GUIDE.md)
- **Technical details:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

### For Developers
- **Docker setup:** [docker-compose.yml](docker-compose.yml)
- **Make commands:** [Makefile](Makefile)
- **SQL schemas:** [sql/schema.sql](sql/schema.sql)

---

## File Structure

```
workshop_files/
├── README.md                          # Main workshop overview
├── WORKSHOP_INDEX.md                  # This file
├── .env.example                       # Environment configuration template
├── docker-compose.yml                 # Service definitions
├── Makefile                           # Workshop automation commands
│
├── config/                            # Configuration files
│   ├── librechat.yaml                # LibreChat UI configuration
│   ├── litellm_config.yaml           # LiteLLM proxy configuration
│   └── system_prompt.txt             # AI agent system prompt
│
├── sql/                               # Database schemas
│   ├── schema.sql                    # Table definitions
│   └── materialized_views.sql        # Pre-aggregated views
│
├── questions/                         # Workshop exercises
│   └── sample_questions.md           # 60+ example questions
│
└── docs/                              # Documentation
    ├── SETUP_GUIDE.md                # Complete setup instructions
    ├── FACILITATOR_GUIDE.md          # Teaching and facilitation guide
    └── ARCHITECTURE.md               # Technical architecture deep-dive
```

---

## File Descriptions

### Core Files

#### [README.md](README.md) (10KB)
**Purpose:** Workshop introduction and quick start guide

**Contents:**
- Workshop overview and objectives
- Architecture diagram
- Quick start instructions
- Example questions
- Learning objectives

**Who needs this:** Everyone (participants, facilitators, developers)

**When to use:** First document to read

---

#### [.env.example](.env.example) (5KB)
**Purpose:** Environment configuration template

**Contents:**
- ClickHouse connection details
- Langfuse Cloud API keys
- LLM provider API keys (OpenAI/Anthropic)
- LibreChat settings
- Security configurations

**Who needs this:** Facilitators, developers

**When to use:** During initial setup (copy to `.env` and fill in values)

**Important:** Never commit `.env` with real credentials to version control!

---

#### [docker-compose.yml](docker-compose.yml) (4KB)
**Purpose:** Defines all workshop services

**Services:**
- `librechat` - Chat UI (port 3000)
- `litellm` - LLM proxy (port 4000)
- `mongodb` - LibreChat database
- `postgres` - LiteLLM database
- `meilisearch` - Search engine

**Who needs this:** Facilitators, developers

**When to use:** Starting/stopping workshop services

---

#### [Makefile](Makefile) (8KB)
**Purpose:** Automation commands for workshop management

**Key commands:**
```bash
make start          # Start all services
make stop           # Stop all services
make status         # Check health
make logs           # View logs
make test-clickhouse # Test ClickHouse
make test-langfuse   # Test Langfuse
make clean          # Remove containers
```

**Who needs this:** Facilitators

**When to use:** Throughout setup, workshop, and teardown

---

### Configuration Files

#### [config/librechat.yaml](config/librechat.yaml) (5KB)
**Purpose:** LibreChat UI customization

**Contents:**
- Model definitions (Claude, GPT-4, etc.)
- System messages
- UI customization
- Rate limits
- Preset configurations

**Customization points:**
- Model selection
- System prompts
- Example questions
- Branding

---

#### [config/litellm_config.yaml](config/litellm_config.yaml) (6KB)
**Purpose:** LLM proxy configuration

**Contents:**
- Model routing
- Langfuse integration
- Fallback logic
- Cost tracking
- Function definitions

**Customization points:**
- Add/remove models
- Adjust timeouts
- Configure caching
- Set budgets

---

#### [config/system_prompt.txt](config/system_prompt.txt) (10KB)
**Purpose:** AI agent instructions for SQL generation

**Contents:**
- Database schema documentation
- Query patterns and examples
- Best practices
- Response formatting

**Customization points:**
- Add domain-specific terminology
- Include custom metrics
- Modify query patterns
- Add visualization hints

**Impact:** This is the MOST IMPORTANT file for query quality!

---

### SQL Files

#### [sql/schema.sql](sql/schema.sql) (10KB)
**Purpose:** OpenTelemetry v2 table definitions

**Tables:**
1. `otel_traces` - Distributed trace spans
2. `otel_metrics` - Time-series metrics
3. `otel_logs` - Structured logs
4. `otel_services` - Service catalog with SLAs

**Features:**
- Optimized indexes
- Compression settings
- TTL policies
- Example queries (commented)

**Who needs this:** Developers, ClickHouse administrators

**When to use:** Setting up new ClickHouse instance

---

#### [sql/materialized_views.sql](sql/materialized_views.sql) (8KB)
**Purpose:** Pre-aggregated views for performance

**Views:**
1. `otel_service_metrics_1m` - Per-minute rollups
2. `otel_error_logs_summary` - Error patterns
3. `otel_service_dependencies` - Service call graph
4. `otel_http_endpoint_metrics` - Endpoint performance
5. `otel_service_health_hourly` - Historical health
6. `otel_active_services` - Currently active services

**Benefits:**
- 10-100x faster queries
- Reduced costs
- Real-time dashboards

---

### Workshop Materials

#### [questions/sample_questions.md](questions/sample_questions.md) (11KB)
**Purpose:** 60+ example questions for workshop exercises

**Organization:**
- Part 1: Service Discovery (5 questions)
- Part 2: Error Analysis (12 questions)
- Part 3: Latency & Performance (15 questions)
- Part 4: Distributed Tracing (15 questions)
- Part 5: SLA & Compliance (8 questions)
- Part 6: Time-Series & Trends (9 questions)
- Part 7: Advanced Analytics (10 questions)

**Includes:**
- Difficulty progression
- Expected outcomes
- Answer key (SQL queries)
- Facilitator notes

**Who needs this:** Facilitators, participants

**When to use:** During workshop exercises

---

### Documentation

#### [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) (20KB)
**Purpose:** Complete step-by-step setup instructions

**Sections:**
1. Prerequisites
2. AWS EC2 setup
3. ClickHouse Cloud setup
4. Langfuse Cloud setup
5. LLM provider setup
6. Workshop deployment
7. Verification tests
8. Troubleshooting
9. Teardown

**Who needs this:** Facilitators (mandatory reading!)

**Time required:** 30-45 minutes to follow

**Important:** Contains security considerations and cost estimates

---

#### [docs/FACILITATOR_GUIDE.md](docs/FACILITATOR_GUIDE.md) (18KB)
**Purpose:** Workshop facilitation and teaching guide

**Sections:**
1. Workshop goals and objectives
2. Pre-workshop preparation
3. Detailed 75-minute agenda
4. Facilitation tips
5. Handling technical issues
6. Measuring success
7. Advanced topics

**Who needs this:** Workshop facilitators

**When to use:**
- 1 week before (preparation)
- Day before (review)
- During workshop (reference)

**Key features:**
- Minute-by-minute timing
- Teaching points for each section
- Interactive exercise ideas
- Common pitfalls to avoid
- Energy management tips

---

#### [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (16KB)
**Purpose:** Technical deep-dive into system architecture

**Sections:**
1. System overview
2. Component details (LibreChat, LiteLLM, Langfuse, ClickHouse)
3. Data flow (end-to-end query trace)
4. Security considerations
5. Performance optimization
6. Cost analysis
7. Monitoring and observability
8. Extending the system

**Who needs this:** Developers, architects

**When to use:**
- Understanding implementation details
- Troubleshooting complex issues
- Planning production deployment
- Extending functionality

---

## Usage Scenarios

### Scenario 1: First-Time Setup (Facilitator)

**Goal:** Get workshop running for the first time

**Path:**
1. Read [README.md](README.md) (5 min)
2. Follow [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) (45 min)
3. Copy `.env.example` to `.env` and configure (10 min)
4. Run `make start` and verify (5 min)
5. Test with questions from [questions/sample_questions.md](questions/sample_questions.md) (10 min)

**Total time:** ~75 minutes

---

### Scenario 2: Preparing to Teach (Facilitator)

**Goal:** Be ready to deliver workshop confidently

**Path:**
1. Review [docs/FACILITATOR_GUIDE.md](docs/FACILITATOR_GUIDE.md) (30 min)
2. Practice with [questions/sample_questions.md](questions/sample_questions.md) (30 min)
3. Customize [config/system_prompt.txt](config/system_prompt.txt) if needed (15 min)
4. Prepare slides/materials (60 min)

**Total time:** ~2 hours

---

### Scenario 3: Participating in Workshop (Participant)

**Goal:** Learn and complete exercises

**Path:**
1. Skim [README.md](README.md) (5 min)
2. Access LibreChat URL (provided by facilitator)
3. Follow along with [questions/sample_questions.md](questions/sample_questions.md)
4. Experiment and ask custom questions

**Total time:** Workshop duration (75 min)

---

### Scenario 4: Troubleshooting Issues (Any Role)

**Goal:** Fix a problem quickly

**Path:**
1. Check [docs/SETUP_GUIDE.md#troubleshooting](docs/SETUP_GUIDE.md#troubleshooting)
2. Run `make status` and `make logs`
3. Consult [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for component details
4. Review relevant config file

---

### Scenario 5: Customizing for Your Organization (Developer)

**Goal:** Adapt workshop for internal data

**Path:**
1. Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (30 min)
2. Modify [sql/schema.sql](sql/schema.sql) for your tables (60 min)
3. Update [config/system_prompt.txt](config/system_prompt.txt) with your schema (45 min)
4. Adjust [questions/sample_questions.md](questions/sample_questions.md) for your domain (30 min)
5. Test and iterate

**Total time:** ~3 hours

---

## Key Customization Points

### 1. System Prompt (Most Important!)
**File:** [config/system_prompt.txt](config/system_prompt.txt)

**Why:** Directly impacts SQL quality and response format

**What to customize:**
- Schema documentation
- Query patterns
- Domain terminology
- Visualization preferences

---

### 2. Sample Questions
**File:** [questions/sample_questions.md](questions/sample_questions.md)

**Why:** Must match your data and use cases

**What to customize:**
- Service names
- Metric names
- Time ranges
- Business scenarios

---

### 3. Model Selection
**File:** [config/litellm_config.yaml](config/litellm_config.yaml)

**Why:** Balance cost vs. quality

**Options:**
- Claude 3.5 Sonnet (best quality, higher cost)
- GPT-4 Turbo (good quality, moderate cost)
- Claude Haiku / GPT-3.5 (lower quality, lowest cost)

---

### 4. Workshop Duration
**File:** [docs/FACILITATOR_GUIDE.md](docs/FACILITATOR_GUIDE.md)

**Current:** 75 minutes

**Adjustable:**
- 60 min (skip advanced topics)
- 90 min (add more exercises)
- 120 min (deep dives)

---

## Best Practices

### For Facilitators

1. **Test everything** 24 hours before workshop
2. **Have backup plans** for technical failures
3. **Start with easy wins** (simple queries first)
4. **Use Langfuse** to show the "magic" (LLM traces)
5. **Encourage experimentation** (it's okay to fail)

### For Developers

1. **Never commit `.env`** with real credentials
2. **Use materialized views** for production
3. **Set budget limits** on LLM APIs
4. **Monitor costs** via Langfuse
5. **Version control system prompt** changes

### For Participants

1. **Start with simple questions** from the list
2. **Iterate on queries** (refine if results aren't right)
3. **Read the SQL** to learn ClickHouse patterns
4. **Ask "why"** questions (understand the data)
5. **Experiment** with time ranges and filters

---

## Common Issues & Solutions

### Issue: "Container won't start"
**Check:** `make logs-[service]`
**Fix:** Usually port conflicts or missing env vars

### Issue: "No query results"
**Check:** ClickHouse connection (`make test-clickhouse`)
**Fix:** Verify credentials in `.env`

### Issue: "LLM errors"
**Check:** API key validity, rate limits
**Fix:** Switch to different model or wait

### Issue: "Slow queries"
**Check:** Query is using indexes and time filters
**Fix:** Use materialized views, add LIMIT

---

## Workshop Checklist

### 1 Week Before
- [ ] Complete setup following SETUP_GUIDE.md
- [ ] Test end-to-end queries
- [ ] Review FACILITATOR_GUIDE.md
- [ ] Prepare slides/materials

### 1 Day Before
- [ ] Verify all services running
- [ ] Test 5-10 sample questions
- [ ] Check Langfuse is receiving traces
- [ ] Confirm participant access info

### Day Of
- [ ] Start services 30 min early
- [ ] Test one query
- [ ] Prepare browser tabs (LibreChat, Langfuse)
- [ ] Have backup queries ready

### After Workshop
- [ ] Export important traces
- [ ] Gather feedback
- [ ] Stop services (or terminate EC2)
- [ ] Update materials based on learnings

---

## Support & Resources

### Documentation
- This workshop: Files in this repository
- ClickHouse: https://clickhouse.com/docs
- OpenTelemetry: https://opentelemetry.io/docs
- Langfuse: https://langfuse.com/docs

### Community
- ClickHouse Slack: https://clickhouse.com/slack
- OpenTelemetry Slack: https://cloud-native.slack.com

### Getting Help
- GitHub Issues: (your repository)
- Email: (your support email)

---

## License

MIT License - See LICENSE file for details

---

## Contributing

Contributions welcome! To improve this workshop:

1. Fork the repository
2. Make your changes
3. Test thoroughly
4. Submit pull request with description

Areas for contribution:
- Additional sample questions
- New use cases
- Performance improvements
- Bug fixes
- Documentation improvements

---

**Happy Workshop-ing!** 🚀

**Questions?** Open an issue or reach out to the workshop organizer.
