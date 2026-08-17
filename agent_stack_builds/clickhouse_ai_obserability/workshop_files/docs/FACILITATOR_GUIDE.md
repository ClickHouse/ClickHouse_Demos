# Workshop Facilitator Guide

## Overview

This guide helps facilitators deliver an engaging 75-minute AI-powered observability analytics workshop.

---

## Workshop Goals

By the end of this workshop, participants will:

1. Understand how AI can simplify observability data analysis
2. Query OpenTelemetry data using natural language
3. Analyze service performance, errors, and latency
4. Build service dependency maps
5. Check SLA compliance
6. Understand the full AI agent stack (LibreChat → LiteLLM → ClickHouse → Langfuse)

---

## Pre-Workshop Preparation (1 week before)

### Technical Setup

- [ ] Provision AWS EC2 instance
- [ ] Set up ClickHouse Cloud (or get sql.clickhouse.com access)
- [ ] Create Langfuse Cloud account and project
- [ ] Obtain LLM API keys (OpenAI or Anthropic)
- [ ] Deploy and test workshop stack
- [ ] Run end-to-end verification
- [ ] Prepare backup instance (optional)

### Materials Preparation

- [ ] Slides or presentation deck
- [ ] Sample questions handout
- [ ] Participant access instructions
- [ ] Troubleshooting quick reference
- [ ] Post-workshop survey
- [ ] Certificate of completion (optional)

### Communication

Send participants:
- Workshop agenda
- Prerequisites (basic SQL, observability concepts)
- Pre-reading materials (optional)
- Connection instructions (will be sent day-of)

---

## Workshop Agenda (75 minutes)

### Part 1: Setup & Introduction (10 minutes)

**Objectives:**
- Set context and expectations
- Verify everyone can access LibreChat
- Explain architecture

**Facilitation:**

1. **Welcome (2 min)**
   ```
   "Welcome to AI-Powered Observability Analytics!

   Today you'll learn how to query production telemetry data
   using natural language instead of writing complex SQL.

   This is hands-on - you'll be writing questions and seeing
   real results from OpenTelemetry data."
   ```

2. **Architecture Overview (3 min)**
   - Show architecture diagram
   - Explain each component:
     - LibreChat: Chat UI
     - LiteLLM: LLM proxy
     - ClickHouse: OLAP database
     - Langfuse: Observability for AI
   - Emphasize the irony: "We're using observability to observe observability!"

3. **Access Verification (5 min)**
   - Share LibreChat URL: `http://your-ec2-ip:3000`
   - Have everyone create an account
   - Select "Claude 3.5 Sonnet" (or GPT-4 Turbo)
   - First collective query: **"What services are currently running?"**
   - Verify everyone sees results

**Troubleshooting:**
- Can't access URL → Check AWS security group
- Can't create account → Check LibreChat logs
- No results → Check ClickHouse connection

---

### Part 2: Basic Observability Queries (15 minutes)

**Objectives:**
- Learn service discovery
- Calculate error rates
- Understand request volumes

**Facilitation:**

1. **Service Discovery (5 min)**

   Guide participants through these questions:

   ```
   1. "What services are currently running?"
   2. "How many requests has each service handled in the last hour?"
   3. "Which service is receiving the most traffic?"
   ```

   **Teaching Points:**
   - Point out the SQL query in the response
   - Explain `SpanKind = 'SERVER'` filters for entry-point requests
   - Note the timestamp filter for performance

2. **Error Analysis (10 min)**

   ```
   4. "Show me error rates by service in the last hour"
   5. "Which service has the highest error rate?"
   6. "Find all failed requests in the last 30 minutes"
   ```

   **Teaching Points:**
   - Error rate = errors / total requests
   - `StatusCode = 'ERROR'` identifies failures
   - Discuss what "acceptable" error rates look like (< 1% typically)

   **Interactive Exercise:**

   "Pick any service and investigate its errors. What do you notice?"

   Give 3 minutes for exploration, then share findings.

**Key Concepts:**
- Natural language → SQL translation
- Aggregations (count, countIf)
- Time windows

---

### Part 3: Advanced Analytics (20 minutes)

**Objectives:**
- Analyze latency distributions
- Identify performance bottlenecks
- Understand percentiles (p50, p95, p99)

**Facilitation:**

1. **Latency Fundamentals (5 min)**

   ```
   7. "What's the p95 and p99 latency for all services?"
   8. "Which service has the highest p99 latency?"
   ```

   **Teaching Points:**
   - Why percentiles > averages (outliers matter!)
   - p95 = 95% of requests faster than this
   - p99 = 99% of requests faster than this
   - Duration is in nanoseconds (divide by 1,000,000 for ms)

   **Whiteboard Moment:**
   Draw latency distribution curve, show where p50/p95/p99 fall.

2. **Slow Request Investigation (10 min)**

   ```
   9. "Show me the 10 slowest requests in the last hour"
   10. "Find requests that took longer than 5 seconds"
   11. "What's the latency for the GET /api/users endpoint?"
   ```

   **Teaching Points:**
   - `ORDER BY Duration DESC` finds slowest
   - `SpanAttributes['http.route']` extracts endpoint
   - Individual slow requests vs consistent slowness

3. **Database Performance (5 min)**

   ```
   12. "How long are database queries taking?"
   13. "Find database queries slower than 1 second"
   ```

   **Teaching Points:**
   - `SpanKind = 'CLIENT'` for outbound calls
   - `SpanAttributes['db.system']` identifies database type
   - Database calls often the bottleneck

**Interactive Exercise:**

"Find the slowest endpoint in the system and hypothesize why it's slow."

Share as a group.

---

### Part 4: Distributed Tracing (15 minutes)

**Objectives:**
- Understand distributed traces
- Build service dependency maps
- Perform root cause analysis

**Facilitation:**

1. **Trace Anatomy (5 min)**

   ```
   14. "Show me all spans for the slowest trace"
   15. "How many services does a typical checkout trace touch?"
   ```

   **Teaching Points:**
   - Trace = collection of spans
   - Span = single operation (API call, DB query, etc.)
   - Parent-child relationships via `ParentSpanId`

2. **Service Dependencies (10 min)**

   ```
   16. "Build a service dependency map for the last hour"
   17. "Which services call the payment service?"
   18. "Show me the critical path for checkout requests"
   ```

   **Teaching Points:**
   - Dependencies reveal architecture
   - JOIN on `TraceId` and `ParentSpanId`
   - Materialized view `otel_service_dependencies` pre-computes this

   **Visualization Exercise:**

   "On paper, draw the dependency graph based on the results."

   Show how this maps to microservices architecture.

3. **Root Cause Analysis (5 min)**

   ```
   19. "Why is the payment service slow today?"
   20. "Find traces with database duration > 80% of total time"
   ```

   **Teaching Points:**
   - Correlate traces with logs
   - Look for common patterns in slow traces
   - Check downstream dependencies

**Key Concepts:**
- Distributed tracing fundamentals
- Service dependency graphs
- Critical path analysis

---

### Part 5: Production Best Practices (10 minutes)

**Objectives:**
- Check SLA compliance
- Optimize queries
- Understand Langfuse traces
- Learn cost considerations

**Facilitation:**

1. **SLA Compliance (5 min)**

   ```
   21. "Are any services violating their SLA?"
   22. "What's the availability of each service?"
   ```

   **Teaching Points:**
   - SLAs defined in `otel_services` table
   - Availability = 1 - error_rate
   - Compare actual vs target latency

2. **Query Optimization (3 min)**

   Open Langfuse dashboard:
   - Show trace for a recent query
   - Point out:
     - Token usage
     - Latency breakdown
     - SQL generation step
     - Query execution time

   **Teaching Points:**
   - Use materialized views when possible
   - Always filter by Timestamp (partition pruning)
   - Limit results to avoid overwhelming output
   - LowCardinality fields for GROUP BY

3. **Cost & Performance (2 min)**

   Discuss:
   - LLM API costs (tokens per query)
   - ClickHouse query costs (data scanned)
   - Caching strategies
   - When to use simpler models (Haiku/GPT-3.5)

**Key Takeaways:**
- Production requires monitoring the monitors
- Optimization matters at scale
- Balance cost vs. accuracy

---

### Part 6: Q&A and Next Steps (5 minutes)

**Facilitation:**

1. **Open Q&A (3 min)**

   Common questions:
   - Can this work with our internal data?
   - How do we customize the system prompt?
   - Can we add custom functions?
   - What about data privacy/security?

2. **Next Steps (2 min)**

   Resources to share:
   - GitHub repository with all code
   - Documentation links (ClickHouse, OpenTelemetry, Langfuse)
   - Community Slack channels
   - Follow-up materials

3. **Survey**

   Quick survey (optional):
   - What was most valuable?
   - What could be improved?
   - Would you implement this?

---

## Facilitation Tips

### Engagement Strategies

1. **Ask Questions Constantly**
   - "What do you think this query will return?"
   - "Why might this service have high latency?"
   - "Who has used percentiles in production?"

2. **Encourage Experimentation**
   - "Try modifying the time window"
   - "Ask about a different service"
   - "Combine multiple conditions"

3. **Use Real-World Scenarios**
   - "You just got paged at 2am for high errors. What's your first query?"
   - "Your CEO asks why the app is slow. How do you investigate?"

4. **Celebrate Failures**
   - If a query fails, use it as a teaching moment
   - Show how to debug with Langfuse traces
   - Explain ClickHouse error messages

5. **Pair Programming**
   - Have participants work in pairs
   - Rotate pairs for different sections

### Time Management

- **Running Ahead?** Add advanced topics:
  - Anomaly detection
  - Time-series analysis
  - Custom aggregations

- **Running Behind?** Skip:
  - Database performance section
  - Some interactive exercises
  - Detailed Langfuse walkthrough

### Common Pitfalls

1. **Overwhelming with SQL**
   - Focus on natural language
   - SQL is secondary (it's auto-generated!)
   - Emphasize "you don't need to write this"

2. **Going Too Fast**
   - Pause after each section
   - "Any questions before we continue?"
   - Check in with participants visually

3. **Ignoring Errors**
   - If something breaks, fix it on the spot
   - Use it as a learning opportunity
   - Have backup queries ready

### Energy Management

- **Start high energy** (enthusiasm is contagious!)
- **Use interactive exercises** (avoid lecture mode)
- **Take micro-breaks** (stretch, questions, discussions)
- **End on a high note** (show impressive complex query)

---

## Handling Technical Issues

### LibreChat Down

**Symptom:** UI not loading

**Quick Fix:**
```bash
make restart
make logs-librechat
```

**Workaround:** Use LiteLLM API directly via curl (have examples ready)

### ClickHouse Connection Failed

**Symptom:** "Connection refused" errors

**Quick Fix:**
```bash
make test-clickhouse
# Check credentials in .env
```

**Workaround:** Use pre-computed results (have screenshots ready)

### LLM API Rate Limit

**Symptom:** "Rate limit exceeded" errors

**Quick Fix:**
- Switch to different model
- Wait 60 seconds
- Use backup API key

**Workaround:** Show pre-recorded examples while waiting

### Slow Queries

**Symptom:** Queries taking >30 seconds

**Quick Fix:**
- Reduce time window
- Use materialized views
- Add more specific filters

**Workaround:** Continue with other examples while waiting

---

## Measuring Success

### During Workshop

- [ ] 90%+ participants successfully run first query
- [ ] At least 5 questions asked during Q&A
- [ ] No extended technical downtime (>5 min)
- [ ] Positive body language and engagement

### Post-Workshop

- [ ] Survey response rate >50%
- [ ] Average satisfaction score >4/5
- [ ] At least 30% express interest in implementation
- [ ] Follow-up questions or requests

### Long-Term

- [ ] Participants implement similar systems
- [ ] Workshop materials shared internally
- [ ] Requests for advanced workshops

---

## Advanced Topics (If Time Permits)

### Custom System Prompts

Show how to modify `config/system_prompt.txt` for:
- Domain-specific terminology
- Company-specific metrics
- Custom visualization formats

### Adding Custom Functions

Demonstrate extending LiteLLM with:
- Alert creation
- Report generation
- Integration with ticketing systems

### Multi-Model Comparison

Run same query with different models:
- Claude vs. GPT-4
- Compare quality, cost, speed
- When to use each

---

## Post-Workshop Checklist

- [ ] Export Langfuse traces (for analysis)
- [ ] Save interesting chat transcripts
- [ ] Document any issues encountered
- [ ] Send thank you email with resources
- [ ] Share survey results with participants
- [ ] Archive instance or tear down
- [ ] Update workshop materials based on feedback

---

## Resources for Facilitators

- [ClickHouse Best Practices](https://clickhouse.com/docs/en/guides/best-practices)
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [Prompt Engineering Guide](https://www.promptingguide.ai/)
- [Adult Learning Principles](https://en.wikipedia.org/wiki/Andragogy)

---

## Contact & Support

For facilitator support:
- **Email**: workshop-support@example.com
- **Slack**: #observability-workshop
- **Office Hours**: Tuesdays 2-3pm ET

---

**Good luck with your workshop! You've got this.** 🎓
