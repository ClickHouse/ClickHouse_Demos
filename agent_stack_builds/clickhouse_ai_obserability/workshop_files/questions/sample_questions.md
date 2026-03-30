# Sample Questions for Observability Workshop

This document contains 60+ natural language questions organized by difficulty and topic. Use these during the workshop to demonstrate the AI agent's capabilities.

---

## PART 1: Basic Service Discovery & Health (5-10 minutes)

### Beginner Questions

1. "What services are currently running?"
2. "Show me all active services in the last hour"
3. "How many requests has each service handled today?"
4. "Which service is receiving the most traffic?"
5. "List all services and their last seen timestamp"

### Expected Outcomes
- Understand service catalog
- See request volumes
- Identify active vs inactive services

---

## PART 2: Error Analysis (10-15 minutes)

### Intermediate Questions

6. "Show me error rates by service in the last hour"
7. "Which service has the highest error rate?"
8. "Find all failed requests in the last 30 minutes"
9. "Show me the most common errors across all services"
10. "What percentage of requests are failing for the payment service?"
11. "Compare error rates today vs yesterday for all services"
12. "Find traces with ERROR status and show their error messages"

### Advanced Questions

13. "Why is the authentication service failing?"
14. "Show me all error logs for traces that failed"
15. "Find the root cause of errors in the checkout service"
16. "What external dependencies are causing errors?"
17. "Are errors correlated with high latency?"

### Expected Outcomes
- Calculate error rates
- Identify problematic services
- Correlate errors with logs
- Root cause analysis

---

## PART 3: Latency & Performance (15-20 minutes)

### Basic Latency Questions

18. "What's the average latency by service?"
19. "Show me p95 and p99 latency for all services"
20. "Which service has the highest p99 latency?"
21. "Find the slowest requests in the last hour"
22. "Show me latency percentiles for the API gateway"

### Endpoint-Specific Questions

23. "What's the latency for the GET /api/users endpoint?"
24. "Show me the slowest HTTP endpoints across all services"
25. "Compare latency for POST /api/orders today vs last week"
26. "Which API endpoints are slower than 1 second?"
27. "Show me p95 latency breakdown by HTTP method (GET, POST, etc.)"

### Database Performance

28. "How long are database queries taking?"
29. "Find database queries slower than 5 seconds"
30. "Which service is making the slowest database calls?"
31. "Show me query performance by database operation (SELECT, INSERT, UPDATE)"
32. "What's the p99 latency for Redis cache operations?"

### Expected Outcomes
- Understand latency distributions
- Identify performance bottlenecks
- Analyze database performance
- Compare endpoints

---

## PART 4: Distributed Tracing (20-25 minutes)

### Trace Analysis

33. "Show me the 10 slowest traces in the last hour"
34. "Find traces that took longer than 10 seconds"
35. "Show me all spans for trace ID abc123..."
36. "How many services does a typical checkout trace touch?"
37. "Find traces with more than 50 spans"

### Service Dependencies

38. "Show me which services call the payment service"
39. "Build a service dependency map for the last hour"
40. "What's the call volume between api-gateway and user-service?"
41. "Find all downstream dependencies of the authentication service"
42. "Show me the critical path services (most dependencies)"

### Cross-Service Analysis

43. "Where is most time spent in a typical API request?"
44. "Show me the span breakdown for GET /api/checkout"
45. "Find bottlenecks in the checkout flow"
46. "Which downstream service is causing the most latency?"
47. "Trace a failed payment request end-to-end"

### Expected Outcomes
- Understand distributed traces
- Build dependency graphs
- Identify cross-service bottlenecks
- Perform end-to-end trace analysis

---

## PART 5: SLA & Compliance (10-15 minutes)

### SLA Questions

48. "Are any services violating their SLA?"
49. "Show me SLA compliance for all services"
50. "Which services have p95 latency above their SLA target?"
51. "Is the checkout service meeting its 99.9% availability target?"
52. "Compare actual vs target error rates for all services"

### Availability

53. "What's the availability of each service in the last 24 hours?"
54. "Calculate uptime percentage for the payment service this week"
55. "Show me services with availability below 99%"

### Expected Outcomes
- Check SLA compliance
- Calculate availability metrics
- Identify SLA violations
- Compare targets vs actuals

---

## PART 6: Time-Series & Trends (10-15 minutes)

### Trend Analysis

56. "Show me request volume trend over the last 24 hours"
57. "How has error rate changed over the past week?"
58. "Compare latency today vs last Monday at the same time"
59. "Show me throughput by service for each hour today"
60. "Is traffic increasing or decreasing this week?"

### Anomaly Detection

61. "Find unusual spikes in error rates today"
62. "Show me when latency was significantly higher than normal"
63. "Identify services with abnormal traffic patterns"
64. "Did any service have a traffic surge in the last hour?"

### Expected Outcomes
- Visualize time-series data
- Compare time periods
- Detect anomalies
- Understand traffic patterns

---

## PART 7: Advanced Analytics (Optional - 10 minutes)

### Complex Queries

65. "What's the correlation between request volume and latency?"
66. "Show me services where errors spike during high load"
67. "Find traces where database calls took >80% of total duration"
68. "Which services have the most variance in latency?"
69. "Calculate the blast radius of the payment service (how many services depend on it)"

### Business Impact

70. "How many failed checkout attempts were there today?"
71. "What's the revenue impact of payment service errors?" (requires revenue data in attributes)
72. "Show me user-facing errors only (exclude internal retries)"
73. "Calculate the customer error rate vs internal error rate"

### Cost Analysis

74. "Which services generate the most spans (highest cardinality)?"
75. "Show me the most verbose services (spans per trace)"
76. "Estimate query cost by looking at data scanned"

---

## Workshop Exercise Path (Facilitator Guide)

### Suggested 75-Minute Flow:

**Minutes 0-10: Setup & Basic Discovery**
- Questions 1-5 (Service discovery)
- Verify everyone can query and see results

**Minutes 10-25: Error Analysis**
- Questions 6-12 (Basic errors)
- Questions 13-17 (Root cause)
- Hands-on: Each participant picks a service to debug

**Minutes 25-45: Latency & Performance**
- Questions 18-22 (Basic latency)
- Questions 23-27 (Endpoints)
- Questions 28-32 (Database)
- Hands-on: Find the slowest endpoint and explain why

**Minutes 45-65: Distributed Tracing**
- Questions 33-37 (Trace analysis)
- Questions 38-42 (Dependencies)
- Questions 43-47 (Cross-service)
- Hands-on: Build a service dependency graph

**Minutes 65-70: SLA & Advanced**
- Questions 48-52 (SLA compliance)
- Questions 65-69 (Advanced analytics)
- Demonstration of complex queries

**Minutes 70-75: Q&A**
- Open discussion
- Custom questions from participants
- Next steps and resources

---

## Tips for Facilitators

### Progressive Complexity
Start simple and gradually increase difficulty. Ensure participants succeed early to build confidence.

### Encourage Exploration
After each section, ask participants to modify questions:
- "Now try this for a different time window"
- "What if you filter by a specific service?"
- "Can you add another metric to this query?"

### Common Pitfalls
1. **Forgetting time windows**: Always specify "in the last hour/day"
2. **Not converting nanoseconds**: Duration must be divided by 1,000,000
3. **Querying without filtering**: Always filter by Timestamp for performance

### Engagement Tactics
- **Competition**: "Who can find the slowest endpoint first?"
- **Real-world scenarios**: "You just got paged for high errors - what's your first query?"
- **Pair programming**: Have participants work in pairs
- **Live debugging**: Simulate a production incident

### Langfuse Integration
After each query, show participants:
1. Open Langfuse dashboard
2. Find the trace for the query
3. See how the LLM converted natural language → SQL
4. Review token usage and latency
5. Discuss prompt engineering improvements

---

## Custom Questions Template

Encourage participants to write their own questions:

**Format:**
- **Intent**: What do you want to know?
- **Entities**: Which services/endpoints/time periods?
- **Metrics**: What are you measuring? (latency, errors, volume)
- **Filters**: Any specific conditions?

**Example:**
"Show me [metric] for [entity] in the [time window] where [condition]"

→ "Show me p95 latency for the payment service in the last 2 hours where error rate > 5%"

---

## Answer Key (For Facilitators)

Sample SQL queries for select questions:

### Question 6: "Show me error rates by service in the last hour"

```sql
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

### Question 19: "Show me p95 and p99 latency for all services"

```sql
SELECT
    ServiceName,
    count() as request_count,
    round(quantile(0.95)(Duration) / 1000000, 2) as p95_latency_ms,
    round(quantile(0.99)(Duration) / 1000000, 2) as p99_latency_ms
FROM otel_traces
WHERE Timestamp >= now() - INTERVAL 1 HOUR
  AND SpanKind = 'SERVER'
GROUP BY ServiceName
ORDER BY p99_latency_ms DESC
```

### Question 39: "Build a service dependency map for the last hour"

```sql
SELECT
    parent_service,
    child_service,
    sum(call_count) as total_calls,
    round(sum(error_count) / sum(call_count) * 100, 2) as error_rate_pct,
    round(avg(p95_duration_ms), 2) as avg_p95_latency_ms
FROM otel_service_dependencies
WHERE timestamp_hour >= now() - INTERVAL 1 HOUR
GROUP BY parent_service, child_service
ORDER BY total_calls DESC
LIMIT 20
```

---

## Resources

- [ClickHouse Query Optimization](https://clickhouse.com/docs/en/guides/improving-query-performance)
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [Langfuse Tracing Guide](https://langfuse.com/docs/tracing)

---

**Remember:** The goal is not just to answer questions, but to teach participants how to think about observability data and formulate their own queries using natural language.
