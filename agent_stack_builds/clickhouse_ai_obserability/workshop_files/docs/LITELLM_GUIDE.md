# LiteLLM Integration Guide

## Why LiteLLM is Central to This Workshop

**LiteLLM** is the core component that makes this workshop possible. It acts as an intelligent proxy between LibreChat and LLM providers (Claude, GPT-4), while automatically sending traces to your Langfuse project.

---

## What LiteLLM Does

### 1. **Unified LLM Interface**
LiteLLM provides a single OpenAI-compatible API that works with multiple LLM providers:
- Anthropic Claude
- OpenAI GPT-4/GPT-4o
- Azure OpenAI
- Google Gemini
- And 100+ more

### 2. **Automatic Langfuse Tracing** (KEY FEATURE!)
Every request that goes through LiteLLM is automatically traced to Langfuse:
- **No additional code required**
- **Captures everything:**
  - Full prompt (including system prompt)
  - LLM response
  - Token usage (input + output)
  - Cost calculation
  - Latency breakdown
  - Model used
  - User metadata

### 3. **System Prompt Injection**
LiteLLM automatically injects the observability schema documentation into every request, so the LLM knows:
- ClickHouse table structures
- Query patterns
- Best practices
- Response formatting

### 4. **Load Balancing & Fallbacks**
If one model fails or is rate-limited, LiteLLM automatically:
- Retries the request
- Falls back to alternative models
- Tracks failures in Langfuse

---

## How It Works in This Workshop

### Request Flow

```
1. User types in LibreChat: "Show me error rates"
   ↓
2. LibreChat sends to LiteLLM (thinks it's OpenAI API)
   POST http://litellm:4000/v1/chat/completions
   ↓
3. LiteLLM processes:
   a. Reads system prompt from config/system_prompt.txt
   b. Injects it into the conversation
   c. Adds metadata (user, tags, etc.)
   d. Routes to Claude/GPT-4
   e. SIMULTANEOUSLY sends trace start to Langfuse
   ↓
4. LLM generates SQL query
   ↓
5. LiteLLM receives response:
   a. Calculates tokens & cost
   b. Sends complete trace to Langfuse
   c. Streams response back to LibreChat
   ↓
6. User sees result in LibreChat
7. Facilitator sees full trace in Langfuse dashboard
```

---

## Configuration Files

### 1. `config/litellm_config.yaml`

This is the main LiteLLM configuration file:

```yaml
# Model definitions
model_list:
  - model_name: claude-3-5-sonnet
    litellm_params:
      model: anthropic/claude-3-5-sonnet-20241022
      api_key: ${ANTHROPIC_API_KEY}
      metadata:
        tags: ["workshop", "observability"]

# Langfuse integration (THIS IS THE MAGIC!)
litellm_settings:
  success_callback: ["langfuse"]  # Send successful requests
  failure_callback: ["langfuse"]  # Send failed requests too!

# Langfuse credentials
general_settings:
  langfuse_public_key: ${LANGFUSE_PUBLIC_KEY}
  langfuse_secret_key: ${LANGFUSE_SECRET_KEY}
  langfuse_host: ${LANGFUSE_HOST}
```

**Key Settings:**
- `success_callback: ["langfuse"]` - Automatically trace all successful LLM calls
- `failure_callback: ["langfuse"]` - Also trace failures for debugging
- Environment variables for secure credential management

### 2. `config/system_prompt.txt`

This 10KB file contains the entire ClickHouse schema documentation that LiteLLM injects into every request. It teaches the LLM:
- Table structures (otel_traces, otel_metrics, etc.)
- Common query patterns
- Best practices (e.g., Duration is in nanoseconds!)
- Response formatting guidelines

### 3. `.env` File

Participants configure their Langfuse credentials here:

```bash
# Langfuse Cloud Configuration
LANGFUSE_PUBLIC_KEY=pk-lf-your-public-key
LANGFUSE_SECRET_KEY=sk-lf-your-secret-key
LANGFUSE_HOST=https://cloud.langfuse.com
```

When LiteLLM starts, it reads these variables and automatically connects to the user's Langfuse project.

---

## Langfuse Dashboard - What Participants See

After running queries, participants can view in Langfuse:

### Traces View
- List of all LLM requests
- Timestamp, duration, tokens, cost
- Model used
- Status (success/failure)

### Individual Trace Details
Click on any trace to see:

1. **Metadata**
   - User: workshop@example.com
   - Model: claude-3-5-sonnet
   - Tags: ["workshop", "observability"]

2. **Input (Prompt)**
   ```
   System: [Full 10KB system prompt with schema]
   User: "Show me error rates by service"
   ```

3. **Output (Completion)**
   ```
   To find error rates, I'll query the otel_traces table...
   [Generated SQL query]
   [Explanation and insights]
   ```

4. **Usage**
   - Input tokens: 2,456
   - Output tokens: 387
   - Total cost: $0.03

5. **Latency**
   - Total: 2.3s
   - Time to first token: 0.8s
   - Tokens per second: 168

---

## Workshop Learning Objectives for LiteLLM

By the end of this workshop, participants will understand:

### 1. **Why Use LiteLLM?**
- ✅ Unified interface to multiple LLM providers
- ✅ Automatic observability with Langfuse
- ✅ Cost tracking and optimization
- ✅ Fallback and retry logic
- ✅ System prompt management

### 2. **How to Configure LiteLLM**
- ✅ Set up model routing
- ✅ Configure Langfuse tracing
- ✅ Inject system prompts
- ✅ Manage API keys securely

### 3. **How to Monitor LLM Usage**
- ✅ View traces in Langfuse
- ✅ Analyze token usage
- ✅ Track costs
- ✅ Debug failed requests
- ✅ Optimize prompts based on traces

---

## Common LiteLLM Patterns

### Pattern 1: Multi-Model Setup

```yaml
model_list:
  # Primary model
  - model_name: claude-3-5-sonnet
    litellm_params:
      model: anthropic/claude-3-5-sonnet-20241022
      api_key: ${ANTHROPIC_API_KEY}

  # Fallback model
  - model_name: gpt-4-turbo
    litellm_params:
      model: gpt-4-turbo-preview
      api_key: ${OPENAI_API_KEY}

# Fallback configuration
router_settings:
  fallbacks:
    - claude-3-5-sonnet: ["gpt-4-turbo"]
```

### Pattern 2: Cost Tracking by User

```yaml
general_settings:
  # Track costs per user
  track_cost_per_user: true

  # Set budgets
  max_budget: 10.0  # $10 per user
  budget_duration: "24h"
```

### Pattern 3: Custom Metadata

```python
# In the request (LibreChat sends this)
{
  "model": "claude-3-5-sonnet",
  "messages": [...],
  "metadata": {
    "user": "workshop-participant-1",
    "session": "2026-03-30-afternoon",
    "query_type": "error_analysis"
  }
}
```

This metadata appears in Langfuse for filtering and analysis.

---

## Troubleshooting LiteLLM

### Issue: Langfuse not receiving traces

**Check:**
```bash
# View LiteLLM logs
make logs-litellm

# Look for Langfuse connection messages
docker logs litellm | grep -i langfuse
```

**Common causes:**
- Incorrect Langfuse API keys
- Wrong Langfuse host URL (use https://cloud.langfuse.com, no trailing slash)
- Network connectivity issues

**Solution:**
```bash
# Test Langfuse connection
make test-langfuse

# Verify credentials in .env
cat .env | grep LANGFUSE
```

### Issue: LiteLLM can't reach LLM provider

**Check:**
```bash
# Test LiteLLM health
curl http://localhost:4000/health

# Check API key validity
curl -H "Authorization: Bearer $ANTHROPIC_API_KEY" \
  https://api.anthropic.com/v1/messages
```

### Issue: System prompt not being injected

**Check:**
```bash
# Verify system prompt file exists
cat workshop_files/config/system_prompt.txt | wc -l
# Should show ~300 lines

# Check LiteLLM config
grep -A5 "default_prompts" config/litellm_config.yaml
```

---

## Advanced: Extending LiteLLM

### Add Custom Functions

LiteLLM can call functions (like executing SQL):

```yaml
function_calling:
  enabled: true
  functions:
    - name: execute_clickhouse_query
      description: Execute SQL against ClickHouse
      parameters:
        type: object
        properties:
          query:
            type: string
            description: The SQL query to run
```

### Caching for Cost Savings

```yaml
cache_settings:
  type: redis
  redis_host: redis
  redis_port: 6379
  ttl: 3600  # Cache for 1 hour
```

Identical queries return cached results, saving API costs.

### Rate Limiting

```yaml
router_settings:
  rate_limit:
    requests_per_minute: 60
    tokens_per_minute: 100000
```

---

## Why This Matters for Production

LiteLLM provides the foundation for production LLM applications:

1. **Observability** - Langfuse traces show exactly what your LLM is doing
2. **Cost Control** - Track and limit spending per user/model
3. **Reliability** - Automatic fallbacks if primary model fails
4. **Flexibility** - Switch models without changing application code
5. **Security** - Centralized API key management
6. **Scalability** - Load balance across multiple API keys/models

---

## Workshop Demonstration Points

### For Facilitators

1. **Start of Workshop (10 min)**
   - Show LiteLLM config file
   - Explain Langfuse integration
   - Set expectations: "Every query you run will be traced"

2. **After First Query (15 min)**
   - Switch to Langfuse dashboard
   - Find the trace for the query just run
   - Show:
     - System prompt that was injected
     - User question
     - Generated SQL
     - Token usage
     - Cost
     - Latency breakdown

3. **Mid-Workshop (45 min)**
   - Compare traces for different models (Claude vs GPT-4)
   - Show failed query trace (for debugging)
   - Demonstrate filtering traces by metadata

4. **End of Workshop (70 min)**
   - Show aggregate analytics in Langfuse
   - Total tokens used
   - Total cost
   - Average latency
   - Most expensive queries

---

## Key Takeaways

1. **LiteLLM is not optional** - It's the core component enabling observability
2. **Langfuse integration is automatic** - No code changes needed
3. **Participants see their own data** - Using their Langfuse project
4. **Production-ready pattern** - This is how real systems should be built

---

## Resources

- [LiteLLM Documentation](https://docs.litellm.ai/)
- [Langfuse Integration Guide](https://langfuse.com/docs/integrations/litellm)
- [LiteLLM GitHub](https://github.com/BerriAI/litellm)
- [Langfuse GitHub](https://github.com/langfuse/langfuse)

---

**Remember:** The workshop's "aha moment" is when participants see their query traced in Langfuse in real-time. This demonstrates the power of LLMOps observability.
