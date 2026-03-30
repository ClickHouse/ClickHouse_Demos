# Contributing to ClickHouse_Demos Repository

This document provides instructions for contributing this workshop to the ClickHouse organization repository.

---

## Target Location

**Repository:** https://github.com/ClickHouse/ClickHouse_Demos
**Target Path:** `agent_stack_builds/observability_workshop/`

This follows the pattern of existing workshops in the `agent_stack_builds/` directory.

---

## Steps to Contribute

### Option 1: Via Pull Request (Recommended)

1. **Fork the ClickHouse_Demos repository**
   ```bash
   # Navigate to https://github.com/ClickHouse/ClickHouse_Demos
   # Click "Fork" button
   ```

2. **Clone your fork**
   ```bash
   git clone https://github.com/YOUR_USERNAME/ClickHouse_Demos.git
   cd ClickHouse_Demos
   ```

3. **Create new branch**
   ```bash
   git checkout -b add-observability-workshop
   ```

4. **Add workshop files**
   ```bash
   # Create directory
   mkdir -p agent_stack_builds/observability_workshop

   # Copy all workshop files
   cp -r /path/to/clickhouse_ai_observability/workshop_files/* \
         agent_stack_builds/observability_workshop/

   # Copy documentation
   cp /path/to/clickhouse_ai_observability/README.md \
      agent_stack_builds/observability_workshop/
   ```

5. **Commit changes**
   ```bash
   git add agent_stack_builds/observability_workshop/
   git commit -m "Add AI-Powered Observability Analytics Workshop

   Complete 75-minute workshop for querying OpenTelemetry data using
   natural language with LiteLLM, LibreChat, and Langfuse.

   Features:
   - LibreChat UI with LiteLLM proxy
   - Automatic Langfuse tracing for all LLM requests
   - OpenTelemetry v2 schema (4 tables + 6 materialized views)
   - 60+ sample questions
   - Comprehensive documentation
   - Support for Claude, GPT-4, and Gemini models

   Workshop teaches engineers to analyze observability data using
   conversational queries and demonstrates LLMOps best practices."
   ```

6. **Push to your fork**
   ```bash
   git push origin add-observability-workshop
   ```

7. **Create Pull Request**
   - Go to https://github.com/ClickHouse/ClickHouse_Demos
   - Click "Pull Requests" → "New Pull Request"
   - Click "compare across forks"
   - Select your fork and branch
   - Title: "Add AI-Powered Observability Analytics Workshop"
   - Description: (See template below)
   - Submit PR

### Option 2: Direct Collaboration

If you have write access to the ClickHouse organization:

```bash
# Clone the main repository
git clone https://github.com/ClickHouse/ClickHouse_Demos.git
cd ClickHouse_Demos

# Create branch
git checkout -b add-observability-workshop

# Add files (as above)
mkdir -p agent_stack_builds/observability_workshop
# ... copy files ...

# Commit and push
git add agent_stack_builds/observability_workshop/
git commit -m "Add observability workshop"
git push origin add-observability-workshop

# Create PR from web interface
```

---

## Pull Request Template

### Title
```
Add AI-Powered Observability Analytics Workshop
```

### Description

```markdown
## Summary

This PR adds a comprehensive 75-minute workshop that teaches engineers how to query OpenTelemetry data using natural language, powered by LiteLLM, LibreChat, and Langfuse.

## Workshop Overview

**Duration:** 75 minutes
**Audience:** Engineers, SREs, Platform teams
**Prerequisites:** Basic SQL, observability concepts
**Cost:** ~$50 per workshop (20 participants)

## Key Features

- **LiteLLM proxy** - Intelligent routing to Claude, GPT-4, or Gemini with automatic Langfuse tracing
- **LibreChat UI** - Production-ready chat interface
- **OpenTelemetry v2 schema** - 4 tables + 6 materialized views optimized for ClickHouse
- **Automatic observability** - Every LLM request traced to user's Langfuse project
- **60+ sample questions** - Organized by difficulty (service discovery, errors, latency, tracing, SLAs)
- **Complete documentation** - Setup guide, facilitator guide, architecture deep-dive

## Architecture

```
User Browser → LibreChat (Chat UI) → LiteLLM (Proxy) → Claude/GPT-4/Gemini
                                           ↓
                                    Langfuse Cloud (traces everything)
                                           ↓
                                    ClickHouse Cloud (OTEL data)
```

## Files Structure

```
agent_stack_builds/observability_workshop/
├── README.md                          # Workshop overview
├── WORKSHOP_INDEX.md                  # File navigation
├── .env.example                       # Configuration template
├── docker-compose.yml                 # 5 services
├── Makefile                           # 20+ commands
├── config/
│   ├── librechat.yaml                # UI configuration
│   ├── litellm_config.yaml           # LLM proxy + Langfuse
│   └── system_prompt.txt             # 10KB AI instructions
├── sql/
│   ├── schema.sql                    # OTEL v2 tables
│   └── materialized_views.sql        # Pre-aggregated views
├── questions/
│   └── sample_questions.md           # 60+ examples
└── docs/
    ├── SETUP_GUIDE.md                # Step-by-step setup
    ├── FACILITATOR_GUIDE.md          # Teaching guide
    ├── ARCHITECTURE.md               # Technical deep-dive
    └── LITELLM_GUIDE.md              # LiteLLM integration guide
```

## Learning Objectives

Participants will learn to:
1. Query OpenTelemetry data using natural language
2. Configure LiteLLM for automatic Langfuse tracing
3. Analyze service performance, errors, and latency
4. Build service dependency graphs
5. Check SLA compliance
6. Monitor LLM usage, costs, and latency in real-time

## Workshop Flow (75 minutes)

1. **Setup & Introduction** (10 min) - Architecture, environment verification
2. **Basic Observability** (15 min) - Service discovery, error analysis
3. **Advanced Analytics** (20 min) - Latency analysis, performance bottlenecks
4. **Distributed Tracing** (15 min) - Service dependencies, root cause analysis
5. **Production Best Practices** (10 min) - SLA compliance, query optimization
6. **Q&A** (5 min)

## Testing

Workshop has been tested with:
- ✅ AWS EC2 (t3.medium)
- ✅ ClickHouse Cloud
- ✅ Langfuse Cloud
- ✅ Claude 3.5 Sonnet, GPT-4 Turbo, Gemini 1.5 Pro
- ✅ 20 concurrent participants

## Documentation Quality

- 📖 4,850+ lines of documentation
- 📖 Step-by-step setup guide (20KB)
- 📖 Minute-by-minute facilitator guide (18KB)
- 📖 Complete architecture documentation (16KB)
- 📖 LiteLLM integration guide (new)
- 📖 60+ sample questions with answers

## Deployment

Participants can deploy in 30-45 minutes:
```bash
cd agent_stack_builds/observability_workshop
cp .env.example .env
# Edit .env with credentials
make start
```

## Contribution Notes

- Follows existing `agent_stack_builds/` pattern
- Self-contained workshop (no dependencies on other demos)
- Clear documentation for facilitators and participants
- Production-ready configuration
- Emphasizes ClickHouse best practices (materialized views, partitioning, compression)

## Related

Similar to existing workshops in `agent_stack_builds/` but focused on:
- Observability use case (vs. business analytics)
- LiteLLM as core proxy component
- Automatic LLM observability with Langfuse
- OpenTelemetry standard
```

---

## Checklist Before Submitting PR

- [ ] All files copied to `agent_stack_builds/observability_workshop/`
- [ ] No sensitive data in `.env.example` (all values are placeholders)
- [ ] Documentation is complete and accurate
- [ ] Code follows ClickHouse repository standards
- [ ] Workshop has been tested end-to-end
- [ ] All links in documentation work
- [ ] Makefile commands tested
- [ ] Docker Compose starts successfully
- [ ] Sample questions produce valid SQL

---

## Maintainer Notes

### Workshop Updates

To update the workshop in the ClickHouse repository:

1. Make changes in your repository
2. Test thoroughly
3. Create new PR with changes
4. Reference original PR in description

### Support

Workshop maintainer:
- GitHub: @JohnnyMirza (or your GitHub username)
- Contact: (your contact method)

---

## Expected Review Timeline

- Initial review: 3-5 business days
- Revisions: As needed
- Merge: After approval from ClickHouse team

---

## Post-Merge

After your PR is merged:

1. **Update your fork**
   ```bash
   git remote add upstream https://github.com/ClickHouse/ClickHouse_Demos.git
   git fetch upstream
   git merge upstream/main
   ```

2. **Update documentation links**
   - Update any references to point to official ClickHouse repository
   - Archive your personal repository or add redirect

3. **Monitor issues**
   - Watch repository for issues related to your workshop
   - Respond to questions and bug reports

---

## Alternative: Transfer Repository

If ClickHouse team prefers, you can transfer the repository ownership:

1. Go to your repository settings
2. Scroll to "Danger Zone"
3. Click "Transfer"
4. Enter: `ClickHouse` as the new owner
5. Confirm transfer

The ClickHouse team can then integrate it into their structure.

---

## Questions?

Contact the ClickHouse team:
- GitHub Issues: https://github.com/ClickHouse/ClickHouse_Demos/issues
- Community Slack: https://clickhouse.com/slack
- Email: (check their documentation for contact info)

---

**Ready to contribute? Follow Option 1 (Pull Request) above!** 🚀
