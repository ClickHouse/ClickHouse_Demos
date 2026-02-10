.PHONY: help setup-local setup-hybrid start stop restart generate-data init-schema check-db explore-data query test test-all logs clean-data clean

# ---------------------------------------------------------------------------
# Mode detection: read DEPLOY_MODE from .env (defaults to local)
# ---------------------------------------------------------------------------
DEPLOY_MODE := $(or $(shell grep -s '^DEPLOY_MODE=' .env 2>/dev/null | cut -d= -f2),local)

# Include mode-specific variables and targets (COMPOSE_CMD, CH_CLIENT, init-schema, etc.)
include Makefile.$(DEPLOY_MODE).mk

help:
	@echo "Telco Workshop - Available Commands"
	@echo "===================================="
	@echo ""
	@echo "Setup:"
	@echo "  make setup-local    - Initialize .env for local deployment (all services in Docker)"
	@echo "  make setup-hybrid   - Initialize .env for hybrid deployment (ClickHouse Cloud remote MCP + Langfuse Cloud)"
	@echo ""
	@echo "Lifecycle:"
	@echo "  make start          - Start all services (mode-aware)"
	@echo "  make stop           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo "  make generate-data  - Generate telco data"
	@echo "  make init-schema    - Push clickhouse/init.sql to ClickHouse (needed for hybrid; automatic for local)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean-data     - Truncate all telco tables (for fresh data reload)"
	@echo "  make clean          - Stop services, remove volumes, and delete generated files"
	@echo ""
	@echo "Verification & Exploration:"
	@echo "  make check-db       - Verify database connectivity and table row counts"
	@echo "  make explore-data   - Run sample queries to explore the loaded data"
	@echo "  make query          - Open interactive ClickHouse SQL shell"
	@echo ""
	@echo "Diagnostics:"
	@echo "  make test           - Test the setup (ClickHouse connectivity, mode-aware)"
	@echo "  make test-all       - Run end-to-end test suite in Docker (no local deps)"
	@echo "  make logs           - View logs from all services"
	@echo ""
	@echo "Current mode: $(DEPLOY_MODE)"

# ---------------------------------------------------------------------------
# Setup targets
# ---------------------------------------------------------------------------
setup-local:
	@echo "Setting up LOCAL deployment environment..."
	@if [ -f .env ]; then \
		echo "[WARN] .env already exists. Back it up or remove it first."; \
		exit 1; \
	fi
	@cp .env.local.example .env
	@echo "[OK] Created .env from .env.local.example"
	@# Generate security keys
	@LANGFUSE_SECRET=$$(openssl rand -hex 32) && \
	 LANGFUSE_SALT=$$(openssl rand -hex 32) && \
	 CREDS_KEY=$$(openssl rand -hex 32) && \
	 CREDS_IV=$$(openssl rand -hex 16) && \
	 JWT_SECRET=$$(openssl rand -hex 32) && \
	 JWT_REFRESH_SECRET=$$(openssl rand -hex 32) && \
	 MEILI_MASTER_KEY=$$(openssl rand -hex 16) && \
	 LITELLM_MK=$$(openssl rand -hex 32) && \
	 sed -i.bak \
	   -e "s|<generate-with-openssl-rand-hex-32>|$$LANGFUSE_SECRET|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-32>/s||$$LANGFUSE_SALT|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-32>/s||$$CREDS_KEY|" \
	   .env && \
	 sed -i.bak \
	   -e "s|<generate-with-openssl-rand-hex-16>|$$CREDS_IV|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-32>/s||$$JWT_SECRET|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-32>/s||$$JWT_REFRESH_SECRET|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-16>/s||$$MEILI_MASTER_KEY|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-32>/s||$$LITELLM_MK|" \
	   .env && \
	 rm -f .env.bak
	@echo "[OK] Generated security keys"
	@# Generate Langfuse API keys (same value written to both LANGFUSE_INIT_PROJECT_* and LANGFUSE_*)
	@LANGFUSE_PK=$$(openssl rand -hex 16) && \
	 LANGFUSE_SK=$$(openssl rand -hex 32) && \
	 sed -i.bak \
	   -e "s|<generate-langfuse-pk>|$$LANGFUSE_PK|g" \
	   -e "s|<generate-langfuse-sk>|$$LANGFUSE_SK|g" \
	   .env && \
	 rm -f .env.bak
	@echo "[OK] Generated Langfuse API keys (tracing auto-configured)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Edit .env to set your GOOGLE_KEY (required for Gemini models)"
	@echo "  2. make start"
	@echo "  3. make generate-data"
	@echo "  4. Open http://localhost:3080 (LibreChat)"
	@echo ""
	@echo "Langfuse tracing is auto-configured. ALL LLM calls are traced via LiteLLM."
	@echo "Log in at http://localhost:3000 with admin@telco.local / admin123 after running 'make start'."

setup-hybrid:
	@echo "Setting up HYBRID deployment environment..."
	@if [ -f .env ]; then \
		echo "[WARN] .env already exists. Back it up or remove it first."; \
		exit 1; \
	fi
	@cp .env.hybrid.example .env
	@echo "[OK] Created .env from .env.hybrid.example"
	@# Generate LibreChat security keys
	@CREDS_KEY=$$(openssl rand -hex 32) && \
	 CREDS_IV=$$(openssl rand -hex 16) && \
	 JWT_SECRET=$$(openssl rand -hex 32) && \
	 JWT_REFRESH_SECRET=$$(openssl rand -hex 32) && \
	 MEILI_MASTER_KEY=$$(openssl rand -hex 16) && \
	 LITELLM_MK=$$(openssl rand -hex 32) && \
	 sed -i.bak \
	   -e "s|<generate-with-openssl-rand-hex-32>|$$CREDS_KEY|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-32>/s||$$JWT_SECRET|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-32>/s||$$JWT_REFRESH_SECRET|" \
	   .env && \
	 sed -i.bak \
	   -e "s|<generate-with-openssl-rand-hex-16>|$$CREDS_IV|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-16>/s||$$MEILI_MASTER_KEY|" \
	   .env && \
	 sed -i.bak \
	   -e "0,/<generate-with-openssl-rand-hex-32>/s||$$LITELLM_MK|" \
	   .env && \
	 rm -f .env.bak
	@echo "[OK] Generated LibreChat security keys"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Edit .env and fill in your ClickHouse Cloud credentials"
	@echo "  2. Enable the Remote MCP Server in your ClickHouse Cloud console (Connect > Remote MCP Server)"
	@echo "  3. Edit .env and fill in your Langfuse Cloud credentials (optional)"
	@echo "  4. Edit .env and set your GOOGLE_KEY (required for Gemini models)"
	@echo "  5. make init-schema"
	@echo "  6. make start"
	@echo "  7. make generate-data"
	@echo "  8. Open http://localhost:3080 (LibreChat)"

# ---------------------------------------------------------------------------
# Lifecycle targets
# ---------------------------------------------------------------------------
start:
	@echo "Starting workshop services (mode: $(DEPLOY_MODE))..."
	@cp librechat.$(DEPLOY_MODE).yaml librechat.yaml
	@echo "[OK] Generated librechat.yaml from librechat.$(DEPLOY_MODE).yaml"
	$(COMPOSE_CMD) up -d
	@echo "[OK] Services started"
	$(SERVICE_TABLE)
	@echo "Waiting for services to be ready..."
	@sleep 15
	@# Create default workshop user (idempotent -- fails silently if user exists)
	@LC_EMAIL=$$(grep -s '^LIBRECHAT_USER_EMAIL=' .env 2>/dev/null | cut -d= -f2) && \
	 LC_NAME=$$(grep -s '^LIBRECHAT_USER_NAME=' .env 2>/dev/null | cut -d= -f2) && \
	 LC_PASS=$$(grep -s '^LIBRECHAT_USER_PASSWORD=' .env 2>/dev/null | cut -d= -f2) && \
	 LC_USER=$$(echo "$$LC_EMAIL" | cut -d@ -f1) && \
	 echo "y" | docker exec -i telco-librechat sh -c \
		"cd /app && node config/create-user.js $$LC_EMAIL $$LC_NAME $$LC_USER $$LC_PASS" \
		>/dev/null 2>&1 && \
		echo "[OK] Default user created: $$LC_EMAIL / $$LC_PASS" || \
		echo "[OK] Default user already exists: $$LC_EMAIL / $$LC_PASS"

stop:
	@echo "Stopping workshop services..."
	$(COMPOSE_CMD) down
	@echo "[OK] Services stopped"

restart: stop start

generate-data:
	@echo "Generating telco data..."
	$(COMPOSE_CMD) --profile generator up data-generator
	@echo "[OK] Data generation complete"

# ---------------------------------------------------------------------------
# Verification & Exploration (mode-aware ClickHouse client)
# ---------------------------------------------------------------------------
# init-schema, COMPOSE_CMD, CH_CLIENT, CH_INTERACTIVE are defined in the
# included Makefile.$(DEPLOY_MODE).mk file.

check-db:
	@echo "Checking database connectivity and data..."
	@echo "============================================"
	@echo ""
	@echo "--- Connectivity Test ---"
	$(CH_CLIENT) --query "SELECT 'ClickHouse is running' AS status"
	@echo ""
	@echo "--- Databases ---"
	$(CH_CLIENT) --query "SHOW DATABASES"
	@echo ""
	@echo "--- Tables in telco ---"
	$(CH_CLIENT) --query "SHOW TABLES FROM telco"
	@echo ""
	@echo "--- Row Counts ---"
	$(CH_CLIENT) --query "SELECT * FROM (SELECT 'customers' AS table_name, count() AS rows FROM telco.customers UNION ALL SELECT 'call_detail_records', count() FROM telco.call_detail_records UNION ALL SELECT 'network_events', count() FROM telco.network_events UNION ALL SELECT 'marketing_campaigns', count() FROM telco.marketing_campaigns) ORDER BY table_name FORMAT PrettyCompact"
	@echo ""
	@echo "[OK] Database check complete"

explore-data:
	@echo "Exploring telco data..."
	@echo "============================================"
	@echo ""
	@echo "--- Databases ---"
	$(CH_CLIENT) --query "SHOW DATABASES"
	@echo ""
	@echo "--- Tables in telco ---"
	$(CH_CLIENT) --query "SHOW TABLES FROM telco"
	@echo ""
	@echo "--- Row Counts ---"
	$(CH_CLIENT) --query "SELECT * FROM (SELECT 'customers' AS table_name, count() AS rows FROM telco.customers UNION ALL SELECT 'call_detail_records', count() FROM telco.call_detail_records UNION ALL SELECT 'network_events', count() FROM telco.network_events UNION ALL SELECT 'marketing_campaigns', count() FROM telco.marketing_campaigns) ORDER BY table_name FORMAT PrettyCompact"
	@echo ""
	@echo "--- Sample Customers (5 rows) ---"
	$(CH_CLIENT) --query "SELECT customer_id, first_name, last_name, segment, plan_type, monthly_spend, churn_probability FROM telco.customers LIMIT 5 FORMAT PrettyCompact"
	@echo ""
	@echo "--- Sample Call Detail Records (5 rows) ---"
	$(CH_CLIENT) --query "SELECT cdr_id, customer_id, timestamp, event_type, duration_seconds, data_mb, cost FROM telco.call_detail_records LIMIT 5 FORMAT PrettyCompact"
	@echo ""
	@echo "--- Sample Network Events (5 rows) ---"
	$(CH_CLIENT) --query "SELECT event_id, timestamp, event_type, base_station_id, region, technology, is_anomaly FROM telco.network_events LIMIT 5 FORMAT PrettyCompact"
	@echo ""
	@echo "--- Sample Marketing Campaigns (5 rows) ---"
	$(CH_CLIENT) --query "SELECT campaign_id, campaign_name, campaign_type, target_segment, channel, budget, conversions, revenue_generated FROM telco.marketing_campaigns LIMIT 5 FORMAT PrettyCompact"
	@echo ""
	@echo "--- Customer Segment Distribution ---"
	$(CH_CLIENT) --query "SELECT segment, count() AS customer_count, round(avg(monthly_spend), 2) AS avg_spend, round(avg(churn_probability), 3) AS avg_churn FROM telco.customers GROUP BY segment ORDER BY customer_count DESC FORMAT PrettyCompact"
	@echo ""
	@echo "--- Top 10 Regions by Network Events ---"
	$(CH_CLIENT) --query "SELECT region, technology, count() AS event_count FROM telco.network_events GROUP BY region, technology ORDER BY event_count DESC LIMIT 10 FORMAT PrettyCompact"
	@echo ""
	@echo "[OK] Data exploration complete"

query:
	@echo "Opening interactive ClickHouse SQL shell..."
	@echo "Type SQL queries directly. Use Ctrl+D or 'exit' to quit."
	@echo ""
	$(CH_INTERACTIVE)

# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
REPO_ROOT := $(shell cd ../.. && pwd)
TEST_DIR := $(REPO_ROOT)/test/telco_marketing

test:
	@echo "Testing workshop setup..."
	pip install -q clickhouse-connect python-dotenv langfuse
	python $(TEST_DIR)/test_setup.py

test-all:
	@echo "Running end-to-end test suite..."
	docker build -t telco-tests -f $(TEST_DIR)/Dockerfile $(TEST_DIR)
	docker run --rm \
		-v $(REPO_ROOT):/repo \
		-e PROJECT_ROOT=/repo/agent_stack_builds/telco_marketing \
		telco-tests python /repo/test/telco_marketing/test_all.py
	@echo "[OK] All tests passed"

logs:
	$(COMPOSE_CMD) logs -f

clean-data:
	@echo "Truncating all telco tables for a fresh data reload..."
	$(CH_CLIENT) --query "TRUNCATE TABLE IF EXISTS telco.customers"
	$(CH_CLIENT) --query "TRUNCATE TABLE IF EXISTS telco.call_detail_records"
	$(CH_CLIENT) --query "TRUNCATE TABLE IF EXISTS telco.network_events"
	$(CH_CLIENT) --query "TRUNCATE TABLE IF EXISTS telco.marketing_campaigns"
	$(CH_CLIENT) --query "TRUNCATE TABLE IF EXISTS telco.customer_usage_summary"
	$(CH_CLIENT) --query "TRUNCATE TABLE IF EXISTS telco.network_health_summary"
	$(CH_CLIENT) --query "TRUNCATE TABLE IF EXISTS telco.campaign_performance"
	@echo "[OK] All telco tables truncated. Run 'make generate-data' to reload."

clean:
	@echo "[WARN] This will stop all services, remove Docker volumes, and delete generated files."
	@echo "       Your .env file will NOT be deleted."
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(COMPOSE_CMD) down -v --remove-orphans; \
		rm -f librechat.yaml; \
		echo "[OK] All services stopped, volumes removed, and generated files deleted."; \
		echo "     To start fresh: make start && make generate-data"; \
	fi
