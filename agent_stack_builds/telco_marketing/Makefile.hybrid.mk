# Makefile.hybrid.mk -- Hybrid deployment: ClickHouse Cloud + Langfuse Cloud
# Included by Makefile when DEPLOY_MODE=hybrid

COMPOSE_CMD := docker compose -f docker-compose.yml

# Helper to read a variable from .env
env_val = $(shell grep -s '^$(1)=' .env 2>/dev/null | cut -d= -f2)

CH_CLIENT := @docker run --rm --network=host clickhouse/clickhouse-server clickhouse-client \
		--host=$(call env_val,CLICKHOUSE_HOST) \
		--port=$(call env_val,CLICKHOUSE_PORT) \
		--user=$(call env_val,CLICKHOUSE_USER) \
		--password=$(call env_val,CLICKHOUSE_PASSWORD) \
		--secure

CH_INTERACTIVE := @docker run --rm -it --network=host clickhouse/clickhouse-server clickhouse-client \
		--host=$(call env_val,CLICKHOUSE_HOST) \
		--port=$(call env_val,CLICKHOUSE_PORT) \
		--user=$(call env_val,CLICKHOUSE_USER) \
		--password=$(call env_val,CLICKHOUSE_PASSWORD) \
		--secure \
		-d telco

# Service table displayed after `make start`
define SERVICE_TABLE
	@echo ""
	@echo "  Service      URL                         Credentials"
	@echo "  ----------   -------------------------   --------------------------------"
	@LC_EMAIL=$$(grep -s '^LIBRECHAT_USER_EMAIL=' .env 2>/dev/null | cut -d= -f2) && \
	 LC_PASS=$$(grep -s '^LIBRECHAT_USER_PASSWORD=' .env 2>/dev/null | cut -d= -f2) && \
	 echo "  LibreChat    http://localhost:3080        $$LC_EMAIL / $$LC_PASS"
	@echo "  LiteLLM      http://localhost:4000"
	@echo "  ClickHouse   (cloud -- see .env)"
	@echo "  Langfuse     (cloud -- see .env)"
	@echo ""
endef

# init-schema: run ephemeral container to push schema to ClickHouse Cloud
init-schema:
	@echo "Pushing schema to ClickHouse Cloud..."
	@docker run --rm \
		-v $(PWD)/clickhouse/init.sql:/init.sql \
		clickhouse/clickhouse-server clickhouse-client \
		--host=$(call env_val,CLICKHOUSE_HOST) \
		--port=$(call env_val,CLICKHOUSE_PORT) \
		--user=$(call env_val,CLICKHOUSE_USER) \
		--password=$(call env_val,CLICKHOUSE_PASSWORD) \
		--secure \
		--queries-file /init.sql
	@echo "[OK] Schema initialized"
