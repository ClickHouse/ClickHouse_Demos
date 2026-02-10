# Makefile.local.mk -- Local deployment: all services in Docker
# Included by Makefile when DEPLOY_MODE=local

COMPOSE_CMD := docker compose -f docker-compose.yml -f docker-compose.local.yml

CH_CLIENT := @docker exec telco-clickhouse clickhouse-client \
		--user default --password clickhouse

CH_INTERACTIVE := @docker exec -it telco-clickhouse clickhouse-client \
		--user default --password clickhouse -d telco

# Service table displayed after `make start`
define SERVICE_TABLE
	@echo ""
	@echo "  Service      URL                         Credentials"
	@echo "  ----------   -------------------------   --------------------------------"
	@LC_EMAIL=$$(grep -s '^LIBRECHAT_USER_EMAIL=' .env 2>/dev/null | cut -d= -f2) && \
	 LC_PASS=$$(grep -s '^LIBRECHAT_USER_PASSWORD=' .env 2>/dev/null | cut -d= -f2) && \
	 echo "  LibreChat    http://localhost:3080        $$LC_EMAIL / $$LC_PASS"
	@echo "  Langfuse     http://localhost:3000        admin@telco.local / admin123"
	@echo "  LiteLLM      http://localhost:4000"
	@echo "  ClickHouse   http://localhost:8124        default / clickhouse"
	@echo ""
endef

# init-schema: use the already-running ClickHouse container
init-schema:
	@echo "Pushing schema to ClickHouse..."
	@docker exec telco-clickhouse clickhouse-client \
		--user default --password clickhouse \
		--queries-file /docker-entrypoint-initdb.d/init.sql
	@echo "[OK] Schema initialized"
