.PHONY: help start stop restart logs status clean reset \
        logs-librechat logs-litellm logs-mongodb logs-postgres \
        test-clickhouse test-langfuse shell-litellm backup

# Default target
.DEFAULT_GOAL := help

# Colors for output
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
NC     := \033[0m # No Color

##@ General

help: ## Display this help message
	@echo "$(GREEN)AI-Powered Observability Analytics Workshop$(NC)"
	@echo "$(YELLOW)Available commands:$(NC)"
	@awk 'BEGIN {FS = ":.*##"; printf "\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(YELLOW)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Workshop Operations

start: ## Start all services
	@echo "$(GREEN)Starting Observability Workshop services...$(NC)"
	@if [ ! -f .env ]; then \
		echo "$(RED)Error: .env file not found. Copy .env.example to .env and configure it.$(NC)"; \
		exit 1; \
	fi
	docker-compose up -d
	@echo "$(GREEN)Services starting...$(NC)"
	@echo "Waiting for services to initialize (30 seconds)..."
	@sleep 30
	@make status
	@echo ""
	@echo "$(GREEN)✓ Workshop is ready!$(NC)"
	@echo ""
	@echo "Access LibreChat at: http://localhost:3000"
	@echo "Access Langfuse at: https://cloud.langfuse.com"
	@echo ""
	@echo "Try these commands:"
	@echo "  make logs           - View all service logs"
	@echo "  make test-clickhouse - Test ClickHouse connection"
	@echo "  make test-langfuse   - Test Langfuse connection"

stop: ## Stop all services
	@echo "$(YELLOW)Stopping all services...$(NC)"
	docker-compose stop
	@echo "$(GREEN)✓ Services stopped$(NC)"

restart: ## Restart all services
	@echo "$(YELLOW)Restarting all services...$(NC)"
	docker-compose restart
	@make status

status: ## Check service health
	@echo "$(GREEN)Service Status:$(NC)"
	@docker-compose ps

##@ Logs

logs: ## View logs from all services
	docker-compose logs -f

logs-librechat: ## View LibreChat logs
	docker-compose logs -f librechat

logs-litellm: ## View LiteLLM logs
	docker-compose logs -f litellm

logs-mongodb: ## View MongoDB logs
	docker-compose logs -f mongodb

logs-postgres: ## View PostgreSQL logs
	docker-compose logs -f postgres

logs-meilisearch: ## View MeiliSearch logs
	docker-compose logs -f meilisearch

##@ Testing & Debugging

test-clickhouse: ## Test ClickHouse connection
	@echo "$(GREEN)Testing ClickHouse connection...$(NC)"
	@if [ -z "$$CLICKHOUSE_HOST" ]; then \
		echo "$(RED)Error: CLICKHOUSE_HOST not set in .env$(NC)"; \
		exit 1; \
	fi
	@docker exec litellm sh -c 'clickhouse-client \
		--host="$$CLICKHOUSE_HOST" \
		--port="$$CLICKHOUSE_PORT" \
		--user="$$CLICKHOUSE_USER" \
		--password="$$CLICKHOUSE_PASSWORD" \
		--secure \
		--query="SELECT version() as version"' || \
		echo "$(RED)Connection failed. Check your ClickHouse credentials in .env$(NC)"

test-langfuse: ## Test Langfuse connection
	@echo "$(GREEN)Testing Langfuse connection...$(NC)"
	@if [ -z "$$LANGFUSE_PUBLIC_KEY" ]; then \
		echo "$(RED)Error: LANGFUSE_PUBLIC_KEY not set in .env$(NC)"; \
		exit 1; \
	fi
	@docker exec litellm sh -c 'curl -s -f \
		-H "Authorization: Bearer $$LANGFUSE_PUBLIC_KEY" \
		$$LANGFUSE_HOST/api/public/health' && \
		echo "$(GREEN)✓ Langfuse connection successful$(NC)" || \
		echo "$(RED)Connection failed. Check your Langfuse credentials in .env$(NC)"

test-llm: ## Test LLM provider connection
	@echo "$(GREEN)Testing LLM provider...$(NC)"
	@docker exec litellm sh -c 'curl -s http://localhost:4000/health' && \
		echo "$(GREEN)✓ LiteLLM is healthy$(NC)" || \
		echo "$(RED)LiteLLM health check failed$(NC)"

shell-litellm: ## Open shell in LiteLLM container
	docker exec -it litellm sh

shell-librechat: ## Open shell in LibreChat container
	docker exec -it librechat sh

##@ Database Operations

db-backup: ## Backup MongoDB data
	@echo "$(GREEN)Backing up MongoDB...$(NC)"
	@mkdir -p backups
	docker exec mongodb mongodump --out=/tmp/backup
	docker cp mongodb:/tmp/backup ./backups/mongodb-$(shell date +%Y%m%d-%H%M%S)
	@echo "$(GREEN)✓ Backup complete$(NC)"

db-restore: ## Restore MongoDB from backup (set BACKUP_DIR=path)
	@if [ -z "$(BACKUP_DIR)" ]; then \
		echo "$(RED)Error: Set BACKUP_DIR=path/to/backup$(NC)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Restoring MongoDB from $(BACKUP_DIR)...$(NC)"
	docker cp $(BACKUP_DIR) mongodb:/tmp/restore
	docker exec mongodb mongorestore /tmp/restore
	@echo "$(GREEN)✓ Restore complete$(NC)"

##@ Cleanup

clean: ## Stop and remove all containers
	@echo "$(YELLOW)Stopping and removing all containers...$(NC)"
	docker-compose down
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

clean-volumes: ## Stop containers and remove all volumes (WARNING: destroys data)
	@echo "$(RED)WARNING: This will delete all data including chat history!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		echo "$(GREEN)✓ All volumes removed$(NC)"; \
	else \
		echo "$(YELLOW)Cancelled$(NC)"; \
	fi

reset: ## Full reset - stop, clean, and restart
	@echo "$(YELLOW)Performing full reset...$(NC)"
	@make clean
	@echo "$(GREEN)Starting fresh...$(NC)"
	@make start

##@ Development

pull: ## Pull latest container images
	@echo "$(GREEN)Pulling latest images...$(NC)"
	docker-compose pull

build: ## Rebuild containers (if using custom images)
	@echo "$(GREEN)Building containers...$(NC)"
	docker-compose build

update: pull restart ## Update all services to latest versions

##@ Workshop Setup

setup: ## Initial workshop setup
	@echo "$(GREEN)Running initial workshop setup...$(NC)"
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(YELLOW)Created .env file. Please edit it with your credentials.$(NC)"; \
		exit 1; \
	fi
	@make test-env
	@echo "$(GREEN)Environment configured correctly$(NC)"
	@make start

test-env: ## Validate environment configuration
	@echo "$(GREEN)Validating environment...$(NC)"
	@bash -c 'source .env && \
		if [ -z "$$CLICKHOUSE_HOST" ]; then echo "$(RED)Missing: CLICKHOUSE_HOST$(NC)"; exit 1; fi; \
		if [ -z "$$LANGFUSE_PUBLIC_KEY" ]; then echo "$(RED)Missing: LANGFUSE_PUBLIC_KEY$(NC)"; exit 1; fi; \
		if [ -z "$$OPENAI_API_KEY" ] && [ -z "$$ANTHROPIC_API_KEY" ]; then echo "$(RED)Missing: OPENAI_API_KEY or ANTHROPIC_API_KEY$(NC)"; exit 1; fi; \
		echo "$(GREEN)✓ All required variables set$(NC)"'

##@ Information

info: ## Display workshop information
	@echo "$(GREEN)Workshop Information$(NC)"
	@echo ""
	@echo "$(YELLOW)Services:$(NC)"
	@echo "  LibreChat:     http://localhost:3000"
	@echo "  LiteLLM API:   http://localhost:4000"
	@echo "  MeiliSearch:   http://localhost:7700"
	@echo "  MongoDB:       mongodb://localhost:27017"
	@echo "  PostgreSQL:    postgresql://localhost:5432"
	@echo ""
	@echo "$(YELLOW)External Services:$(NC)"
	@echo "  Langfuse:      https://cloud.langfuse.com"
	@echo "  ClickHouse:    $${CLICKHOUSE_HOST:-Not configured}"
	@echo ""
	@echo "$(YELLOW)Useful Commands:$(NC)"
	@echo "  make logs           - View all logs"
	@echo "  make status         - Check service health"
	@echo "  make restart        - Restart services"
	@echo "  make clean          - Stop and remove containers"

version: ## Show service versions
	@echo "$(GREEN)Service Versions:$(NC)"
	@docker exec librechat cat package.json | grep version || echo "LibreChat: Latest"
	@docker exec litellm litellm --version || echo "LiteLLM: Latest"
	@docker exec mongodb mongod --version | head -1
	@docker exec postgres postgres --version

urls: ## Display all service URLs
	@echo "$(GREEN)Service URLs:$(NC)"
	@echo ""
	@echo "$(YELLOW)Local Services:$(NC)"
	@echo "  LibreChat UI:        http://localhost:3000"
	@echo "  LiteLLM API:         http://localhost:4000"
	@echo "  LiteLLM Docs:        http://localhost:4000/docs"
	@echo "  LiteLLM Health:      http://localhost:4000/health"
	@echo ""
	@echo "$(YELLOW)Cloud Services:$(NC)"
	@echo "  Langfuse Dashboard:  https://cloud.langfuse.com"
	@echo "  ClickHouse Console:  https://clickhouse.cloud"
