#!/bin/bash
set -euo pipefail

# ================================================================
# Lab Cleanup Script
# Tears down all resources provisioned across Part 1 and Part 3
# ================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_ROOT="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo " ClickHouse Migration Lab — Cleanup"
echo "============================================"
echo ""
echo "This will stop and remove all Docker containers,"
echo "volumes, and images created by the lab."
echo ""
read -r -p "Are you sure? (yes/no): " confirm
if [[ "$confirm" != "yes" ]]; then
  echo "Aborted."
  exit 0
fi
echo ""

# ────────────────────────────────────────
# Part 1: Source stack (Elasticsearch)
# ────────────────────────────────────────
PART1_COMPOSE="$LAB_ROOT/part1/docker/docker-compose.source.yml"
if [ -f "$PART1_COMPOSE" ]; then
  echo "Stopping Part 1 source stack..."
  docker compose -f "$PART1_COMPOSE" down -v --remove-orphans 2>/dev/null && \
    echo "[DONE] Part 1 source stack removed (containers + volumes)." || \
    echo "[SKIP] Part 1 stack not running."
else
  echo "[SKIP] Part 1 docker-compose.source.yml not found."
fi

# ────────────────────────────────────────
# Part 3: Target stack (OTel + HyperDX)
# Part 3 uses two overlay compose files (parallel-run and cutover variants)
# layered on top of Part 1's source + otel-demo compose files, plus a
# standalone otelcol-lab container started via `docker run`.
# ────────────────────────────────────────
PART1_SRC_COMPOSE="$LAB_ROOT/part1/docker/docker-compose.source.yml"
PART1_OTEL_COMPOSE="$LAB_ROOT/part1/docker/docker-compose.otel-demo.yml"
for PHASE in parallel cutover; do
  OVERLAY="$LAB_ROOT/part3/docker/docker-compose.otel-demo.${PHASE}.yml"
  if [ -f "$OVERLAY" ] && [ -f "$PART1_SRC_COMPOSE" ] && [ -f "$PART1_OTEL_COMPOSE" ]; then
    echo "Stopping Part 3 ${PHASE} overlay..."
    docker compose \
      -f "$PART1_SRC_COMPOSE" \
      -f "$PART1_OTEL_COMPOSE" \
      -f "$OVERLAY" \
      down -v --remove-orphans 2>/dev/null && \
      echo "[DONE] Part 3 ${PHASE} overlay removed." || \
      echo "[SKIP] Part 3 ${PHASE} overlay not running."
  fi
done

# Standalone otelcol-lab container (started via `docker run`, not compose)
if docker ps -a --format '{{.Names}}' | grep -q '^otelcol-lab$'; then
  echo "Removing standalone otelcol-lab container..."
  docker rm -f otelcol-lab >/dev/null && \
    echo "[DONE] otelcol-lab container removed." || \
    echo "[SKIP] otelcol-lab removal failed."
fi

# ────────────────────────────────────────
# Terraform resources (EC2 only)
# ────────────────────────────────────────
TERRAFORM_DIR="$LAB_ROOT/part1/terraform"
if [ -f "$TERRAFORM_DIR/terraform.tfstate" ]; then
  echo ""
  echo "Terraform state detected. To destroy EC2 resources:"
  echo "  cd $TERRAFORM_DIR"
  echo "  terraform destroy"
  echo ""
  read -r -p "Run terraform destroy now? (yes/no): " tf_confirm
  if [[ "$tf_confirm" == "yes" ]]; then
    cd "$TERRAFORM_DIR"
    terraform destroy -auto-approve
    echo "[DONE] EC2 resources destroyed."
    cd "$LAB_ROOT"
  fi
fi

# ────────────────────────────────────────
# Prune unused Docker resources
# ────────────────────────────────────────
echo ""
read -r -p "Prune unused Docker images and build cache? (yes/no): " prune_confirm
if [[ "$prune_confirm" == "yes" ]]; then
  docker system prune -f
  echo "[DONE] Docker system pruned."
fi

echo ""
echo "Cleanup complete."
