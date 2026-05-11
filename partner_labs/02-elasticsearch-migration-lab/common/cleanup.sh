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
# ────────────────────────────────────────
PART3_COMPOSE="$LAB_ROOT/part3/docker/docker-compose.target.yml"
if [ -f "$PART3_COMPOSE" ]; then
  echo "Stopping Part 3 target stack..."
  docker compose -f "$PART3_COMPOSE" down -v --remove-orphans 2>/dev/null && \
    echo "[DONE] Part 3 target stack removed." || \
    echo "[SKIP] Part 3 stack not running."
else
  echo "[SKIP] Part 3 docker-compose.target.yml not found."
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
