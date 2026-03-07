#!/usr/bin/env bash

# scripts/deploy.sh — Silver Layer deployment

# Usage:
#   bash scripts/deploy.sh               # deploy to dev (default)
#   bash scripts/deploy.sh staging       # deploy to staging
#   bash scripts/deploy.sh prod          # deploy to prod
#
# What it does:
#   1. Validates the Databricks Asset Bundle
#   2. Deploys to the target workspace
#
# All pipeline configuration is driven by silver_config.csv.
# To onboard a new table: add a row to the CSV — no redeploy needed.


set -euo pipefail

TARGET="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo ""
echo "  Silver Layer — Deployment"
echo "  Target  : ${TARGET}"
echo "  Root    : ${REPO_ROOT}"

#  Step 1: Validate bundle ─
echo ""
echo "[1/2] Validating Databricks Asset Bundle ..."
databricks bundle validate --target "${TARGET}"

#  Step 2: Deploy 
echo ""
echo "[2/2] Deploying to ${TARGET} ..."
databricks bundle deploy --target "${TARGET}"

echo ""
echo "  Deployment complete — target: ${TARGET}"
echo ""
echo "  Run pipeline:"
echo "    databricks bundle run -t ${TARGET} silver_layer_pipeline"
echo ""
echo "  Run specific tables only:"
echo "    databricks bundle run -t ${TARGET} silver_layer_pipeline \\"
echo "      --param filter_ids=cust_001,orders_001"
echo ""
echo "  Full refresh all tables:"
echo "    databricks bundle run -t ${TARGET} silver_layer_pipeline \\"
echo "      --param run_mode=full_refresh"
echo ""
