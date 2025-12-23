#!/bin/bash
# End-to-end tests for Incremental Materialized Views demo
# Usage: ./run_tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Load environment variables
if [ -f .env ]; then
    source .env
else
    echo -e "${RED}ERROR: .env file not found. Copy .env.example to .env and configure.${NC}"
    exit 1
fi

# Build clickhouse-client command based on environment
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_SECURE="${CLICKHOUSE_SECURE:-false}"

# Validate required settings
if [ -z "$CLICKHOUSE_HOST" ]; then
    echo -e "${RED}ERROR: CLICKHOUSE_HOST is not set in .env${NC}"
    exit 1
fi

if [ -z "$CLICKHOUSE_USER" ]; then
    echo -e "${RED}ERROR: CLICKHOUSE_USER is not set in .env${NC}"
    exit 1
fi

if [ "$CLICKHOUSE_SECURE" = "true" ]; then
    # ClickHouse Cloud - password is required
    if [ -z "$CLICKHOUSE_PASSWORD" ]; then
        echo -e "${RED}ERROR: CLICKHOUSE_PASSWORD is required for ClickHouse Cloud${NC}"
        exit 1
    fi
    CH_CMD="clickhouse-client --host=$CLICKHOUSE_HOST --port=9440 --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --secure"
else
    # Local ClickHouse
    CH_CMD="clickhouse-client --host=$CLICKHOUSE_HOST --user=$CLICKHOUSE_USER"
    if [ -n "$CLICKHOUSE_PASSWORD" ]; then
        CH_CMD="$CH_CMD --password=$CLICKHOUSE_PASSWORD"
    fi
fi

echo "=========================================="
echo "ClickHouse Connection: $CLICKHOUSE_HOST"
echo "=========================================="

# Helper function to run SQL file
run_sql() {
    local file="$1"
    echo -e "${YELLOW}Running: $file${NC}"
    $CH_CMD --queries-file "$file" 2>&1 || {
        echo -e "${RED}FAILED: $file${NC}"
        return 1
    }
}

# Helper function to run SQL query and return result
query() {
    $CH_CMD --query "$1" 2>/dev/null
}

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

test_pass() {
    echo -e "${GREEN}PASS: $1${NC}"
    ((TESTS_PASSED++))
}

test_fail() {
    echo -e "${RED}FAIL: $1${NC}"
    ((TESTS_FAILED++))
}

# ==========================================
# CLEANUP FIRST
# ==========================================
echo ""
echo "=========================================="
echo "Cleanup: Dropping all demo databases"
echo "=========================================="
run_sql "MASTER_CLEANUP.sql" || true

# ==========================================
# EXAMPLE 1: Basic Incremental MVs
# ==========================================
echo ""
echo "=========================================="
echo "Example 1: Basic Incremental MVs"
echo "=========================================="

run_sql "01_basic_incremental_mvs/00_setup.sql"
run_sql "01_basic_incremental_mvs/01_basic_mv.sql"
run_sql "01_basic_incremental_mvs/02_queries.sql"

# Validate: Check that page_views_count has data
COUNT=$(query "SELECT count() FROM mv_demo_basic.page_views_count")
if [ "$COUNT" -gt 0 ]; then
    test_pass "Example 1 - page_views_count has $COUNT rows"
else
    test_fail "Example 1 - page_views_count is empty"
fi

run_sql "01_basic_incremental_mvs/99_cleanup.sql"

# ==========================================
# EXAMPLE 2: SummingMergeTree
# ==========================================
echo ""
echo "=========================================="
echo "Example 2: SummingMergeTree"
echo "=========================================="

run_sql "02_summing_merge_tree/00_setup.sql"
run_sql "02_summing_merge_tree/01_summing_tables.sql"
run_sql "02_summing_merge_tree/02_mvs.sql"
run_sql "02_summing_merge_tree/03_queries.sql"

# Validate: Check that hourly_metrics has data
COUNT=$(query "SELECT count() FROM mv_demo_summing.hourly_metrics")
if [ "$COUNT" -gt 0 ]; then
    test_pass "Example 2 - hourly_metrics has $COUNT rows"
else
    test_fail "Example 2 - hourly_metrics is empty"
fi

# Validate: Check that sums are calculated
PAGEVIEWS=$(query "SELECT sum(pageviews) FROM mv_demo_summing.hourly_metrics")
if [ "$PAGEVIEWS" -gt 0 ]; then
    test_pass "Example 2 - pageviews sum = $PAGEVIEWS"
else
    test_fail "Example 2 - pageviews sum is 0"
fi

run_sql "02_summing_merge_tree/99_cleanup.sql"

# ==========================================
# EXAMPLE 3: AggregatingMergeTree
# ==========================================
echo ""
echo "=========================================="
echo "Example 3: AggregatingMergeTree"
echo "=========================================="

run_sql "03_aggregating_merge_tree/00_setup.sql"
run_sql "03_aggregating_merge_tree/01_aggregating_tables.sql"
run_sql "03_aggregating_merge_tree/02_state_merge_mvs.sql"
run_sql "03_aggregating_merge_tree/03_queries.sql"

# Validate: Check that hourly_sales has data
COUNT=$(query "SELECT count() FROM mv_demo_aggregating.hourly_sales")
if [ "$COUNT" -gt 0 ]; then
    test_pass "Example 3 - hourly_sales has $COUNT rows"
else
    test_fail "Example 3 - hourly_sales is empty"
fi

# Validate: Check unique customer count (should be 3 for Electronics after all inserts)
UNIQUE=$(query "SELECT uniqMerge(unique_customers) FROM mv_demo_aggregating.hourly_sales WHERE category = 'Electronics'")
if [ "$UNIQUE" -eq 3 ]; then
    test_pass "Example 3 - Electronics unique customers = $UNIQUE (correct)"
else
    test_fail "Example 3 - Electronics unique customers = $UNIQUE (expected 3)"
fi

run_sql "03_aggregating_merge_tree/99_cleanup.sql"

# ==========================================
# EXAMPLE 4: Dictionaries
# ==========================================
echo ""
echo "=========================================="
echo "Example 4: Dictionaries"
echo "=========================================="

run_sql "04_dictionaries/00_setup.sql"
run_sql "04_dictionaries/01_dictionaries.sql"
run_sql "04_dictionaries/02_dictget_examples.sql"
run_sql "04_dictionaries/03_join_vs_dictget.sql"
run_sql "04_dictionaries/04_mv_with_dictionaries.sql"

# Validate: Check that dictionaries are loaded
DICT_COUNT=$(query "SELECT count() FROM system.dictionaries WHERE database = 'mv_demo_dictionaries'")
if [ "$DICT_COUNT" -eq 2 ]; then
    test_pass "Example 4 - Dictionaries created: $DICT_COUNT"
else
    test_fail "Example 4 - Expected 2 dictionaries, found $DICT_COUNT"
fi

# Validate: Check that enriched orders have data
ENRICHED_COUNT=$(query "SELECT count() FROM mv_demo_dictionaries.orders_enriched")
if [ "$ENRICHED_COUNT" -gt 0 ]; then
    test_pass "Example 4 - orders_enriched has $ENRICHED_COUNT rows"
else
    test_fail "Example 4 - orders_enriched is empty"
fi

# Validate: Check dictGet works (lookup product name)
PRODUCT_NAME=$(query "SELECT dictGet('mv_demo_dictionaries.products_dict', 'product_name', toUInt32(1))")
if [ "$PRODUCT_NAME" = "Laptop Pro 15" ]; then
    test_pass "Example 4 - dictGet lookup works"
else
    test_fail "Example 4 - dictGet lookup failed (got: $PRODUCT_NAME)"
fi

run_sql "04_dictionaries/99_cleanup.sql"

# ==========================================
# EXAMPLE 5: Medallion Architecture
# ==========================================
echo ""
echo "=========================================="
echo "Example 5: Medallion Architecture"
echo "=========================================="

# Setup
run_sql "05_medallion_architecture/01_setup/00_config.sql"
run_sql "05_medallion_architecture/01_setup/01_dimensions.sql"

# Generate dimensions with Python
echo -e "${YELLOW}Running: Python generate_dimensions.py${NC}"
python3 05_medallion_architecture/scripts/generate_dimensions.py || {
    echo -e "${RED}FAILED: generate_dimensions.py${NC}"
    test_fail "Example 5 - generate_dimensions.py"
}

# Bronze
run_sql "05_medallion_architecture/02_bronze/10_bronze_tables.sql"

# Silver
run_sql "05_medallion_architecture/03_silver/20_silver_tables.sql"
run_sql "05_medallion_architecture/03_silver/21_incremental_mvs.sql"
run_sql "05_medallion_architecture/03_silver/22_dictionaries.sql"

# Gold
run_sql "05_medallion_architecture/04_gold/30_gold_minute.sql"
run_sql "05_medallion_architecture/04_gold/31_gold_hourly.sql"

# Generate test events with Python
echo -e "${YELLOW}Running: Python generate_events.py --count 1000${NC}"
python3 05_medallion_architecture/scripts/generate_events.py --count 1000 || {
    echo -e "${RED}FAILED: generate_events.py${NC}"
    test_fail "Example 5 - generate_events.py"
}

# Validate Bronze layer
BRONZE_COUNT=$(query "SELECT count() FROM fastmart_demo.events_raw")
if [ "$BRONZE_COUNT" -gt 0 ]; then
    test_pass "Example 5 - Bronze (events_raw) has $BRONZE_COUNT rows"
else
    test_fail "Example 5 - Bronze (events_raw) is empty"
fi

# Validate Silver layer
SILVER_COUNT=$(query "SELECT count() FROM fastmart_demo.orders_enriched")
if [ "$SILVER_COUNT" -gt 0 ]; then
    test_pass "Example 5 - Silver (orders_enriched) has $SILVER_COUNT rows"
else
    test_fail "Example 5 - Silver (orders_enriched) is empty"
fi

# Validate Gold layer
GOLD_COUNT=$(query "SELECT count() FROM fastmart_demo.sales_by_minute")
if [ "$GOLD_COUNT" -gt 0 ]; then
    test_pass "Example 5 - Gold (sales_by_minute) has $GOLD_COUNT rows"
else
    test_fail "Example 5 - Gold (sales_by_minute) is empty"
fi

# Validate cascading (hour should have data if minute has data)
HOUR_COUNT=$(query "SELECT count() FROM fastmart_demo.sales_by_hour")
if [ "$HOUR_COUNT" -gt 0 ]; then
    test_pass "Example 5 - Gold (sales_by_hour) has $HOUR_COUNT rows"
else
    test_fail "Example 5 - Gold (sales_by_hour) is empty"
fi

run_sql "05_medallion_architecture/06_cleanup/99_cleanup.sql"

# ==========================================
# FINAL CLEANUP
# ==========================================
echo ""
echo "=========================================="
echo "Final Cleanup"
echo "=========================================="
run_sql "MASTER_CLEANUP.sql" || true

# ==========================================
# SUMMARY
# ==========================================
echo ""
echo "=========================================="
echo "TEST SUMMARY"
echo "=========================================="
echo -e "${GREEN}Passed: $TESTS_PASSED${NC}"
echo -e "${RED}Failed: $TESTS_FAILED${NC}"
echo "=========================================="

if [ $TESTS_FAILED -gt 0 ]; then
    exit 1
fi

echo -e "${GREEN}All tests passed!${NC}"
