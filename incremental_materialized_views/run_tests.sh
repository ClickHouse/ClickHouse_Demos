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
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Load environment variables
if [ -f .env ]; then
    source .env
else
    echo -e "${RED}ERROR: .env file not found. Copy .env.example to .env and configure.${NC}"
    exit 1
fi

# Setup Python virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
    PYTHON_CMD="python3"
else
    echo -e "${YELLOW}WARNING: venv not found. Python scripts may fail.${NC}"
    echo -e "${YELLOW}Run: python3 -m venv venv && source venv/bin/activate && pip install clickhouse-connect${NC}"
    PYTHON_CMD="python3"
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
echo "ClickHouse Incremental MVs - Test Suite"
echo "=========================================="
echo "Connection: $CLICKHOUSE_HOST"
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
    echo -e "${GREEN}  PASS: $1${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

test_fail() {
    echo -e "${RED}  FAIL: $1${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

section() {
    echo ""
    echo -e "${BLUE}===========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===========================================${NC}"
}

# ==========================================
# CLEANUP FIRST
# ==========================================
section "Step 0: Cleanup - Dropping all demo databases"
run_sql "MASTER_CLEANUP.sql" || true

# ==========================================
# EXAMPLE 1: Basic Incremental MVs
# ==========================================
section "Example 1: Basic Incremental MVs"
echo "Testing: MV as INSERT trigger, automatic aggregation"

run_sql "01_basic_incremental_mvs/00_setup.sql"
run_sql "01_basic_incremental_mvs/01_basic_mv.sql"
run_sql "01_basic_incremental_mvs/02_queries.sql"

# Validations
COUNT=$(query "SELECT count() FROM mv_demo_basic.page_views_count")
if [ "$COUNT" -gt 0 ]; then
    test_pass "page_views_count has $COUNT rows"
else
    test_fail "page_views_count is empty"
fi

# Verify MV exists
MV_EXISTS=$(query "SELECT count() FROM system.tables WHERE database='mv_demo_basic' AND engine='MaterializedView'")
if [ "$MV_EXISTS" -gt 0 ]; then
    test_pass "Materialized View exists"
else
    test_fail "Materialized View not found"
fi

# Verify aggregation worked (total should match raw inserts)
TOTAL_VIEWS=$(query "SELECT sum(view_count) FROM mv_demo_basic.page_views_count")
if [ "$TOTAL_VIEWS" -gt 0 ]; then
    test_pass "Aggregation working (total views: $TOTAL_VIEWS)"
else
    test_fail "Aggregation not working"
fi

# Performance comparison
echo -e "${YELLOW}Running performance comparison...${NC}"
run_sql "01_basic_incremental_mvs/03_performance.sql" || true

run_sql "01_basic_incremental_mvs/99_cleanup.sql"

# ==========================================
# EXAMPLE 2: SummingMergeTree
# ==========================================
section "Example 2: SummingMergeTree"
echo "Testing: Auto-sum on merge, FINAL keyword"

run_sql "02_summing_merge_tree/00_setup.sql"
run_sql "02_summing_merge_tree/01_summing_tables.sql"
run_sql "02_summing_merge_tree/02_mvs.sql"
run_sql "02_summing_merge_tree/03_queries.sql"

# Validations
COUNT=$(query "SELECT count() FROM mv_demo_summing.hourly_metrics")
if [ "$COUNT" -gt 0 ]; then
    test_pass "hourly_metrics has $COUNT rows"
else
    test_fail "hourly_metrics is empty"
fi

# Check SummingMergeTree engine (ClickHouse Cloud uses "SharedSummingMergeTree")
ENGINE=$(query "SELECT engine FROM system.tables WHERE database='mv_demo_summing' AND name='hourly_metrics'")
if [[ "$ENGINE" == *"SummingMergeTree"* ]]; then
    test_pass "hourly_metrics uses $ENGINE"
else
    test_fail "Wrong engine: $ENGINE (expected SummingMergeTree)"
fi

# Verify sums
PAGEVIEWS=$(query "SELECT sum(pageviews) FROM mv_demo_summing.hourly_metrics")
if [ "$PAGEVIEWS" -gt 0 ]; then
    test_pass "Pageviews sum = $PAGEVIEWS"
else
    test_fail "Pageviews sum is 0"
fi

# Performance comparison
echo -e "${YELLOW}Running performance comparison...${NC}"
run_sql "02_summing_merge_tree/04_performance.sql" || true

run_sql "02_summing_merge_tree/99_cleanup.sql"

# ==========================================
# EXAMPLE 3: AggregatingMergeTree
# ==========================================
section "Example 3: AggregatingMergeTree"
echo "Testing: State/Merge pattern, COUNT DISTINCT across batches"

run_sql "03_aggregating_merge_tree/00_setup.sql"
run_sql "03_aggregating_merge_tree/01_aggregating_tables.sql"
run_sql "03_aggregating_merge_tree/02_state_merge_mvs.sql"
run_sql "03_aggregating_merge_tree/03_queries.sql"

# Validations
COUNT=$(query "SELECT count() FROM mv_demo_aggregating.hourly_sales")
if [ "$COUNT" -gt 0 ]; then
    test_pass "hourly_sales has $COUNT rows"
else
    test_fail "hourly_sales is empty"
fi

# Check AggregatingMergeTree engine (ClickHouse Cloud uses "SharedAggregatingMergeTree")
ENGINE=$(query "SELECT engine FROM system.tables WHERE database='mv_demo_aggregating' AND name='hourly_sales'")
if [[ "$ENGINE" == *"AggregatingMergeTree"* ]]; then
    test_pass "hourly_sales uses $ENGINE"
else
    test_fail "Wrong engine: $ENGINE (expected AggregatingMergeTree)"
fi

# Verify unique customer count (should be 3 for Electronics)
UNIQUE=$(query "SELECT uniqMerge(unique_customers) FROM mv_demo_aggregating.hourly_sales WHERE category = 'Electronics'")
if [ "$UNIQUE" -eq 3 ]; then
    test_pass "COUNT DISTINCT works across batches (Electronics: $UNIQUE unique customers)"
else
    test_fail "COUNT DISTINCT incorrect (got $UNIQUE, expected 3)"
fi

# Verify avgMerge works
AVG_ORDER=$(query "SELECT round(avgMerge(avg_order_value), 2) FROM mv_demo_aggregating.hourly_sales WHERE category = 'Electronics'")
if [ -n "$AVG_ORDER" ]; then
    test_pass "avgMerge works (avg order: $AVG_ORDER)"
else
    test_fail "avgMerge not working"
fi

# Performance comparison
echo -e "${YELLOW}Running performance comparison...${NC}"
run_sql "03_aggregating_merge_tree/04_performance.sql" || true

run_sql "03_aggregating_merge_tree/99_cleanup.sql"

# ==========================================
# EXAMPLE 4: Dictionaries
# ==========================================
section "Example 4: Dictionaries"
echo "Testing: O(1) lookups, dictGet(), MV enrichment"

run_sql "04_dictionaries/00_setup.sql"
run_sql "04_dictionaries/01_dictionaries.sql"
run_sql "04_dictionaries/02_dictget_examples.sql"
run_sql "04_dictionaries/03_join_vs_dictget.sql"
run_sql "04_dictionaries/04_mv_with_dictionaries.sql"

# Validations
DICT_COUNT=$(query "SELECT count() FROM system.dictionaries WHERE database = 'mv_demo_dictionaries'")
if [ "$DICT_COUNT" -eq 2 ]; then
    test_pass "Dictionaries created: $DICT_COUNT"
else
    test_fail "Expected 2 dictionaries, found $DICT_COUNT"
fi

# Check dictionary status
LOADED=$(query "SELECT count() FROM system.dictionaries WHERE database = 'mv_demo_dictionaries' AND status = 'LOADED'")
if [ "$LOADED" -eq 2 ]; then
    test_pass "All dictionaries loaded into memory"
else
    test_fail "Not all dictionaries loaded (only $LOADED of 2)"
fi

# Verify dictGet lookup
PRODUCT_NAME=$(query "SELECT dictGet('mv_demo_dictionaries.products_dict', 'product_name', toUInt32(1))")
if [ "$PRODUCT_NAME" = "Laptop Pro 15" ]; then
    test_pass "dictGet lookup works"
else
    test_fail "dictGet lookup failed (got: $PRODUCT_NAME)"
fi

# Verify enriched orders have data
ENRICHED_COUNT=$(query "SELECT count() FROM mv_demo_dictionaries.orders_enriched")
if [ "$ENRICHED_COUNT" -gt 0 ]; then
    test_pass "orders_enriched has $ENRICHED_COUNT rows"
else
    test_fail "orders_enriched is empty"
fi

# Verify enrichment populated fields
ENRICHED_FIELDS=$(query "SELECT countIf(customer_name != '') FROM mv_demo_dictionaries.orders_enriched")
if [ "$ENRICHED_FIELDS" -gt 0 ]; then
    test_pass "Enrichment populated customer_name in $ENRICHED_FIELDS rows"
else
    test_fail "Enrichment did not populate customer_name"
fi

# Performance comparison
echo -e "${YELLOW}Running performance comparison...${NC}"
run_sql "04_dictionaries/05_performance.sql" || true

run_sql "04_dictionaries/99_cleanup.sql"

# ==========================================
# EXAMPLE 5: Medallion Architecture
# ==========================================
section "Example 5: Medallion Architecture"
echo "Testing: Bronze -> Silver -> Gold pipeline with all patterns"

# Setup
echo -e "${YELLOW}Step 5.1: Setup${NC}"
run_sql "05_medallion_architecture/01_setup/00_config.sql"
run_sql "05_medallion_architecture/01_setup/01_dimensions.sql"

# Generate dimensions with Python
echo -e "${YELLOW}Step 5.2: Generate dimension data (Python)${NC}"
$PYTHON_CMD 05_medallion_architecture/scripts/generate_dimensions.py --products 100 --customers 1000 --suppliers 10 || {
    echo -e "${RED}FAILED: generate_dimensions.py${NC}"
    test_fail "generate_dimensions.py failed"
}

# Verify dimensions created
PRODUCT_COUNT=$(query "SELECT count() FROM fastmart_demo.products")
CUSTOMER_COUNT=$(query "SELECT count() FROM fastmart_demo.customers")
if [ "$PRODUCT_COUNT" -gt 0 ] && [ "$CUSTOMER_COUNT" -gt 0 ]; then
    test_pass "Dimensions generated (Products: $PRODUCT_COUNT, Customers: $CUSTOMER_COUNT)"
else
    test_fail "Dimension generation failed"
fi

# Bronze
echo -e "${YELLOW}Step 5.3: Bronze layer${NC}"
run_sql "05_medallion_architecture/02_bronze/10_bronze_tables.sql"

# Silver
echo -e "${YELLOW}Step 5.4: Silver layer${NC}"
run_sql "05_medallion_architecture/03_silver/20_silver_tables.sql"
run_sql "05_medallion_architecture/03_silver/21_incremental_mvs.sql"
run_sql "05_medallion_architecture/03_silver/22_dictionaries.sql"

# Verify dictionaries loaded
DICT_STATUS=$(query "SELECT count() FROM system.dictionaries WHERE database = 'fastmart_demo' AND status = 'LOADED'")
if [ "$DICT_STATUS" -ge 2 ]; then
    test_pass "Silver dictionaries loaded: $DICT_STATUS"
else
    test_fail "Silver dictionaries not loaded (found $DICT_STATUS)"
fi

# Gold
echo -e "${YELLOW}Step 5.5: Gold layer${NC}"
run_sql "05_medallion_architecture/04_gold/30_gold_minute.sql"
run_sql "05_medallion_architecture/04_gold/31_gold_hourly.sql"

# Generate test events with Python (more events for realistic test)
echo -e "${YELLOW}Step 5.6: Generate events (Python)${NC}"
$PYTHON_CMD 05_medallion_architecture/scripts/generate_events.py --count 5000 --days 7 || {
    echo -e "${RED}FAILED: generate_events.py${NC}"
    test_fail "generate_events.py failed"
}

# Wait for MVs to process
echo -e "${YELLOW}Waiting for MVs to process...${NC}"
sleep 3

# Validate Bronze layer
BRONZE_COUNT=$(query "SELECT count() FROM fastmart_demo.events_raw")
if [ "$BRONZE_COUNT" -gt 0 ]; then
    test_pass "Bronze (events_raw): $BRONZE_COUNT events"
else
    test_fail "Bronze (events_raw) is empty"
fi

# Validate Silver layer - orders_silver
SILVER_COUNT=$(query "SELECT count() FROM fastmart_demo.orders_silver")
if [ "$SILVER_COUNT" -gt 0 ]; then
    test_pass "Silver (orders_silver): $SILVER_COUNT orders"
else
    test_fail "Silver (orders_silver) is empty"
fi

# Validate Silver layer - orders_enriched
ENRICHED_COUNT=$(query "SELECT count() FROM fastmart_demo.orders_enriched")
if [ "$ENRICHED_COUNT" -gt 0 ]; then
    test_pass "Silver (orders_enriched): $ENRICHED_COUNT enriched orders"
else
    test_fail "Silver (orders_enriched) is empty"
fi

# Validate enrichment quality
ENRICHMENT_RATE=$(query "SELECT round(countIf(product_name != '') * 100.0 / count(), 1) FROM fastmart_demo.orders_enriched")
if [ "$(echo "$ENRICHMENT_RATE > 90" | bc)" -eq 1 ]; then
    test_pass "Enrichment rate: $ENRICHMENT_RATE%"
else
    test_fail "Enrichment rate too low: $ENRICHMENT_RATE%"
fi

# Validate Gold layer - minute aggregates
GOLD_MINUTE=$(query "SELECT count() FROM fastmart_demo.sales_by_minute")
if [ "$GOLD_MINUTE" -gt 0 ]; then
    test_pass "Gold (sales_by_minute): $GOLD_MINUTE aggregate rows"
else
    test_fail "Gold (sales_by_minute) is empty"
fi

# Validate Gold layer - hourly aggregates
GOLD_HOUR=$(query "SELECT count() FROM fastmart_demo.sales_by_hour")
if [ "$GOLD_HOUR" -gt 0 ]; then
    test_pass "Gold (sales_by_hour): $GOLD_HOUR aggregate rows"
else
    test_fail "Gold (sales_by_hour) is empty"
fi

# Validate cascading aggregation consistency
echo -e "${YELLOW}Step 5.7: Validate aggregation consistency${NC}"
MINUTE_ORDERS=$(query "SELECT countMerge(total_orders) FROM fastmart_demo.sales_by_minute")
HOUR_ORDERS=$(query "SELECT countMerge(total_orders) FROM fastmart_demo.sales_by_hour")
if [ "$MINUTE_ORDERS" -eq "$HOUR_ORDERS" ]; then
    test_pass "Cascading consistency: minute ($MINUTE_ORDERS) = hour ($HOUR_ORDERS)"
else
    test_fail "Cascading mismatch: minute ($MINUTE_ORDERS) != hour ($HOUR_ORDERS)"
fi

# Run validation queries
echo -e "${YELLOW}Step 5.8: Running validation queries${NC}"
run_sql "05_medallion_architecture/05_queries/40_validation.sql" || true

# Count total MVs
MV_COUNT=$(query "SELECT count() FROM system.tables WHERE database = 'fastmart_demo' AND engine LIKE '%MaterializedView%'")
if [ "$MV_COUNT" -ge 5 ]; then
    test_pass "Total Materialized Views: $MV_COUNT"
else
    test_fail "Expected at least 5 MVs, found $MV_COUNT"
fi

# Performance comparison
echo -e "${YELLOW}Step 5.9: Running performance comparison...${NC}"
run_sql "05_medallion_architecture/05_queries/43_performance.sql" || true

# Cleanup
run_sql "05_medallion_architecture/06_cleanup/99_cleanup.sql"

# ==========================================
# FINAL CLEANUP
# ==========================================
section "Final Cleanup"
run_sql "MASTER_CLEANUP.sql" || true

# ==========================================
# SUMMARY
# ==========================================
section "TEST SUMMARY"

TOTAL=$((TESTS_PASSED + TESTS_FAILED))
echo ""
echo -e "${GREEN}Passed: $TESTS_PASSED${NC}"
echo -e "${RED}Failed: $TESTS_FAILED${NC}"
echo "Total:  $TOTAL"
echo ""
echo "=========================================="

if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
fi

echo -e "${GREEN}All tests passed!${NC}"
echo ""
echo "Examples demonstrated:"
echo "  1. Basic Incremental MVs - MV as INSERT trigger"
echo "  2. SummingMergeTree - Auto-sum on merge"
echo "  3. AggregatingMergeTree - State/Merge pattern"
echo "  4. Dictionaries - O(1) lookups with dictGet()"
echo "  5. Medallion Architecture - Bronze -> Silver -> Gold"
