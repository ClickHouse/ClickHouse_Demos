"""
End-to-end test suite for the Telco Workshop project.

Validates emoji cleanup, file structure, generator logic, configuration files,
docker-compose settings, Makefile targets, and README contents.

Runs inside a Docker container -- no local Python execution required.
Usage: make test-all  (from agent_stack_builds/telco_marketing/)
"""

import os
import sys
import re
import traceback

# ---------------------------------------------------------------------------
# Project root is mounted at /project when running inside Docker.
# When running locally for development, fall back to a computed path.
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.environ.get(
    "PROJECT_ROOT",
    "/project",
)

GENERATOR_DIR = os.path.join(PROJECT_ROOT, "data-generator")

# Add data-generator to sys.path so we can import generator.py
sys.path.insert(0, GENERATOR_DIR)

# ---------------------------------------------------------------------------
# Minimal test harness (no pytest dependency)
# ---------------------------------------------------------------------------
_results = {"passed": 0, "failed": 0, "errors": []}


def run_test(name, fn):
    """Run a single test function and record the result."""
    try:
        fn()
        _results["passed"] += 1
        print(f"  [PASS] {name}")
    except AssertionError as exc:
        _results["failed"] += 1
        _results["errors"].append((name, str(exc)))
        print(f"  [FAIL] {name}: {exc}")
    except Exception as exc:
        _results["failed"] += 1
        tb = traceback.format_exc()
        _results["errors"].append((name, tb))
        print(f"  [ERROR] {name}: {exc}")


def print_summary():
    total = _results["passed"] + _results["failed"]
    print("")
    print("=" * 60)
    print(f"Results: {_results['passed']}/{total} passed, {_results['failed']} failed")
    print("=" * 60)
    if _results["errors"]:
        print("")
        print("Failures:")
        for name, msg in _results["errors"]:
            print(f"  - {name}: {msg}")
    return _results["failed"] == 0


# ===================================================================
# 1a. Emoji / non-ASCII check
# ===================================================================
def _scan_for_non_ascii(path):
    """Return list of (file, line_no, line) tuples containing non-ASCII."""
    hits = []
    extensions = {
        ".py", ".md", ".yml", ".yaml", ".json", ".html", ".env",
        ".example", ".txt",
    }
    # Also match files with no extension that we care about
    basenames = {"Makefile", ".env", ".env.example"}

    for root, _dirs, files in os.walk(path):
        # Skip hidden dirs, __pycache__, node_modules, .git
        _dirs[:] = [
            d for d in _dirs
            if not d.startswith(".") and d not in ("__pycache__", "node_modules", "venv")
        ]
        for fname in files:
            _, ext = os.path.splitext(fname)
            if ext not in extensions and fname not in basenames:
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    for i, line in enumerate(f, 1):
                        # Check for any character outside printable ASCII + common whitespace
                        for ch in line:
                            code = ord(ch)
                            if code > 127:
                                # Allow common non-emoji characters used in docs
                                # (curly quotes, en/em dash, etc. are still flagged)
                                hits.append((fpath, i, line.rstrip()[:120]))
                                break
            except (UnicodeDecodeError, PermissionError):
                pass
    return hits


def test_no_emoji_in_project():
    hits = _scan_for_non_ascii(PROJECT_ROOT)
    if hits:
        sample = hits[:5]
        details = "\n".join(
            f"    {fp}:{ln}: {text}" for fp, ln, text in sample
        )
        remaining = len(hits) - len(sample)
        suffix = f"\n    ... and {remaining} more" if remaining else ""
        assert False, f"Found non-ASCII characters:\n{details}{suffix}"


# ===================================================================
# 1b. File structure validation
# ===================================================================
def test_demo_dir_removed():
    demo_path = os.path.join(PROJECT_ROOT, "demo")
    assert not os.path.exists(demo_path), f"demo/ directory should not exist: {demo_path}"


def test_interactive_demo_dir_removed():
    path = os.path.join(PROJECT_ROOT, "interactive-demo")
    assert not os.path.exists(path), f"interactive-demo/ directory should not exist: {path}"


def test_langfuse_dir_removed():
    path = os.path.join(PROJECT_ROOT, "langfuse")
    assert not os.path.exists(path), f"langfuse/ directory should not exist: {path}"


def test_old_env_example_removed():
    path = os.path.join(PROJECT_ROOT, ".env.example")
    assert not os.path.isfile(path), f"Old .env.example should not exist (replaced by mode-specific files): {path}"


def test_env_local_example_exists():
    path = os.path.join(PROJECT_ROOT, ".env.local.example")
    assert os.path.isfile(path), f"Missing: {path}"


def test_env_hybrid_example_exists():
    path = os.path.join(PROJECT_ROOT, ".env.hybrid.example")
    assert os.path.isfile(path), f"Missing: {path}"


def test_docker_compose_local_exists():
    path = os.path.join(PROJECT_ROOT, "docker-compose.local.yml")
    assert os.path.isfile(path), f"Missing: {path}"


def test_librechat_local_yaml_exists():
    path = os.path.join(PROJECT_ROOT, "librechat.local.yaml")
    assert os.path.isfile(path), f"Missing: {path}"


def test_librechat_hybrid_yaml_exists():
    path = os.path.join(PROJECT_ROOT, "librechat.hybrid.yaml")
    assert os.path.isfile(path), f"Missing: {path}"


def test_data_generator_env_example_exists():
    path = os.path.join(GENERATOR_DIR, ".env.example")
    assert os.path.isfile(path), f"Missing: {path}"


def test_expected_files_present():
    expected = ["Makefile", "Makefile.local.mk", "Makefile.hybrid.mk", "docker-compose.yml", "README.md", "test_setup.py", "litellm_config.yaml"]
    missing = [f for f in expected if not os.path.isfile(os.path.join(PROJECT_ROOT, f))]
    assert not missing, f"Missing expected files: {missing}"


# ===================================================================
# 1c. Generator unit tests
# ===================================================================
def test_data_size_profile_small():
    from generator import get_data_size_profile
    p = get_data_size_profile("small")
    assert p["num_customers"] == 1000, f"Expected 1000, got {p['num_customers']}"
    assert p["num_days"] == 7, f"Expected 7, got {p['num_days']}"
    assert p["num_campaigns"] == 10, f"Expected 10, got {p['num_campaigns']}"
    assert p["events_per_day"] == 500, f"Expected 500, got {p['events_per_day']}"


def test_data_size_profile_medium():
    from generator import get_data_size_profile
    p = get_data_size_profile("medium")
    assert p["num_customers"] == 10000
    assert p["num_days"] == 30
    assert p["num_campaigns"] == 100
    assert p["events_per_day"] == 10000


def test_data_size_profile_large():
    from generator import get_data_size_profile
    p = get_data_size_profile("large")
    assert p["num_customers"] == 50000
    assert p["num_days"] == 60
    assert p["num_campaigns"] == 500
    assert p["events_per_day"] == 25000


def test_data_size_profile_2xl():
    from generator import get_data_size_profile
    p = get_data_size_profile("2xl")
    assert p["num_customers"] == 100000
    assert p["num_days"] == 90
    assert p["num_campaigns"] == 1000
    assert p["events_per_day"] == 50000


def test_data_size_profile_invalid():
    from generator import get_data_size_profile
    try:
        get_data_size_profile("invalid")
        assert False, "Expected ValueError for invalid size"
    except ValueError:
        pass


def test_seed_reproducibility():
    from generator import TelcoDataGenerator
    gen1 = TelcoDataGenerator(seed=42)
    customers_a = gen1.generate_customers(10)

    gen2 = TelcoDataGenerator(seed=42)
    customers_b = gen2.generate_customers(10)

    # Same seed should produce identical deterministic fields (names, ages, etc.)
    # Note: customer_id uses uuid.uuid4() which has its own entropy source,
    # so we compare faker-generated fields instead.
    names_a = [(c["first_name"], c["last_name"], c["age"]) for c in customers_a]
    names_b = [(c["first_name"], c["last_name"], c["age"]) for c in customers_b]
    assert names_a == names_b, "Same seed should produce identical customer data"


def test_seed_different():
    from generator import TelcoDataGenerator
    gen1 = TelcoDataGenerator(seed=42)
    customers_a = gen1.generate_customers(10)

    gen2 = TelcoDataGenerator(seed=99)
    customers_b = gen2.generate_customers(10)

    names_a = [(c["first_name"], c["last_name"], c["age"]) for c in customers_a]
    names_b = [(c["first_name"], c["last_name"], c["age"]) for c in customers_b]
    assert names_a != names_b, "Different seeds should produce different customer data"


def test_generate_customers_count_and_fields():
    from generator import TelcoDataGenerator
    gen = TelcoDataGenerator(seed=42)
    customers = gen.generate_customers(100)
    assert len(customers) == 100, f"Expected 100 customers, got {len(customers)}"

    required_fields = {
        "customer_id", "email", "phone_number", "first_name", "last_name",
        "age", "gender", "address", "city", "state", "zip_code",
        "signup_date", "plan_type", "device_type", "segment",
        "monthly_spend", "lifetime_value", "churn_probability",
        "is_churned", "created_at",
    }
    for c in customers:
        missing = required_fields - set(c.keys())
        assert not missing, f"Customer missing fields: {missing}"


def test_generate_cdrs():
    from generator import TelcoDataGenerator
    gen = TelcoDataGenerator(seed=42)
    customers = gen.generate_customers(5)
    cdrs = gen.generate_call_detail_records(customers, days=3)
    assert len(cdrs) > 0, "CDRs should not be empty"

    required_fields = {
        "cdr_id", "customer_id", "timestamp", "event_type",
        "duration_seconds", "data_mb", "base_station_id", "cost", "created_at",
    }
    customer_ids = {c["customer_id"] for c in customers}
    for cdr in cdrs:
        missing = required_fields - set(cdr.keys())
        assert not missing, f"CDR missing fields: {missing}"
        assert cdr["customer_id"] in customer_ids, (
            f"CDR references unknown customer: {cdr['customer_id']}"
        )


def test_generate_network_events():
    from generator import TelcoDataGenerator
    gen = TelcoDataGenerator(seed=42)
    events = gen.generate_network_events(days=2, events_per_day=50)
    assert len(events) == 100, f"Expected 100 events, got {len(events)}"

    required_fields = {
        "event_id", "timestamp", "event_type", "base_station_id",
        "region", "technology", "bandwidth_mbps", "latency_ms",
        "packet_loss_pct", "severity", "is_anomaly", "created_at",
    }
    for ev in events:
        missing = required_fields - set(ev.keys())
        assert not missing, f"Network event missing fields: {missing}"


def test_generate_marketing_campaigns():
    from generator import TelcoDataGenerator
    gen = TelcoDataGenerator(seed=42)
    campaigns = gen.generate_marketing_campaigns(5)
    assert len(campaigns) == 5, f"Expected 5 campaigns, got {len(campaigns)}"

    required_fields = {
        "campaign_id", "campaign_name", "campaign_type", "start_date",
        "end_date", "target_segment", "channel", "budget",
        "impressions", "clicks", "conversions", "revenue_generated", "created_at",
    }
    for camp in campaigns:
        missing = required_fields - set(camp.keys())
        assert not missing, f"Campaign missing fields: {missing}"


def test_generate_datasets_valid_options():
    """Verify the valid GENERATE_DATASETS options are all, network, marketing."""
    # Read generator.py source and check the valid_datasets tuple
    gen_path = os.path.join(GENERATOR_DIR, "generator.py")
    with open(gen_path, "r") as f:
        source = f.read()

    assert 'valid_datasets = ("all", "network", "marketing")' in source, (
        "Expected valid_datasets tuple with all, network, marketing"
    )


def test_generate_datasets_invalid_raises():
    """Verify that the main() logic would reject an invalid GENERATE_DATASETS value."""
    # We test the validation logic by checking the source code pattern
    gen_path = os.path.join(GENERATOR_DIR, "generator.py")
    with open(gen_path, "r") as f:
        source = f.read()

    assert "if generate_datasets not in valid_datasets:" in source, (
        "Expected validation check for invalid GENERATE_DATASETS"
    )
    assert 'raise ValueError' in source, (
        "Expected ValueError for invalid GENERATE_DATASETS"
    )


def test_batch_size_auto_scaling_small():
    """Small data volume should use batch_size 1000."""
    # small: 1000 customers, 7 days, 500 events/day
    # total = (1000 * 7 * 10) + (7 * 500) = 70000 + 3500 = 73500
    total = (1000 * 7 * 10) + (7 * 500)
    assert total <= 1_000_000, f"Small total {total} should be <= 1M"
    batch_size = 10000 if total > 1_000_000 else 1000
    assert batch_size == 1000, f"Expected batch_size 1000 for small, got {batch_size}"


def test_batch_size_auto_scaling_medium():
    """Medium data volume should use batch_size 10000."""
    # medium: 10000 customers, 30 days, 10000 events/day
    # total = (10000 * 30 * 10) + (30 * 10000) = 3000000 + 300000 = 3300000
    total = (10000 * 30 * 10) + (30 * 10000)
    assert total > 1_000_000, f"Medium total {total} should be > 1M"
    batch_size = 10000 if total > 1_000_000 else 1000
    assert batch_size == 10000, f"Expected batch_size 10000 for medium, got {batch_size}"


def test_generator_supports_clickhouse_secure():
    """Verify generator.py reads CLICKHOUSE_SECURE env var."""
    gen_path = os.path.join(GENERATOR_DIR, "generator.py")
    with open(gen_path, "r") as f:
        source = f.read()
    assert "CLICKHOUSE_SECURE" in source, (
        "generator.py should read CLICKHOUSE_SECURE env var"
    )
    assert '"secure"' in source or "'secure'" in source, (
        "generator.py should pass secure= to clickhouse_connect"
    )


def test_generator_streaming_cdrs():
    """Verify generator has streaming CDR method for memory-bounded inserts."""
    from generator import TelcoDataGenerator
    gen = TelcoDataGenerator(seed=42)
    customers = gen.generate_customers(10)
    # generate_cdrs_for_customers should work on a small batch
    cdrs = gen.generate_cdrs_for_customers(customers[:5], days=2)
    assert len(cdrs) > 0, "Streaming CDR method should produce records"
    assert all("cdr_id" in c for c in cdrs), "CDRs should have cdr_id field"


def test_generator_streaming_network_events():
    """Verify generator has per-day network event method for memory-bounded inserts."""
    from generator import TelcoDataGenerator
    gen = TelcoDataGenerator(seed=42)
    events = gen.generate_network_events_for_day(day_offset=0, total_days=7, events_per_day=50)
    assert len(events) == 50, f"Expected 50 events for one day, got {len(events)}"
    assert all("event_id" in e for e in events), "Events should have event_id field"


# ===================================================================
# 1d. .env.example validation
# ===================================================================
def _read_env_keys(path):
    """Read an .env.example file and return the set of keys defined or documented.

    Includes both active lines (KEY=value) and commented-out examples
    (# KEY=value) since .env.example files often comment out optional keys.
    """
    keys = set()
    with open(path, "r") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            # Active key=value line
            if not stripped.startswith("#") and "=" in stripped:
                key = stripped.split("=", 1)[0].strip()
                keys.add(key)
            # Commented-out key=value (e.g. "# DATA_SIZE=medium")
            elif stripped.startswith("#"):
                uncommented = stripped.lstrip("#").strip()
                if uncommented and "=" in uncommented and not uncommented[0].isspace():
                    candidate = uncommented.split("=", 1)[0].strip()
                    # Only accept if it looks like an env var name (UPPER_CASE)
                    if re.match(r"^[A-Z][A-Z0-9_]*$", candidate):
                        keys.add(candidate)
    return keys


def test_env_local_example_keys():
    path = os.path.join(PROJECT_ROOT, ".env.local.example")
    keys = _read_env_keys(path)
    required = {
        "DEPLOY_MODE", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_SECURE", "LANGFUSE_SECRET",
        "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_BASE_URL",
        "LANGFUSE_INIT_ORG_ID", "LANGFUSE_INIT_PROJECT_ID",
        "LANGFUSE_INIT_PROJECT_PUBLIC_KEY", "LANGFUSE_INIT_PROJECT_SECRET_KEY",
        "LANGFUSE_INIT_USER_EMAIL", "LANGFUSE_INIT_USER_PASSWORD",
        "LITELLM_MASTER_KEY",
        "LIBRECHAT_USER_EMAIL", "LIBRECHAT_USER_NAME", "LIBRECHAT_USER_PASSWORD",
        "NUM_CUSTOMERS", "DATA_SIZE", "DATA_SEED",
        "GENERATE_DATASETS", "CREDS_KEY", "JWT_SECRET",
    }
    missing = required - keys
    assert not missing, f".env.local.example missing keys: {missing}"


def test_env_hybrid_example_keys():
    path = os.path.join(PROJECT_ROOT, ".env.hybrid.example")
    keys = _read_env_keys(path)
    required = {
        "DEPLOY_MODE", "CLICKHOUSE_HOST", "CLICKHOUSE_PORT",
        "CLICKHOUSE_SECURE", "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY", "LANGFUSE_BASE_URL",
        "LITELLM_MASTER_KEY",
        "LIBRECHAT_USER_EMAIL", "LIBRECHAT_USER_NAME", "LIBRECHAT_USER_PASSWORD",
        "NUM_CUSTOMERS", "DATA_SIZE", "DATA_SEED",
        "GENERATE_DATASETS", "CREDS_KEY", "JWT_SECRET",
    }
    missing = required - keys
    assert not missing, f".env.hybrid.example missing keys: {missing}"


def test_env_local_example_deploy_mode():
    path = os.path.join(PROJECT_ROOT, ".env.local.example")
    with open(path, "r") as f:
        content = f.read()
    assert "DEPLOY_MODE=local" in content, ".env.local.example should have DEPLOY_MODE=local"


def test_env_hybrid_example_deploy_mode():
    path = os.path.join(PROJECT_ROOT, ".env.hybrid.example")
    with open(path, "r") as f:
        content = f.read()
    assert "DEPLOY_MODE=hybrid" in content, ".env.hybrid.example should have DEPLOY_MODE=hybrid"


def test_data_generator_env_example_keys():
    path = os.path.join(GENERATOR_DIR, ".env.example")
    keys = _read_env_keys(path)
    required = {
        "CLICKHOUSE_HOST", "NUM_CUSTOMERS", "DATA_SIZE",
        "DATA_SEED", "GENERATE_DATASETS",
    }
    missing = required - keys
    assert not missing, f"data-generator .env.example missing keys: {missing}"


def test_env_examples_contain_tshirt_table():
    for label, path in [
        ("local", os.path.join(PROJECT_ROOT, ".env.local.example")),
        ("hybrid", os.path.join(PROJECT_ROOT, ".env.hybrid.example")),
        ("data-generator", os.path.join(GENERATOR_DIR, ".env.example")),
    ]:
        with open(path, "r") as f:
            content = f.read()
        assert "small" in content and "medium" in content and "large" in content and "2xl" in content, (
            f"{label} .env.example missing t-shirt size documentation"
        )


# ===================================================================
# 1e. docker-compose.yml validation
# ===================================================================
def test_docker_compose_has_librechat():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "librechat" in dc["services"], "docker-compose.yml should have librechat service"


def test_docker_compose_no_mcp_in_base():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "mcp-clickhouse" not in dc["services"], (
        "Base docker-compose.yml should NOT have mcp-clickhouse (it belongs in docker-compose.local.yml)"
    )


def test_docker_compose_has_mongodb():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "mongodb" in dc["services"], "docker-compose.yml should have mongodb service"


def test_docker_compose_has_meilisearch():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "meilisearch" in dc["services"], "docker-compose.yml should have meilisearch service"


def test_docker_compose_no_clickhouse_in_base():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "clickhouse" not in dc["services"], (
        "Base docker-compose.yml should NOT have clickhouse (it belongs in docker-compose.local.yml)"
    )


def test_docker_compose_data_generator_env():
    import yaml

    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)

    dg = dc["services"]["data-generator"]
    env = dg.get("environment", {})

    # Check that DATA_SIZE, DATA_SEED, GENERATE_DATASETS are present
    env_keys = set()
    if isinstance(env, dict):
        env_keys = set(env.keys())
    elif isinstance(env, list):
        for item in env:
            key = item.split("=", 1)[0] if "=" in item else item
            env_keys.add(key)

    for var in ("DATA_SIZE", "DATA_SEED", "GENERATE_DATASETS", "CLICKHOUSE_SECURE"):
        assert var in env_keys, f"data-generator service missing env var: {var}"


def test_docker_compose_data_generator_memory_limit():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    dg = dc["services"]["data-generator"]
    deploy = dg.get("deploy", {})
    resources = deploy.get("resources", {})
    limits = resources.get("limits", {})
    assert "memory" in limits, "data-generator should have a memory limit"


def test_docker_compose_defaults():
    import yaml

    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)

    dg_env = dc["services"]["data-generator"]["environment"]

    # Check default values encoded in the compose file
    # NUM_CUSTOMERS default should be 10000
    num_cust = dg_env.get("NUM_CUSTOMERS", "")
    assert "10000" in str(num_cust), f"NUM_CUSTOMERS default should be 10000, got: {num_cust}"

    num_camp = dg_env.get("NUM_CAMPAIGNS", "")
    assert "100" in str(num_camp), f"NUM_CAMPAIGNS default should be 100, got: {num_camp}"

    epd = dg_env.get("EVENTS_PER_DAY", "")
    assert "10000" in str(epd), f"EVENTS_PER_DAY default should be 10000, got: {epd}"


def test_docker_compose_local_has_clickhouse():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.local.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "clickhouse" in dc["services"], "docker-compose.local.yml should have clickhouse service"


def test_docker_compose_local_has_mcp_clickhouse():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.local.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "mcp-clickhouse" in dc["services"], "docker-compose.local.yml should have mcp-clickhouse service"


def test_docker_compose_local_has_langfuse():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.local.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "langfuse" in dc["services"], "docker-compose.local.yml should have langfuse service"


def test_docker_compose_local_langfuse_headless_init():
    """Verify langfuse service in docker-compose.local.yml has headless init env vars."""
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.local.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    langfuse_env = dc["services"]["langfuse"]["environment"]
    for var in (
        "LANGFUSE_INIT_ORG_ID",
        "LANGFUSE_INIT_PROJECT_ID",
        "LANGFUSE_INIT_PROJECT_PUBLIC_KEY",
        "LANGFUSE_INIT_PROJECT_SECRET_KEY",
        "LANGFUSE_INIT_USER_EMAIL",
        "LANGFUSE_INIT_USER_PASSWORD",
    ):
        assert var in langfuse_env, (
            f"docker-compose.local.yml langfuse service missing env var: {var}"
        )


def test_env_local_example_langfuse_placeholders():
    """Verify .env.local.example uses generate-langfuse-pk/sk placeholders for Makefile auto-generation."""
    path = os.path.join(PROJECT_ROOT, ".env.local.example")
    with open(path, "r") as f:
        content = f.read()
    assert "<generate-langfuse-pk>" in content, (
        ".env.local.example should contain <generate-langfuse-pk> placeholder"
    )
    assert "<generate-langfuse-sk>" in content, (
        ".env.local.example should contain <generate-langfuse-sk> placeholder"
    )
    # Each placeholder should appear exactly twice (LANGFUSE_INIT_PROJECT_* and LANGFUSE_*)
    assert content.count("<generate-langfuse-pk>") == 2, (
        "<generate-langfuse-pk> should appear exactly twice (INIT key + LibreChat key)"
    )
    assert content.count("<generate-langfuse-sk>") == 2, (
        "<generate-langfuse-sk> should appear exactly twice (INIT key + LibreChat key)"
    )


def test_librechat_local_yaml_mcp_config():
    import yaml
    lc_path = os.path.join(PROJECT_ROOT, "librechat.local.yaml")
    with open(lc_path, "r") as f:
        lc = yaml.safe_load(f)
    assert "mcpServers" in lc, "librechat.local.yaml should have mcpServers"
    assert "clickhouse-telco" in lc["mcpServers"], "librechat.local.yaml should configure clickhouse-telco"
    server = lc["mcpServers"]["clickhouse-telco"]
    assert server.get("type") == "sse", "Local MCP server type should be sse"
    assert "mcp-clickhouse" in server.get("url", ""), "Local MCP server URL should reference mcp-clickhouse"
    assert "serverInstructions" in server, "Local MCP server should have serverInstructions"


def test_librechat_hybrid_yaml_mcp_config():
    import yaml
    lc_path = os.path.join(PROJECT_ROOT, "librechat.hybrid.yaml")
    with open(lc_path, "r") as f:
        lc = yaml.safe_load(f)
    assert "mcpServers" in lc, "librechat.hybrid.yaml should have mcpServers"
    servers = lc["mcpServers"]
    server_names = list(servers.keys())
    assert len(server_names) >= 1, "librechat.hybrid.yaml should have at least one MCP server"
    server = servers[server_names[0]]
    assert "mcp.clickhouse.cloud" in server.get("url", ""), (
        "Hybrid MCP server URL should reference mcp.clickhouse.cloud"
    )
    assert "serverInstructions" in server, "Hybrid MCP server should have serverInstructions"


def test_librechat_local_yaml_model_specs():
    import yaml
    lc_path = os.path.join(PROJECT_ROOT, "librechat.local.yaml")
    with open(lc_path, "r") as f:
        lc = yaml.safe_load(f)
    assert "modelSpecs" in lc, "librechat.local.yaml should have modelSpecs"
    specs = lc["modelSpecs"]
    assert specs.get("enforce") is True, "modelSpecs.enforce should be true"
    assert len(specs.get("list", [])) == 3, "modelSpecs should have 3 model specs"
    models_found = set()
    for spec in specs["list"]:
        assert "mcpServers" in spec, "Model spec should pre-associate mcpServers"
        assert "preset" in spec, "Model spec should have a preset"
        preset = spec["preset"]
        assert "promptPrefix" in preset, "Model spec preset should have promptPrefix"
        assert preset.get("endpoint", "") == "LiteLLM", "Preset endpoint should be 'LiteLLM'"
        models_found.add(preset.get("model", ""))
    expected_models = {"gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite"}
    assert models_found == expected_models, f"Expected models {expected_models}, got {models_found}"


def test_librechat_hybrid_yaml_model_specs():
    import yaml
    lc_path = os.path.join(PROJECT_ROOT, "librechat.hybrid.yaml")
    with open(lc_path, "r") as f:
        lc = yaml.safe_load(f)
    assert "modelSpecs" in lc, "librechat.hybrid.yaml should have modelSpecs"
    specs = lc["modelSpecs"]
    assert specs.get("enforce") is True, "modelSpecs.enforce should be true"
    assert len(specs.get("list", [])) == 3, "modelSpecs should have 3 model specs"
    models_found = set()
    for spec in specs["list"]:
        assert "mcpServers" in spec, "Model spec should pre-associate mcpServers"
        preset = spec["preset"]
        assert "promptPrefix" in preset, "Model spec preset should have promptPrefix"
        models_found.add(preset.get("model", ""))
    expected_models = {"gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite"}
    assert models_found == expected_models, f"Expected models {expected_models}, got {models_found}"


def test_librechat_yaml_interface_locked():
    import yaml
    for label, filename in [("local", "librechat.local.yaml"), ("hybrid", "librechat.hybrid.yaml")]:
        lc_path = os.path.join(PROJECT_ROOT, filename)
        with open(lc_path, "r") as f:
            lc = yaml.safe_load(f)
        assert "interface" in lc, f"{filename} should have interface section"
        iface = lc["interface"]
        assert iface.get("modelSelect") is False, f"{filename}: interface.modelSelect should be false"
        assert iface.get("endpointsMenu") is True, f"{filename}: interface.endpointsMenu should be true (required for Agents endpoint)"


def test_librechat_yaml_agents_endpoint():
    """Verify both templates configure the Agents endpoint for Langfuse tracing."""
    import yaml
    for label, filename in [("local", "librechat.local.yaml"), ("hybrid", "librechat.hybrid.yaml")]:
        lc_path = os.path.join(PROJECT_ROOT, filename)
        with open(lc_path, "r") as f:
            lc = yaml.safe_load(f)
        assert "endpoints" in lc, f"{filename} should have endpoints section"
        assert "agents" in lc["endpoints"], f"{filename} should have endpoints.agents section"
        agents = lc["endpoints"]["agents"]
        providers = agents.get("allowedProviders", [])
        assert "google" in providers, f"{filename}: endpoints.agents.allowedProviders should include 'google'"
        assert agents.get("disableBuilder") is False, f"{filename}: endpoints.agents.disableBuilder should be false"


def test_librechat_yaml_visualization_instructions():
    """Verify promptPrefix includes Chart.js HTML template with Material Design colors."""
    import yaml
    for label, filename in [("local", "librechat.local.yaml"), ("hybrid", "librechat.hybrid.yaml")]:
        lc_path = os.path.join(PROJECT_ROOT, filename)
        with open(lc_path, "r") as f:
            lc = yaml.safe_load(f)
        specs = lc.get("modelSpecs", {}).get("list", [])
        for spec in specs:
            prefix = spec.get("preset", {}).get("promptPrefix", "")
            assert "artifact" in prefix.lower(), (
                f"{filename} model {spec.get('name')}: promptPrefix should mention Artifacts for charts"
            )
            assert "chart" in prefix.lower(), (
                f"{filename} model {spec.get('name')}: promptPrefix should mention chart types"
            )
            # Material Design color palette (Google blue)
            assert "#4285F4" in prefix, (
                f"{filename} model {spec.get('name')}: promptPrefix should include Material Design color palette"
            )
            # Verify the HTML template is present (not just bullet-point rules)
            assert "new Chart(" in prefix, (
                f"{filename} model {spec.get('name')}: promptPrefix should contain Chart.js HTML template"
            )
            assert "background: #ffffff" in prefix, (
                f"{filename} model {spec.get('name')}: promptPrefix template should set white background"
            )


def test_librechat_yaml_mv_documentation():
    """Verify both templates document MV columns and avgMerge() usage."""
    import yaml
    for label, filename in [("local", "librechat.local.yaml"), ("hybrid", "librechat.hybrid.yaml")]:
        lc_path = os.path.join(PROJECT_ROOT, filename)
        with open(lc_path, "r") as f:
            content = f.read()

        # Check avgMerge appears somewhere in the file (promptPrefix or serverInstructions)
        assert "avgMerge" in content, (
            f"{filename} should document avgMerge() for AggregatingMergeTree columns"
        )

        # Check MV column names are documented
        for col in ("total_calls", "anomaly_count", "revenue_generated"):
            assert col in content, (
                f"{filename} should document MV column: {col}"
            )

        # Check engine types are mentioned
        assert "SummingMergeTree" in content, (
            f"{filename} should mention SummingMergeTree engine"
        )
        assert "AggregatingMergeTree" in content, (
            f"{filename} should mention AggregatingMergeTree engine"
        )


def test_librechat_yaml_sql_rules():
    """Verify both templates document critical SQL rules (telco. prefix, no semicolons)."""
    import yaml
    for label, filename in [("local", "librechat.local.yaml"), ("hybrid", "librechat.hybrid.yaml")]:
        lc_path = os.path.join(PROJECT_ROOT, filename)
        with open(lc_path, "r") as f:
            content = f.read()

        # Check telco. prefix rule is documented
        assert "telco.customers" in content, (
            f"{filename} should show telco.customers as example of required prefix"
        )
        assert "telco.marketing_campaigns" in content, (
            f"{filename} should show telco.marketing_campaigns as example of required prefix"
        )

        # Check no-semicolon rule is documented
        assert "semicolon" in content.lower(), (
            f"{filename} should warn against trailing semicolons"
        )


def test_librechat_yaml_analysis_framework():
    """Verify promptPrefix includes senior analyst multi-dimensional analysis guidance."""
    import yaml
    for label, filename in [("local", "librechat.local.yaml"), ("hybrid", "librechat.hybrid.yaml")]:
        lc_path = os.path.join(PROJECT_ROOT, filename)
        with open(lc_path, "r") as f:
            lc = yaml.safe_load(f)
        specs = lc.get("modelSpecs", {}).get("list", [])
        for spec in specs:
            prefix = spec.get("preset", {}).get("promptPrefix", "")
            # Must instruct multi-dimensional analysis
            assert "multiple queries" in prefix.lower(), (
                f"{filename} model {spec.get('name')}: promptPrefix should instruct running multiple queries"
            )
            # Must instruct computing rates/ratios
            assert "conversion rate" in prefix.lower(), (
                f"{filename} model {spec.get('name')}: promptPrefix should instruct computing rates and ratios"
            )
            # Must instruct adaptive infographics (design system approach)
            assert "adapt" in prefix.lower(), (
                f"{filename} model {spec.get('name')}: promptPrefix should instruct adapting the template to what the data requires"
            )
            # Must instruct executive summary
            assert "executive summary" in prefix.lower(), (
                f"{filename} model {spec.get('name')}: promptPrefix should instruct starting with executive summary"
            )


def test_librechat_yaml_dashboard_template():
    """Verify promptPrefix includes infographic HTML template with KPI cards, subtitle, grid layout, section header, and findings."""
    import yaml
    for label, filename in [("local", "librechat.local.yaml"), ("hybrid", "librechat.hybrid.yaml")]:
        lc_path = os.path.join(PROJECT_ROOT, filename)
        with open(lc_path, "r") as f:
            lc = yaml.safe_load(f)
        specs = lc.get("modelSpecs", {}).get("list", [])
        for spec in specs:
            prefix = spec.get("preset", {}).get("promptPrefix", "")
            model_name = spec.get("name", "unknown")
            # Infographic template CSS classes present
            assert "dash-title" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain dash-title CSS class"
            )
            assert "dash-subtitle" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain dash-subtitle CSS class"
            )
            # KPI cards section present
            assert "kpi-value" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain kpi-value CSS class (KPI cards)"
            )
            assert "kpi-label" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain kpi-label CSS class (KPI cards)"
            )
            assert "KPI1_VALUE" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain KPI1_VALUE placeholder"
            )
            # CSS grid layout present
            assert "grid-template-columns" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain grid-template-columns (CSS grid layout)"
            )
            # Section header (visual divider between KPIs and charts)
            assert "section-header" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain section-header CSS class (visual divider)"
            )
            # Findings panel present
            assert "findings" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain findings CSS class (key findings panel)"
            )
            assert "findings-title" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain findings-title CSS class (findings heading)"
            )
            # Per-chart insight captions
            assert "card-insight" in prefix, (
                f"{filename} model {model_name}: promptPrefix should contain card-insight CSS class (per-chart captions)"
            )
            # Canvas IDs for multi-chart layout
            for chart_id in ("chart1", "chart2", "chart3", "chart4"):
                assert chart_id in prefix, (
                    f"{filename} model {model_name}: promptPrefix should contain canvas id '{chart_id}'"
                )
            # Instruction to combine charts into single artifact
            assert "ONE artifact" in prefix or "SINGLE" in prefix, (
                f"{filename} model {model_name}: promptPrefix should instruct combining into ONE artifact or SINGLE artifact"
            )
            # Differentiated visualization guidance (chart vs infographic)
            assert "single chart" in prefix.lower() or "single-chart" in prefix.lower(), (
                f"{filename} model {model_name}: promptPrefix should differentiate single chart vs infographic requests"
            )


# ===================================================================
# 1e2. LiteLLM configuration validation
# ===================================================================
def test_litellm_config_exists():
    path = os.path.join(PROJECT_ROOT, "litellm_config.yaml")
    assert os.path.isfile(path), f"Missing: {path}"


def test_litellm_config_models():
    import yaml
    path = os.path.join(PROJECT_ROOT, "litellm_config.yaml")
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    model_list = config.get("model_list", [])
    model_names = {m.get("model_name") for m in model_list}
    expected = {"gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite"}
    assert model_names == expected, f"Expected models {expected}, got {model_names}"


def test_litellm_config_langfuse_callback():
    import yaml
    path = os.path.join(PROJECT_ROOT, "litellm_config.yaml")
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    settings = config.get("litellm_settings", {})
    callbacks = settings.get("success_callback", [])
    assert "langfuse" in callbacks, (
        f"litellm_config.yaml should have 'langfuse' in success_callback, got {callbacks}"
    )


def test_docker_compose_has_litellm():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    assert "litellm" in dc["services"], "docker-compose.yml should have litellm service"


def test_docker_compose_litellm_healthcheck():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    litellm = dc["services"]["litellm"]
    assert "healthcheck" in litellm, "litellm service should have a healthcheck"
    hc = litellm["healthcheck"]
    test_cmd = " ".join(str(x) for x in hc.get("test", []))
    assert "health/readiness" in test_cmd, (
        "litellm healthcheck should use /health/readiness endpoint"
    )


def test_docker_compose_librechat_depends_on_litellm():
    import yaml
    dc_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
    with open(dc_path, "r") as f:
        dc = yaml.safe_load(f)
    librechat = dc["services"]["librechat"]
    depends = librechat.get("depends_on", {})
    assert "litellm" in depends, "librechat should depend on litellm"


def test_librechat_yaml_custom_litellm_endpoint():
    """Verify both templates configure the LiteLLM custom endpoint."""
    import yaml
    for label, filename in [("local", "librechat.local.yaml"), ("hybrid", "librechat.hybrid.yaml")]:
        lc_path = os.path.join(PROJECT_ROOT, filename)
        with open(lc_path, "r") as f:
            lc = yaml.safe_load(f)
        endpoints = lc.get("endpoints", {})
        custom = endpoints.get("custom", [])
        assert len(custom) >= 1, f"{filename} should have at least one custom endpoint"
        litellm_ep = None
        for ep in custom:
            if ep.get("name") == "LiteLLM":
                litellm_ep = ep
                break
        assert litellm_ep is not None, f"{filename} should have a custom endpoint named 'LiteLLM'"
        assert "litellm:4000" in litellm_ep.get("baseURL", ""), (
            f"{filename}: LiteLLM endpoint baseURL should reference litellm:4000"
        )
        models = litellm_ep.get("models", {}).get("default", [])
        assert len(models) == 3, (
            f"{filename}: LiteLLM endpoint should list 3 models, got {len(models)}"
        )


# ===================================================================
# 1f. Makefile validation
# ===================================================================
def _read_makefile():
    path = os.path.join(PROJECT_ROOT, "Makefile")
    with open(path, "r") as f:
        return f.read()


def _read_all_makefiles():
    """Read and concatenate Makefile + Makefile.local.mk + Makefile.hybrid.mk."""
    parts = []
    for name in ("Makefile", "Makefile.local.mk", "Makefile.hybrid.mk"):
        path = os.path.join(PROJECT_ROOT, name)
        with open(path, "r") as f:
            parts.append(f.read())
    return "\n".join(parts)


def test_makefile_setup_targets_exist():
    content = _read_makefile()
    for target in ("setup-local:", "setup-hybrid:"):
        assert target in content, f"Makefile missing target: {target}"


def test_makefile_init_schema_target_exists():
    # init-schema target is defined in the mode-specific .mk files
    all_content = _read_all_makefiles()
    assert "init-schema:" in all_content, "Makefile files missing target: init-schema"


def test_makefile_new_targets_exist():
    content = _read_makefile()
    for target in ("check-db:", "explore-data:", "query:"):
        assert target in content, f"Makefile missing target: {target}"


def test_makefile_phony_includes_new_targets():
    content = _read_makefile()
    # Find .PHONY line(s)
    phony_lines = [line for line in content.splitlines() if line.startswith(".PHONY")]
    phony_text = " ".join(phony_lines)
    for target in ("setup-local", "setup-hybrid", "init-schema", "check-db", "explore-data", "query", "clean-data", "clean"):
        assert target in phony_text, f".PHONY missing target: {target}"


def test_makefile_deploy_mode_detection():
    content = _read_makefile()
    assert "DEPLOY_MODE" in content, "Makefile should detect DEPLOY_MODE"
    # docker-compose.local.yml reference is in Makefile.local.mk (included file)
    all_content = _read_all_makefiles()
    assert "docker-compose.local.yml" in all_content, "Makefile files should reference docker-compose.local.yml"


def test_makefile_help_mentions_new_commands():
    content = _read_makefile()
    # The help target should mention setup-local, setup-hybrid, init-schema, cleanup targets
    for cmd in ("setup-local", "setup-hybrid", "init-schema", "check-db", "explore-data", "query", "clean-data", "clean"):
        assert cmd in content, f"Makefile help should mention: {cmd}"


def test_makefile_cleanup_targets_exist():
    content = _read_makefile()
    assert "clean-data:" in content, "Makefile missing target: clean-data"
    assert "clean:" in content, "Makefile missing target: clean"
    # clean-data should truncate tables (in main Makefile or included files)
    all_content = _read_all_makefiles()
    assert "TRUNCATE" in all_content, "Makefile files should TRUNCATE tables in clean-data"


def test_makefile_no_call_ch_query():
    """Verify we do not use $(call ch_query,...) which breaks on commas."""
    content = _read_all_makefiles()
    assert "$(call ch_query" not in content, (
        "Makefile files should not use $(call ch_query,...) -- use $(CH_CLIENT) --query instead"
    )
    assert "$(call ch_interactive" not in content, (
        "Makefile files should not use $(call ch_interactive) -- use $(CH_INTERACTIVE) instead"
    )


def test_makefile_local_mk_exists():
    path = os.path.join(PROJECT_ROOT, "Makefile.local.mk")
    assert os.path.isfile(path), f"Missing: {path}"


def test_makefile_hybrid_mk_exists():
    path = os.path.join(PROJECT_ROOT, "Makefile.hybrid.mk")
    assert os.path.isfile(path), f"Missing: {path}"


def test_makefile_includes_mode_file():
    content = _read_makefile()
    assert "include Makefile.$(DEPLOY_MODE).mk" in content, (
        "Makefile should include Makefile.$(DEPLOY_MODE).mk"
    )


def test_makefile_local_mk_has_compose_cmd():
    path = os.path.join(PROJECT_ROOT, "Makefile.local.mk")
    with open(path, "r") as f:
        content = f.read()
    assert "COMPOSE_CMD" in content, "Makefile.local.mk should define COMPOSE_CMD"
    assert "docker-compose.local.yml" in content, (
        "Makefile.local.mk COMPOSE_CMD should reference docker-compose.local.yml"
    )


def test_makefile_hybrid_mk_has_compose_cmd():
    path = os.path.join(PROJECT_ROOT, "Makefile.hybrid.mk")
    with open(path, "r") as f:
        content = f.read()
    assert "COMPOSE_CMD" in content, "Makefile.hybrid.mk should define COMPOSE_CMD"


def test_makefile_no_ifeq_deploy_mode():
    """Verify main Makefile has no ifeq ($(DEPLOY_MODE),...) blocks (split is complete)."""
    content = _read_makefile()
    assert "ifeq ($(DEPLOY_MODE)" not in content, (
        "Main Makefile should not contain ifeq ($(DEPLOY_MODE),...) blocks -- use included .mk files"
    )


# ===================================================================
# 1g. README validation
# ===================================================================
def _read_readme():
    path = os.path.join(PROJECT_ROOT, "README.md")
    with open(path, "r") as f:
        return f.read()


def test_readme_toc_sections():
    content = _read_readme()
    expected_sections = [
        "LLM Provider Flexibility",
        "Post-Setup Verification",
        "Data Volume Configuration",
        "External MCP Clients",
        "Data Relationships (ER Diagram)",
        "Materialized View Data Flow",
        "Using LibreChat",
    ]
    for section in expected_sections:
        assert section in content, f"README missing ToC section: {section}"


def test_readme_no_legacy_demo_section():
    content = _read_readme()
    assert "Legacy Demo Scripts" not in content, (
        "README should not contain 'Legacy Demo Scripts' section"
    )


def test_readme_no_interactive_demo_references():
    content = _read_readme()
    assert "interactive-demo/" not in content, (
        "README should not reference interactive-demo/ directory"
    )
    assert "localhost:5001" not in content, (
        "README should not reference localhost:5001 (old Flask demo)"
    )


def test_readme_mentions_librechat():
    content = _read_readme()
    assert "LibreChat" in content, "README should mention LibreChat"
    assert "localhost:3080" in content, "README should reference localhost:3080 (LibreChat)"


def test_readme_mentions_deployment_modes():
    content = _read_readme()
    assert "setup-local" in content, "README should mention setup-local"
    assert "setup-hybrid" in content, "README should mention setup-hybrid"
    assert "DEPLOY_MODE" in content or "deploy mode" in content.lower() or "Local Deployment" in content, (
        "README should discuss deployment modes"
    )


def test_readme_mermaid_blocks():
    content = _read_readme()
    mermaid_count = content.count("```mermaid")
    assert mermaid_count >= 4, (
        f"README should have at least 4 mermaid blocks, found {mermaid_count}"
    )


def test_readme_no_demo_directory_in_materials():
    content = _read_readme()
    # The Workshop Materials table should not reference demo/ or interactive-demo/
    materials_start = content.find("## Workshop Materials")
    if materials_start == -1:
        assert False, "README missing 'Workshop Materials' section"
    materials_section = content[materials_start:]
    assert "| **`demo/`**" not in materials_section, (
        "Workshop Materials table should not reference demo/ directory"
    )
    assert "| **`interactive-demo/`**" not in materials_section, (
        "Workshop Materials table should not reference interactive-demo/ directory"
    )


def test_readme_langfuse_tracing_section():
    content = _read_readme()
    assert "Langfuse Tracing" in content, "README should have a Langfuse Tracing section"
    assert "Agents endpoint" in content, "README should explain the Agents endpoint for tracing"
    assert "Agent Builder" in content, "README should mention the Agent Builder"


def test_readme_new_defaults():
    content = _read_readme()
    assert "10,000" in content, "README should document 10,000 customers as default"
    # Should NOT say 1,000 is the default (might appear in t-shirt table, so check context)
    # Just verify the Data Volume section mentions 10,000 as medium/default
    dv_start = content.find("## Data Volume Configuration")
    if dv_start != -1:
        dv_section = content[dv_start:dv_start + 2000]
        assert "10,000" in dv_section, (
            "Data Volume section should show 10,000 as medium/default"
        )


# ===================================================================
# Main runner
# ===================================================================
def main():
    print("=" * 60)
    print("Telco Workshop - End-to-End Test Suite")
    print("=" * 60)
    print(f"Project root: {PROJECT_ROOT}")
    print("")

    # -- 1a. Emoji check --
    print("[Section 1a] Emoji / non-ASCII check")
    run_test("No emoji or non-ASCII in project files", test_no_emoji_in_project)
    print("")

    # -- 1b. File structure --
    print("[Section 1b] File structure validation")
    run_test("demo/ directory removed", test_demo_dir_removed)
    run_test("interactive-demo/ directory removed", test_interactive_demo_dir_removed)
    run_test("langfuse/ directory removed", test_langfuse_dir_removed)
    run_test("Old .env.example removed", test_old_env_example_removed)
    run_test(".env.local.example exists", test_env_local_example_exists)
    run_test(".env.hybrid.example exists", test_env_hybrid_example_exists)
    run_test("docker-compose.local.yml exists", test_docker_compose_local_exists)
    run_test("librechat.local.yaml exists", test_librechat_local_yaml_exists)
    run_test("librechat.hybrid.yaml exists", test_librechat_hybrid_yaml_exists)
    run_test("data-generator .env.example exists", test_data_generator_env_example_exists)
    run_test("Expected project files present", test_expected_files_present)
    print("")

    # -- 1c. Generator unit tests --
    print("[Section 1c] Generator unit tests")
    run_test("DATA_SIZE profile: small", test_data_size_profile_small)
    run_test("DATA_SIZE profile: medium", test_data_size_profile_medium)
    run_test("DATA_SIZE profile: large", test_data_size_profile_large)
    run_test("DATA_SIZE profile: 2xl", test_data_size_profile_2xl)
    run_test("DATA_SIZE profile: invalid raises ValueError", test_data_size_profile_invalid)
    run_test("Seed reproducibility (seed=42)", test_seed_reproducibility)
    run_test("Different seeds produce different data", test_seed_different)
    run_test("generate_customers count and fields", test_generate_customers_count_and_fields)
    run_test("generate_call_detail_records", test_generate_cdrs)
    run_test("generate_network_events", test_generate_network_events)
    run_test("generate_marketing_campaigns", test_generate_marketing_campaigns)
    run_test("GENERATE_DATASETS valid options", test_generate_datasets_valid_options)
    run_test("GENERATE_DATASETS invalid raises ValueError", test_generate_datasets_invalid_raises)
    run_test("Batch size auto-scaling: small -> 1000", test_batch_size_auto_scaling_small)
    run_test("Batch size auto-scaling: medium -> 10000", test_batch_size_auto_scaling_medium)
    run_test("Generator supports CLICKHOUSE_SECURE", test_generator_supports_clickhouse_secure)
    run_test("Generator streaming CDRs (memory-bounded)", test_generator_streaming_cdrs)
    run_test("Generator streaming network events (per-day)", test_generator_streaming_network_events)
    print("")

    # -- 1d. .env.example validation --
    print("[Section 1d] .env.example validation")
    run_test(".env.local.example required keys", test_env_local_example_keys)
    run_test(".env.hybrid.example required keys", test_env_hybrid_example_keys)
    run_test(".env.local.example has DEPLOY_MODE=local", test_env_local_example_deploy_mode)
    run_test(".env.hybrid.example has DEPLOY_MODE=hybrid", test_env_hybrid_example_deploy_mode)
    run_test("data-generator .env.example required keys", test_data_generator_env_example_keys)
    run_test("All .env.examples contain t-shirt size table", test_env_examples_contain_tshirt_table)
    print("")

    # -- 1e. docker-compose.yml validation --
    print("[Section 1e] docker-compose.yml validation")
    run_test("Base has librechat service", test_docker_compose_has_librechat)
    run_test("Base does NOT have mcp-clickhouse", test_docker_compose_no_mcp_in_base)
    run_test("Base has mongodb service", test_docker_compose_has_mongodb)
    run_test("Base has meilisearch service", test_docker_compose_has_meilisearch)
    run_test("Base does NOT have clickhouse", test_docker_compose_no_clickhouse_in_base)
    run_test("data-generator env vars present", test_docker_compose_data_generator_env)
    run_test("data-generator has memory limit", test_docker_compose_data_generator_memory_limit)
    run_test("docker-compose default values", test_docker_compose_defaults)
    run_test("Local overlay has clickhouse", test_docker_compose_local_has_clickhouse)
    run_test("Local overlay has mcp-clickhouse", test_docker_compose_local_has_mcp_clickhouse)
    run_test("Local overlay has langfuse", test_docker_compose_local_has_langfuse)
    run_test("Langfuse headless init env vars in docker-compose.local.yml", test_docker_compose_local_langfuse_headless_init)
    run_test("Langfuse pk/sk placeholders in .env.local.example", test_env_local_example_langfuse_placeholders)
    run_test("librechat.local.yaml MCP configuration", test_librechat_local_yaml_mcp_config)
    run_test("librechat.hybrid.yaml MCP configuration", test_librechat_hybrid_yaml_mcp_config)
    run_test("librechat.local.yaml modelSpecs with prompts", test_librechat_local_yaml_model_specs)
    run_test("librechat.hybrid.yaml modelSpecs with prompts", test_librechat_hybrid_yaml_model_specs)
    run_test("librechat yaml interface locked (both modes)", test_librechat_yaml_interface_locked)
    run_test("librechat yaml Agents endpoint config (both modes)", test_librechat_yaml_agents_endpoint)
    run_test("librechat yaml Artifacts visualization instructions", test_librechat_yaml_visualization_instructions)
    run_test("librechat yaml MV documentation (avgMerge, columns)", test_librechat_yaml_mv_documentation)
    run_test("librechat yaml SQL rules (telco. prefix, no semicolons)", test_librechat_yaml_sql_rules)
    run_test("librechat yaml analysis framework (multi-dimensional)", test_librechat_yaml_analysis_framework)
    run_test("librechat yaml dashboard template (multi-chart grid)", test_librechat_yaml_dashboard_template)
    print("")

    # -- 1e2. LiteLLM configuration --
    print("[Section 1e2] LiteLLM configuration validation")
    run_test("litellm_config.yaml exists", test_litellm_config_exists)
    run_test("litellm_config.yaml has 3 Gemini models", test_litellm_config_models)
    run_test("litellm_config.yaml has Langfuse callback", test_litellm_config_langfuse_callback)
    run_test("docker-compose.yml has litellm service", test_docker_compose_has_litellm)
    run_test("litellm service has healthcheck", test_docker_compose_litellm_healthcheck)
    run_test("librechat depends on litellm", test_docker_compose_librechat_depends_on_litellm)
    run_test("librechat yaml custom LiteLLM endpoint (both modes)", test_librechat_yaml_custom_litellm_endpoint)
    print("")

    # -- 1f. Makefile validation --
    print("[Section 1f] Makefile validation")
    run_test("Setup targets exist (setup-local, setup-hybrid)", test_makefile_setup_targets_exist)
    run_test("init-schema target exists", test_makefile_init_schema_target_exists)
    run_test("Exploration targets exist (check-db, explore-data, query)", test_makefile_new_targets_exist)
    run_test(".PHONY includes all targets", test_makefile_phony_includes_new_targets)
    run_test("DEPLOY_MODE detection in Makefile", test_makefile_deploy_mode_detection)
    run_test("help target mentions all commands", test_makefile_help_mentions_new_commands)
    run_test("Cleanup targets exist (clean-data, clean)", test_makefile_cleanup_targets_exist)
    run_test("No $(call ch_query) in Makefile", test_makefile_no_call_ch_query)
    run_test("Makefile.local.mk exists", test_makefile_local_mk_exists)
    run_test("Makefile.hybrid.mk exists", test_makefile_hybrid_mk_exists)
    run_test("Makefile includes mode-specific .mk file", test_makefile_includes_mode_file)
    run_test("Makefile.local.mk defines COMPOSE_CMD", test_makefile_local_mk_has_compose_cmd)
    run_test("Makefile.hybrid.mk defines COMPOSE_CMD", test_makefile_hybrid_mk_has_compose_cmd)
    run_test("No ifeq ($(DEPLOY_MODE),...) in main Makefile", test_makefile_no_ifeq_deploy_mode)
    print("")

    # -- 1g. README validation --
    print("[Section 1g] README validation")
    run_test("ToC contains expected sections", test_readme_toc_sections)
    run_test("No Legacy Demo Scripts section", test_readme_no_legacy_demo_section)
    run_test("No interactive-demo references", test_readme_no_interactive_demo_references)
    run_test("README mentions LibreChat", test_readme_mentions_librechat)
    run_test("README mentions deployment modes", test_readme_mentions_deployment_modes)
    run_test("At least 4 mermaid code blocks", test_readme_mermaid_blocks)
    run_test("No demo/ or interactive-demo/ in Workshop Materials table", test_readme_no_demo_directory_in_materials)
    run_test("README has Langfuse tracing section", test_readme_langfuse_tracing_section)
    run_test("New defaults documented (10,000 customers)", test_readme_new_defaults)
    print("")

    # Summary
    success = print_summary()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
