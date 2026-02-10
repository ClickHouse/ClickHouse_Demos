"""
Test script to validate the telco workshop setup.

Mode-aware: reads DEPLOY_MODE from .env to determine which tests to run.
- ClickHouse connectivity is always tested (works for both local and cloud).
- Langfuse is tested only in local mode or when LANGFUSE_HOST is set.
"""

import os
import sys
import time
from typing import Tuple

import clickhouse_connect
from dotenv import load_dotenv


def test_clickhouse_connection() -> Tuple[bool, str]:
    """Test ClickHouse connection."""
    try:
        print("Testing ClickHouse connection...")
        connect_kwargs = dict(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
        if os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true":
            connect_kwargs["secure"] = True

        client = clickhouse_connect.get_client(**connect_kwargs)

        # Test query
        result = client.query("SELECT version()")
        version = result.result_rows[0][0]

        client.close()
        return True, f"Connected to ClickHouse version {version}"
    except Exception as e:
        return False, f"Failed to connect to ClickHouse: {str(e)}"


def test_clickhouse_database() -> Tuple[bool, str]:
    """Test if telco database exists."""
    try:
        print("Testing telco database...")
        connect_kwargs = dict(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
        if os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true":
            connect_kwargs["secure"] = True

        client = clickhouse_connect.get_client(**connect_kwargs)

        # Check if database exists
        result = client.query("SHOW DATABASES")
        databases = [row[0] for row in result.result_rows]

        if "telco" not in databases:
            client.close()
            return False, "Database 'telco' does not exist"

        # Check tables
        result = client.query("SHOW TABLES FROM telco")
        tables = [row[0] for row in result.result_rows]

        expected_tables = [
            "customers",
            "call_detail_records",
            "network_events",
            "marketing_campaigns"
        ]

        missing_tables = [t for t in expected_tables if t not in tables]

        client.close()

        if missing_tables:
            return False, f"Missing tables: {', '.join(missing_tables)}"

        return True, f"Database 'telco' exists with all required tables"
    except Exception as e:
        return False, f"Failed to check database: {str(e)}"


def test_clickhouse_data() -> Tuple[bool, str]:
    """Test if data exists in tables."""
    try:
        print("Testing data in tables...")
        connect_kwargs = dict(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
        if os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true":
            connect_kwargs["secure"] = True

        client = clickhouse_connect.get_client(**connect_kwargs)

        tables = [
            "customers",
            "call_detail_records",
            "network_events",
            "marketing_campaigns"
        ]

        counts = {}
        for table in tables:
            result = client.query(f"SELECT count() FROM telco.{table}")
            counts[table] = result.result_rows[0][0]

        client.close()

        empty_tables = [t for t, c in counts.items() if c == 0]

        if empty_tables:
            return False, f"Empty tables: {', '.join(empty_tables)}"

        summary = ", ".join([f"{t}: {c:,}" for t, c in counts.items()])
        return True, f"Data exists in all tables ({summary})"
    except Exception as e:
        return False, f"Failed to check data: {str(e)}"


def test_langfuse_connection() -> Tuple[bool, str]:
    """Test Langfuse connection."""
    try:
        print("Testing Langfuse connection...")

        public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
        secret_key = os.getenv("LANGFUSE_SECRET_KEY")
        host = os.getenv("LANGFUSE_HOST", "http://localhost:3000")

        if not public_key or not secret_key:
            return False, "Langfuse API keys not configured (LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY)"

        from langfuse import Langfuse

        langfuse = Langfuse(
            public_key=public_key,
            secret_key=secret_key,
            host=host
        )

        # Test by creating a simple trace
        trace = langfuse.trace(name="test_trace")
        langfuse.flush()

        return True, f"Connected to Langfuse at {host}"
    except ImportError:
        return False, "Langfuse library not installed (pip install langfuse)"
    except Exception as e:
        return False, f"Failed to connect to Langfuse: {str(e)}"


def main():
    """Run all tests."""
    # Load environment variables
    load_dotenv()

    deploy_mode = os.getenv("DEPLOY_MODE", "local")

    print("=" * 80)
    print("TELCO WORKSHOP SETUP VALIDATION")
    print("=" * 80)
    print(f"Deploy mode: {deploy_mode}")
    print()

    tests = [
        ("ClickHouse Connection", test_clickhouse_connection),
        ("ClickHouse Database", test_clickhouse_database),
        ("ClickHouse Data", test_clickhouse_data),
    ]

    # Test Langfuse in local mode or when LANGFUSE_HOST is explicitly set
    if deploy_mode == "local" or os.getenv("LANGFUSE_HOST"):
        tests.append(("Langfuse Connection", test_langfuse_connection))

    results = []

    for test_name, test_func in tests:
        print(f"\n[{test_name}]")
        success, message = test_func()
        results.append((test_name, success, message))

        status = "[PASS]" if success else "[FAIL]"
        print(f"{status}: {message}")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, success, _ in results if success)
    total = len(results)

    for test_name, success, message in results:
        status = "[OK]" if success else "[X]"
        print(f"{status} {test_name}")

    print(f"\nPassed: {passed}/{total}")

    if passed == total:
        print("\n[OK] All tests passed! Workshop is ready.")
        return 0
    else:
        print(f"\n[FAIL] {total - passed} test(s) failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
