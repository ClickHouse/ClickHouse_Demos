from pathlib import Path

import pytest

from agents.policy import load_policy, with_policy


def test_policy_versions_encode_the_intentional_active_customer_drift():
    stale = load_policy("policy-v1")
    current = load_policy("policy-v2")
    assert "signup_date" in stale.metrics["active_customers"]
    assert "90 days" in stale.metrics["active_customers"]
    assert "v_orders" in current.metrics["active_customers"]
    assert "30 days" in current.metrics["active_customers"]
    assert "cancelled" in current.metrics["active_customers"]
    assert "returned" in current.metrics["active_customers"]


def test_policy_v2_governs_multiple_metrics_and_renders_into_context():
    policy = load_policy("policy-v2")
    assert set(policy.metrics) == {
        "active_customers",
        "revenue",
        "view_to_purchase_conversion",
        "gross_margin",
    }
    rendered = with_policy("# Schema", policy)
    assert "# Schema" in rendered
    assert "Business metric policy (policy-v2)" in rendered
    assert "active_customers" in rendered and "gross_margin" in rendered


def test_policy_loader_rejects_unknown_or_malformed_versions(tmp_path: Path):
    with pytest.raises(ValueError, match="unknown policy version"):
        load_policy("../../secrets", root=tmp_path)
    (tmp_path / "policy-v3.yaml").write_text("version: policy-v3\nmetrics: []\n")
    with pytest.raises(ValueError, match="metrics must be a non-empty mapping"):
        load_policy("policy-v3", root=tmp_path)
