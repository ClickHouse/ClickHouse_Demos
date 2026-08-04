import sys

import scripts.provision_workshop_keys as provision
from scripts.provision_workshop_keys import build_create_body, extract_key, key_hash


def test_build_create_body_includes_limit():
    assert build_create_body("wk", 20) == {"name": "wk", "limit": 20}


def test_build_create_body_omits_limit_when_none():
    assert build_create_body("wk", None) == {"name": "wk"}


def test_extract_key_top_level():
    assert extract_key({"key": "sk-or-v1-abc", "data": {"hash": "h1"}}) == "sk-or-v1-abc"


def test_extract_key_under_data_wrapper():
    assert extract_key({"data": {"key": "sk-or-v1-xyz", "hash": "h2"}}) == "sk-or-v1-xyz"


def test_extract_key_missing_returns_none():
    assert extract_key({"data": {"hash": "h3"}}) is None
    assert extract_key({}) is None


def test_key_hash_both_shapes():
    assert key_hash({"data": {"hash": "h1"}}) == "h1"
    assert key_hash({"hash": "h2"}) == "h2"
    assert key_hash({}) is None


def test_main_uses_current_create_and_daily_reset_api(monkeypatch, capsys):
    calls = []

    def fake_req(method, url, mgmt_key, body=None):
        calls.append((method, url, mgmt_key, body))
        return {"key": "sk-test", "data": {"hash": "hash-1"}}

    monkeypatch.setattr(provision, "_req", fake_req)
    monkeypatch.setenv("OPENROUTER_PROVISIONING_KEY", "mgmt-test")
    monkeypatch.setattr(sys, "argv", ["provision", "--name", "learner", "--daily"])

    provision.main()

    assert calls == [
        ("POST", "https://openrouter.ai/api/v1/keys", "mgmt-test",
         {"name": "learner", "limit": 20.0}),
        ("PATCH", "https://openrouter.ai/api/v1/keys/hash-1", "mgmt-test",
         {"limit_reset": "daily"}),
    ]
    assert "created: learner" in capsys.readouterr().out
