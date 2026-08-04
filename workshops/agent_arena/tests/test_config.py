import os
from arena.config import load_config


def test_env_expansion(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_CLOUD_HOST", "example.clickhouse.cloud")
    monkeypatch.setenv("CLICKHOUSE_CLOUD_DATABASE", "arena_house")
    monkeypatch.setenv("CLICKHOUSE_CLOUD_USER", "default")
    monkeypatch.setenv("CLICKHOUSE_CLOUD_PASSWORD", "pw")
    monkeypatch.setenv("ARENA_RO_PASSWORD", "ropw")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "https://us.cloud.langfuse.com")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk")
    monkeypatch.setenv("OPENROUTER_API_KEY", "or-test")
    monkeypatch.setenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
    cfg = load_config("config.yaml")
    assert cfg.clickhouse.host == "example.clickhouse.cloud"
    assert cfg.clickhouse.database == "arena_house"
    assert cfg.clickhouse.ro_user == "arena_ro"
    assert cfg.openrouter.base_url == "https://openrouter.ai/api/v1"
    assert cfg.openrouter.api_key == "or-test"
    assert cfg.models, "at least one model configured"
    assert {"anthropic", "openai"} <= {m.family for m in cfg.models}
    assert {"P1_zeroshot", "P3_dialect"} <= {p.name for p in cfg.prompts}
    # prices are present and positive for cost computation
    assert all(m.price_per_1m_in > 0 and m.price_per_1m_out > 0 for m in cfg.models)


def test_resolve_grid():
    cfg = load_config("config.yaml")
    model_names, prompt_names = cfg.resolved_grid()
    # grid is "*" -> resolves to every configured model/prompt name
    assert model_names == [m.name for m in cfg.models]
    assert prompt_names == [p.name for p in cfg.prompts]
    assert "P3_dialect" in prompt_names
