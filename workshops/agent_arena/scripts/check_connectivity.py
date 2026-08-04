"""Verify all external dependencies are reachable before building further.
Usage: source .env && python scripts/check_connectivity.py
"""
from arena.config import load_config


def check_clickhouse(cfg) -> None:
    from agents.chclient import make_admin_client
    admin = make_admin_client(cfg.clickhouse)
    v = admin.query("SELECT version()").result_rows[0][0]
    print(f"[ok] ClickHouse reachable, version {v}")


def check_openrouter(cfg) -> None:
    from agents.llm import OpenRouterClient
    bc = OpenRouterClient(cfg.openrouter.base_url, cfg.openrouter.api_key)
    model_id = cfg.models[0].id
    res = bc.converse(model_id, system="You are a calculator.",
                      messages=[{"role": "user", "content": [{"text": "Reply with the number 2 only."}]}],
                      inference={"temperature": 0.0, "max_tokens": 8})
    print(f"[ok] OpenRouter {model_id} responded: {res.text!r}, usage={res.usage}")


def check_langfuse(cfg) -> None:
    from langfuse import Langfuse
    lf = Langfuse(public_key=cfg.langfuse.public_key,
                  secret_key=cfg.langfuse.secret_key,
                  host=cfg.langfuse.host)
    assert lf.auth_check(), "LangFuse auth failed"
    print("[ok] LangFuse auth OK")


def main() -> None:
    cfg = load_config()
    check_clickhouse(cfg)
    check_langfuse(cfg)
    try:
        check_openrouter(cfg)
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] OpenRouter check failed (need API key + model access): {e}")


if __name__ == "__main__":
    main()
