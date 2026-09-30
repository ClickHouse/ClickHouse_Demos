from __future__ import annotations

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from fastapi.testclient import TestClient

import app.chat as chat
import app.chat_service as chat_service
import app.llm_bedrock as llm_bedrock
import app.main as main
from app.settings import settings


@pytest.fixture(scope="session", autouse=True)
def wait_for_api() -> None:
    return None


class _Result:
    column_names = ["trips"]
    result_rows = [(42,)]


class _FakeCh:
    def query(self, sql: str, settings=None):  # noqa: A002
        if sql.startswith("DESCRIBE"):
            raise DatabaseError("no schema in this test")
        return _Result()


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(chat, "get_client", lambda **_kw: _FakeCh())
    monkeypatch.setattr(chat_service, "_schema_cache", None)
    return TestClient(main.app)


def test_openai_without_key_is_503_with_the_workshop_message(monkeypatch, client):
    monkeypatch.setattr(settings, "llm_provider", "openai")
    monkeypatch.setattr(settings, "openai_api_key", "")
    r = client.post("/api/chat", json={"message": "how many trips"})
    assert r.status_code == 503
    assert r.json()["detail"].startswith("AI chat is not configured. Set OPENAI_API_KEY")


def test_bedrock_without_openai_key_answers(monkeypatch, client):
    monkeypatch.setattr(settings, "llm_provider", "bedrock")
    monkeypatch.setattr(settings, "openai_api_key", "")
    monkeypatch.setattr(llm_bedrock, "generate_sql_plan", lambda *_a, **_kw: {
        "answer": "Total trips.", "sql": "SELECT count() AS trips FROM taxi_trips",
        "chart": {"type": "none", "x": None, "y": None}})
    r = client.post("/api/chat", json={"message": "how many trips"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["rows"] == [{"trips": 42}]
    assert body["sql"].endswith(f"LIMIT {settings.chat_row_limit}")


def test_bedrock_bad_output_is_502(monkeypatch, client):
    monkeypatch.setattr(settings, "llm_provider", "bedrock")

    def boom(*_a, **_kw):
        raise llm_bedrock.LlmOutputError("malformed_model_output")

    monkeypatch.setattr(llm_bedrock, "generate_sql_plan", boom)
    r = client.post("/api/chat", json={"message": "how many trips"})
    assert r.status_code == 502
    assert "invalid JSON" in r.json()["detail"]


def test_bedrock_without_credentials_is_503_with_the_workshop_message(monkeypatch, client):
    from botocore.exceptions import NoCredentialsError

    monkeypatch.setattr(settings, "llm_provider", "bedrock")

    def no_creds(*_a, **_kw):
        raise NoCredentialsError()

    monkeypatch.setattr(llm_bedrock, "generate_sql_plan", no_creds)
    r = client.post("/api/chat", json={"message": "how many trips"})
    assert r.status_code == 503
    assert r.json()["detail"].startswith("AI chat is not configured. Set AWS_BEARER_TOKEN_BEDROCK")
