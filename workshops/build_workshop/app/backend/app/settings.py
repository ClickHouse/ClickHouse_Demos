from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    clickhouse_host: str = ""
    clickhouse_port: int = 8443
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "nyc_tlc_data"
    # The workshop connects only to ClickHouse Cloud, so TLS is the default.
    clickhouse_secure: bool = True
    # ClickHouse Cloud services can idle-scale to zero and take a few seconds to
    # wake, so keep the connect timeout generous.
    clickhouse_connect_timeout: int = 10

    api_cors_origins: str = "http://localhost:5173,http://localhost:8080"

    query_timeout_seconds: int = 5
    # For the full TLC dataset, aggregates over multi-month ranges can easily exceed 5M rows read.
    # Keep these configurable via env; defaults are tuned for "full dataset demo" rather than the tiny seed.
    max_rows_to_read: int = 200_000_000
    max_bytes_to_read: int = 5_000_000_000

    # --- AI chat (NL-to-SQL) ---
    # Participants bring their own key. When unset, the /api/chat endpoint returns 503
    # and the rest of the app is unaffected.
    openai_api_key: str = ""
    llm_model: str = "gpt-5.4-mini"
    llm_base_url: str = "https://api.openai.com/v1"

    # Guardrails applied to every model-generated query.
    chat_row_limit: int = 100  # appended as LIMIT when the model omits one
    chat_max_result_rows: int = 1000
    chat_query_timeout_seconds: int = 30

    # --- Langfuse tracing (optional, v4 SDK) ---
    # When both keys are set the chat flow is traced; when absent tracing is disabled gracefully.
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    # LANGFUSE_BASE_URL is the v4 env name; LANGFUSE_HOST is accepted as a fallback alias.
    langfuse_base_url: str = Field(
        default="https://us.cloud.langfuse.com",
        validation_alias=AliasChoices("LANGFUSE_BASE_URL", "LANGFUSE_HOST"),
    )

    @property
    def clickhouse_secure_effective(self) -> bool:
        return self.clickhouse_secure


settings = Settings()
