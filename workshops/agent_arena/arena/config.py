import os
import re
from typing import Any
import yaml
from pydantic import BaseModel

_ENV_RE = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        def repl(m: "re.Match") -> str:
            var = m.group(1)
            if var not in os.environ:
                raise KeyError(f"Missing required env var: {var}")
            return os.environ[var]
        return _ENV_RE.sub(repl, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


class QueryLimits(BaseModel):
    max_execution_time: int
    max_result_rows: int
    max_memory_usage: int
    max_rows_to_read: int
    max_bytes_to_read: int
    result_limit: int


class ClickHouseCfg(BaseModel):
    host: str
    port: int
    secure: bool
    database: str
    admin_user: str
    admin_password: str
    ro_user: str
    ro_password: str
    query_limits: QueryLimits


class OpenRouterCfg(BaseModel):
    base_url: str
    api_key: str
    inference: dict


class LangfuseCfg(BaseModel):
    host: str
    public_key: str
    secret_key: str


class EvalCfg(BaseModel):
    float_dp: int
    default_max_retries: int
    run_tag: str


class ModelCfg(BaseModel):
    id: str
    name: str
    family: str
    price_per_1m_in: float
    price_per_1m_out: float


class PromptCfg(BaseModel):
    name: str
    self_correct: bool = False
    k: int = 0
    desc: str = ""


class GridCfg(BaseModel):
    models: list[str]
    prompts: list[str]


class Config(BaseModel):
    clickhouse: ClickHouseCfg
    openrouter: OpenRouterCfg
    langfuse: LangfuseCfg
    eval: EvalCfg
    models: list[ModelCfg]
    prompts: list[PromptCfg]
    grid: GridCfg

    def resolved_grid(self) -> tuple[list[str], list[str]]:
        all_models = [m.name for m in self.models]
        all_prompts = [p.name for p in self.prompts]
        mods = all_models if self.grid.models == ["*"] else self.grid.models
        prms = all_prompts if self.grid.prompts == ["*"] else self.grid.prompts
        return mods, prms

    def model_by_name(self, name: str) -> ModelCfg:
        return next(m for m in self.models if m.name == name)

    def prompt_by_name(self, name: str) -> PromptCfg:
        return next(p for p in self.prompts if p.name == name)


def load_config(path: str = "config.yaml") -> Config:
    from dotenv import load_dotenv
    load_dotenv()
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Config(**_expand(raw))
