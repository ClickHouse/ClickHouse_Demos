"""OpenRouter LLM provider. Keeps the internal Bedrock-Converse call contract
(converse(model_id, system, messages, inference) -> ConverseResult) so every
existing call site works unchanged; the OpenAI-shape translation happens here.
"""
import json
import sys
import time
import urllib.request
from urllib.error import HTTPError
from dataclasses import dataclass


@dataclass(frozen=True)
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0

    def __add__(self, other: "Usage") -> "Usage":
        return Usage(self.input_tokens + other.input_tokens,
                     self.output_tokens + other.output_tokens)


def ZeroUsage() -> Usage:
    return Usage(0, 0)


@dataclass
class ConverseResult:
    text: str
    usage: Usage


def _to_openai_messages(system: str, messages: list[dict]) -> list[dict]:
    """Bedrock-Converse shape [{"role","content":[{"text"}]}] -> OpenAI chat
    [{"role","content": str}], prepending a system message when present."""
    out: list[dict] = []
    if system:
        out.append({"role": "system", "content": system})
    for m in messages:
        parts = m.get("content", [])
        if isinstance(parts, list):
            text = "".join(p.get("text", "") for p in parts)
        else:
            text = str(parts)
        out.append({"role": m["role"], "content": text})
    return out


def _parse_response(resp: dict) -> ConverseResult:
    choices = resp.get("choices") or []
    if not choices:
        print("[llm] OpenRouter response had no choices:", str(resp)[:200], file=sys.stderr)
        return ConverseResult(text="", usage=ZeroUsage())
    text = (choices[0].get("message", {}) or {}).get("content") or ""
    u = resp.get("usage", {}) or {}
    usage = Usage(input_tokens=u.get("prompt_tokens", 0),
                  output_tokens=u.get("completion_tokens", 0))
    return ConverseResult(text=text, usage=usage)


class OpenRouterClient:
    def __init__(self, base_url: str, api_key: str):
        self._base = base_url.rstrip("/")
        self._key = api_key

    def converse(self, model_id: str, system: str, messages: list[dict],
                 inference: dict) -> ConverseResult:
        body: dict = {"model": model_id,
                      "messages": _to_openai_messages(system, messages)}
        if "temperature" in inference:
            body["temperature"] = inference["temperature"]
        max_tok = inference.get("max_tokens", inference.get("maxTokens"))
        if max_tok is not None:
            body["max_tokens"] = max_tok
        req = urllib.request.Request(
            f"{self._base}/chat/completions",
            data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {self._key}",
                     "Content-Type": "application/json"},
            method="POST")
        for attempt in range(4):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    resp = json.loads(r.read().decode())
                break
            except HTTPError as e:
                if e.code != 429 or attempt == 3:
                    raise
                retry_after = e.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else 2 ** attempt
                except ValueError:
                    delay = 2 ** attempt
                time.sleep(max(0.0, delay))
        return _parse_response(resp)


def cost_usd(usage: Usage, price_in_per_1m: float, price_out_per_1m: float) -> float:
    return (usage.input_tokens / 1_000_000.0) * price_in_per_1m + \
           (usage.output_tokens / 1_000_000.0) * price_out_per_1m


def _parse_prices(data: dict) -> dict[str, tuple[float, float]]:
    """OpenRouter /models pricing is USD-per-token strings; x1e6 -> per-1M."""
    out: dict[str, tuple[float, float]] = {}
    for m in data.get("data", []):
        pricing = m.get("pricing") or {}
        try:
            pin = float(pricing["prompt"]) * 1_000_000.0
            pout = float(pricing["completion"]) * 1_000_000.0
        except (KeyError, TypeError, ValueError):
            continue
        mid = m.get("id")
        if not mid:
            continue
        out[mid] = (round(pin, 4), round(pout, 4))
    return out


def fetch_openrouter_prices(base_url: str, timeout: int = 15) -> dict[str, tuple[float, float]]:
    req = urllib.request.Request(f"{base_url.rstrip('/')}/models")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode())
    return _parse_prices(data)


def apply_live_prices(models, prices):
    """Return new ModelCfg list with live prices where the id is known."""
    updated = []
    for m in models:
        if m.id in prices:
            pin, pout = prices[m.id]
            updated.append(m.model_copy(update={"price_per_1m_in": pin,
                                                "price_per_1m_out": pout}))
        else:
            updated.append(m)
    return updated
