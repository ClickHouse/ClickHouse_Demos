"""Minimal authenticated client for Langfuse administrative public APIs."""

import base64
import json
import urllib.request


class LangfuseAdmin:
    def __init__(self, host: str, public_key: str, secret_key: str):
        self.host = host.rstrip("/")
        self.auth = base64.b64encode(
            f"{public_key}:{secret_key}".encode()
        ).decode()

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.host + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Basic {self.auth}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.load(response)

    def list_named(self, path: str, name: str) -> list[dict]:
        rows = self.call("GET", path).get("data", [])
        return [row for row in rows if row.get("name") == name]
