"""Minimal authenticated client for Langfuse administrative public APIs."""

import base64
import json
import urllib.request
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


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
        rows = []
        requested_pages = set()
        returned_pages = set()
        page = 1
        limit = 50

        while page not in requested_pages:
            requested_pages.add(page)
            payload = self.call("GET", _page_path(path, page, limit))
            meta = payload.get("meta", {})
            returned_page = meta.get("page")
            total_pages = meta.get("totalPages")
            if not isinstance(returned_page, int) or returned_page in returned_pages:
                break

            returned_pages.add(returned_page)
            rows.extend(payload.get("data", []))
            if not isinstance(total_pages, int) or returned_page >= total_pages:
                break
            page = returned_page + 1

        return [row for row in rows if row.get("name") == name]


def _page_path(path: str, page: int, limit: int) -> str:
    parts = urlsplit(path)
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key not in {"page", "limit"}
    ]
    query.extend([("page", page), ("limit", limit)])
    return urlunsplit((
        parts.scheme,
        parts.netloc,
        parts.path,
        urlencode(query),
        parts.fragment,
    ))
