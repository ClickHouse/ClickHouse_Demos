"""Minimal authenticated client for Langfuse administrative public APIs."""

import base64
import json
import urllib.request
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


MAX_API_PAGES = 100


def iter_numbered_pages(fetch_page, max_pages: int = MAX_API_PAGES):
    """Yield bounded pages only when page/totalPages metadata is trustworthy."""
    requested_pages = set()
    returned_pages = set()
    page = 1
    for _ in range(max_pages):
        if page in requested_pages:
            return
        requested_pages.add(page)
        payload = fetch_page(page)
        meta = payload.get("meta", {})
        returned_page = meta.get("page")
        total_pages = meta.get("totalPages")
        if (
            not isinstance(returned_page, int)
            or not isinstance(total_pages, int)
            or returned_page != page
            or returned_page in returned_pages
            or returned_page < 1
            or total_pages < returned_page
        ):
            return
        returned_pages.add(returned_page)
        yield payload
        if returned_page >= total_pages:
            return
        page = returned_page + 1


def iter_cursor_pages(fetch_page, max_pages: int = MAX_API_PAGES):
    """Yield bounded cursor pages and stop before a repeated/invalid page."""
    cursor = None
    requested_cursors = {None}
    for _ in range(max_pages):
        payload = fetch_page(cursor)
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            return
        if "cursor" not in meta:
            limit = meta.get("limit")
            data = payload.get("data")
            if (
                isinstance(limit, int)
                and limit > 0
                and isinstance(data, list)
                and len(data) <= limit
            ):
                yield payload
            return
        next_cursor = meta.get("cursor")
        if next_cursor is not None and not isinstance(next_cursor, str):
            return
        yield payload
        if (
            not next_cursor
            or next_cursor == cursor
            or next_cursor in requested_cursors
        ):
            return
        requested_cursors.add(next_cursor)
        cursor = next_cursor


def score_trace_id(score: dict) -> str | None:
    direct = score.get("traceId")
    if direct:
        return direct
    subject = score.get("subject")
    if not isinstance(subject, dict):
        return None
    linked_trace = subject.get("traceId")
    if linked_trace:
        return linked_trace
    if subject.get("kind") == "trace":
        return subject.get("id")
    return None


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
