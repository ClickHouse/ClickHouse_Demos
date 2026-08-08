import pytest

from scripts.langfuse_admin import (
    LangfuseAdmin,
    iter_cursor_pages,
    iter_numbered_pages,
)


class StubAdmin(LangfuseAdmin):
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        return next(self.responses)


def _page(data, page, total_pages, limit=50, total_items=None):
    return {
        "data": data,
        "meta": {
            "page": page,
            "limit": limit,
            "totalItems": len(data) if total_items is None else total_items,
            "totalPages": total_pages,
        },
    }


def test_list_named_finds_exact_target_on_later_page():
    api = StubAdmin([
        _page([{"name": "other"}], page=1, total_pages=2, total_items=2),
        _page([{"name": "target"}], page=2, total_pages=2, total_items=2),
    ])

    assert api.list_named("/api/public/unstable/evaluators", "target") == [
        {"name": "target"},
    ]
    assert api.calls == [
        ("GET", "/api/public/unstable/evaluators?page=1&limit=50", None),
        ("GET", "/api/public/unstable/evaluators?page=2&limit=50", None),
    ]


def test_list_named_does_not_return_partial_name_matches():
    api = StubAdmin([
        _page(
            [{"name": "target-extra"}, {"name": "pre-target"}],
            page=1,
            total_pages=1,
        ),
    ])

    assert api.list_named("/api/public/unstable/evaluators", "target") == []


def test_list_named_preserves_endpoint_and_url_encodes_query_parameters():
    api = StubAdmin([_page([], page=1, total_pages=1)])

    api.list_named(
        "/api/public/unstable/evaluators?scope=project managed&tag=a/b",
        "target",
    )

    assert api.calls == [
        (
            "GET",
            "/api/public/unstable/evaluators?scope=project+managed&tag=a%2Fb&page=1&limit=50",
            None,
        ),
    ]


def test_list_named_fails_closed_when_server_repeats_page_metadata():
    api = StubAdmin([
        _page([{"name": "first"}], page=1, total_pages=3, total_items=3),
        _page([{"name": "second"}], page=1, total_pages=3, total_items=3),
        _page([{"name": "must-not-be-read"}], page=1, total_pages=3, total_items=3),
    ])

    with pytest.raises(RuntimeError, match="pagination response is incomplete"):
        api.list_named("/api/public/unstable/evaluators", "target")
    assert len(api.calls) == 2


def test_list_named_fails_closed_when_total_pages_exceeds_bound():
    api = StubAdmin([_page([], page=1, total_pages=101, total_items=101)])

    with pytest.raises(RuntimeError, match="pagination response is incomplete"):
        api.list_named("/api/public/unstable/evaluators", "target")

    assert len(api.calls) == 1


def test_list_named_fails_closed_on_page_mismatch():
    api = StubAdmin([_page([], page=2, total_pages=2)])

    with pytest.raises(RuntimeError, match="pagination response is incomplete"):
        api.list_named("/api/public/unstable/evaluators", "target")


def test_list_named_fails_closed_when_total_pages_changes_mid_traversal():
    api = StubAdmin([
        _page([], page=1, total_pages=3, total_items=3),
        _page([], page=2, total_pages=2, total_items=3),
    ])

    with pytest.raises(RuntimeError, match="pagination response is incomplete"):
        api.list_named("/api/public/unstable/evaluators", "target")


def test_list_named_fails_closed_when_total_items_are_not_traversed():
    api = StubAdmin([_page([{"name": "target"}], page=1, total_pages=1,
                           total_items=2)])

    with pytest.raises(RuntimeError, match="pagination response is incomplete"):
        api.list_named("/api/public/unstable/evaluators", "target")


@pytest.mark.parametrize(
    "meta",
    [
        {"page": 1, "totalPages": 1},
        {"page": 1, "totalPages": 1, "limit": 0},
        {"page": 1, "totalPages": 1, "limit": 49},
        {"page": 1, "totalPages": 0, "limit": 50},
    ],
)
def test_list_named_fails_closed_on_incomplete_or_insane_metadata(meta):
    api = StubAdmin([{"data": [], "meta": meta}])

    with pytest.raises(RuntimeError, match="pagination response is incomplete"):
        api.list_named("/api/public/unstable/evaluators", "target")


def test_list_named_filters_only_after_collecting_every_page():
    api = StubAdmin([
        _page([{"name": "target", "id": "first"}], page=1, total_pages=2,
              total_items=2),
        _page([{"name": "target", "id": "second"}], page=2, total_pages=2,
              total_items=2),
    ])

    assert api.list_named("/api/public/unstable/evaluators", "target") == [
        {"name": "target", "id": "first"},
        {"name": "target", "id": "second"},
    ]


def test_numbered_pages_reject_returned_page_that_was_not_requested():
    calls = []

    def fetch(page):
        calls.append(page)
        return _page([{"id": "ambiguous"}], page=2, total_pages=2)

    assert list(iter_numbered_pages(fetch)) == []
    assert calls == [1]


def test_cursor_pages_retains_full_cursorless_terminal_page():
    terminal = {"data": ["first", "second"], "meta": {"limit": 2}}

    assert list(iter_cursor_pages(lambda cursor: terminal)) == [terminal]


def test_cursor_pages_retains_full_last_page_after_cursor():
    pages = {
        None: {"data": ["first"], "meta": {"cursor": "next"}},
        "next": {"data": ["second", "third"], "meta": {"limit": 2}},
    }

    assert [item for page in iter_cursor_pages(pages.get)
            for item in page["data"]] == ["first", "second", "third"]


def test_cursor_pages_yields_page_before_stopping_on_repeated_cursor():
    pages = iter([
        {"data": ["first"], "meta": {"cursor": "same"}},
        {"data": ["second"], "meta": {"cursor": "same"}},
    ])

    assert [item for page in iter_cursor_pages(lambda cursor: next(pages))
            for item in page["data"]] == ["first", "second"]
