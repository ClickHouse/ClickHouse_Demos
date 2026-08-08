from scripts.langfuse_admin import LangfuseAdmin


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


def test_list_named_stops_when_server_repeats_page_metadata():
    api = StubAdmin([
        _page([{"name": "first"}], page=1, total_pages=3, total_items=3),
        _page([{"name": "second"}], page=1, total_pages=3, total_items=3),
        _page([{"name": "must-not-be-read"}], page=1, total_pages=3, total_items=3),
    ])

    assert api.list_named("/api/public/unstable/evaluators", "target") == []
    assert len(api.calls) == 2


def test_list_named_filters_only_after_collecting_every_page():
    api = StubAdmin([
        _page([{"name": "target", "id": "first"}], page=1, total_pages=2),
        _page([{"name": "target", "id": "second"}], page=2, total_pages=2),
    ])

    assert api.list_named("/api/public/unstable/evaluators", "target") == [
        {"name": "target", "id": "first"},
        {"name": "target", "id": "second"},
    ]
