import json
from datetime import UTC, datetime

import pytest

from collector.models import (
    decimal_text,
    normalize_book,
    normalize_markets,
    normalize_trade,
    normalize_ws_message,
    stable_batch_token,
)


def test_normalize_markets_parses_json_encoded_arrays_and_limit():
    payload = [
        {
            "id": "42",
            "conditionId": "0x" + "a" * 64,
            "clobTokenIds": '["123", "456"]',
            "outcomes": '["Yes", "No"]',
            "question": "Will it ship?",
            "slug": "will-it-ship",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "volume24hr": 12.5,
        },
        {
            "id": "43",
            "conditionId": "0x" + "b" * 64,
            "clobTokenIds": '["789", "987"]',
            "outcomes": '["Up", "Down"]',
            "active": True,
            "closed": False,
            "acceptingOrders": True,
        },
    ]

    result = normalize_markets(payload, 1)

    assert [(row.token_id, row.outcome) for row in result] == [(123, "Yes"), (456, "No")]
    assert result[0].volume_24h == "12.5"


def test_normalize_markets_skips_closed_and_mismatched_markets():
    payload = [
        {
            "id": "1",
            "conditionId": "0x" + "a" * 64,
            "clobTokenIds": '["1"]',
            "outcomes": '["Yes", "No"]',
            "active": True,
            "closed": False,
            "acceptingOrders": True,
        },
        {
            "id": "2",
            "conditionId": "0x" + "b" * 64,
            "clobTokenIds": '["2", "3"]',
            "outcomes": '["Yes", "No"]',
            "active": False,
            "closed": True,
        },
    ]

    assert normalize_markets(payload, 5) == []


def test_normalize_markets_rejects_malformed_external_identifiers():
    payload = [
        {
            "id": "1",
            "conditionId": "not-a-condition-hash",
            "clobTokenIds": '["1", "2"]',
            "outcomes": '["Yes", "No"]',
            "active": True,
            "closed": False,
            "acceptingOrders": True,
        },
        {
            "id": "2",
            "conditionId": "0x" + "b" * 64,
            "clobTokenIds": '["3", "4"]',
            "outcomes": '["Yes", "No"]',
            "active": True,
            "closed": False,
            "acceptingOrders": True,
        },
    ]

    result = normalize_markets(payload, 5)

    assert {row.market_id for row in result} == {2}


def test_normalize_markets_skips_markets_without_live_order_books():
    payload = [
        {
            "id": "1",
            "conditionId": "0x" + "a" * 64,
            "clobTokenIds": '["1", "2"]',
            "outcomes": '["Yes", "No"]',
            "active": True,
            "closed": False,
            "acceptingOrders": False,
        }
    ]

    assert normalize_markets(payload, 5) == []


@pytest.mark.parametrize(
    ("message", "kind", "midpoint"),
    [
        (
            {
                "event_type": "book",
                "market": "0x" + "1" * 64,
                "asset_id": "123",
                "timestamp": "1782753357257",
                "bids": [{"price": "0.48", "size": "2"}],
                "asks": [{"price": "0.52", "size": "3"}],
            },
            "book_snapshot",
            "0.5",
        ),
        (
            {
                "event_type": "last_trade_price",
                "market": "0x" + "1" * 64,
                "asset_id": "123",
                "timestamp": "1782753357257",
                "price": "0.51",
                "size": "9",
                "side": "BUY",
            },
            "last_trade_price",
            "0",
        ),
        (
            {
                "event_type": "best_bid_ask",
                "market": "0x" + "1" * 64,
                "asset_id": "123",
                "timestamp": "1782753357257",
                "best_bid": "0.2",
                "best_ask": "0.4",
            },
            "best_bid_ask",
            "0.3",
        ),
    ],
)
def test_normalize_ws_event_types(message, kind, midpoint):
    row = normalize_ws_message(message)[0]

    assert row["event_kind"] == kind
    assert row["midpoint"] == midpoint
    assert len(row["event_id"]) == 64


def test_normalize_wrapped_price_changes_flattens_each_change():
    message = {
        "topic": "market",
        "type": "price_change",
        "payload": {
            "market": "0x" + "2" * 64,
            "timestamp": "1782753357257",
            "priceChanges": [
                {
                    "tokenId": "10",
                    "price": "0.1",
                    "size": "2",
                    "side": "BUY",
                    "bestBid": "0.1",
                    "bestAsk": "0.2",
                },
                {
                    "tokenId": "11",
                    "price": "0.9",
                    "size": "3",
                    "side": "SELL",
                    "bestBid": "0.8",
                    "bestAsk": "0.9",
                },
            ],
        },
    }

    rows = normalize_ws_message(message)

    assert [row["token_id"] for row in rows] == [10, 11]
    assert [row["midpoint"] for row in rows] == ["0.15", "0.85"]
    assert json.loads(rows[0]["raw_payload"])["change"]["tokenId"] == "10"
    assert "priceChanges" not in json.loads(rows[0]["raw_payload"])


def test_normalize_book_uses_highest_bid_and_lowest_ask():
    row = normalize_book(
        {
            "asset_id": "9",
            "timestamp": "1782753357257",
            "bids": [{"price": "0.2"}, {"price": "0.3"}],
            "asks": [{"price": "0.7"}, {"price": "0.6"}],
        },
        "0x" + "3" * 64,
    )

    assert row["best_bid"] == "0.3"
    assert row["best_ask"] == "0.6"
    assert row["midpoint"] == "0.45"
    assert row["source"] == "CLOB_REST"


def test_trade_id_is_deterministic_and_uses_full_row_identity():
    base = {
        "transactionHash": "0x" + "4" * 64,
        "asset": "123",
        "proxyWallet": "0x" + "5" * 40,
        "side": "BUY",
        "price": "0.5000",
        "size": "2.00",
        "timestamp": 1_700_000_000,
        "conditionId": "0x" + "6" * 64,
        "outcome": "Yes",
        "title": "Question",
    }

    first = normalize_trade(base)
    second = normalize_trade({**base, "price": "0.5"})
    different = normalize_trade({**base, "size": "3"})

    assert first["trade_id"] == second["trade_id"]
    assert first["trade_id"] != different["trade_id"]
    assert first["event_at"] == datetime.fromtimestamp(1_700_000_000, UTC)


def test_trade_rejects_malformed_fixed_width_identifiers():
    with pytest.raises(ValueError, match="proxyWallet"):
        normalize_trade(
            {
                "transactionHash": "0x" + "4" * 64,
                "asset": "123",
                "proxyWallet": "not-a-wallet",
                "price": "0.5",
                "size": "2",
                "timestamp": 1_700_000_000,
                "conditionId": "0x" + "6" * 64,
            }
        )


def test_stable_batch_token_is_order_independent():
    assert stable_batch_token(["b", "a"]) == stable_batch_token(["a", "b"])


def test_decimal_text_rejects_invalid_input():
    with pytest.raises(ValueError, match="invalid decimal"):
        decimal_text("not-a-number")


@pytest.mark.parametrize("value", ["Infinity", "NaN", "1e999999999", "9" * 129])
def test_decimal_text_rejects_non_finite_or_unbounded_input(value):
    with pytest.raises(ValueError):
        decimal_text(value)
