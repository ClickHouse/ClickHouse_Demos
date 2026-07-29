from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, DecimalException
from typing import Any, Iterable


ZERO = Decimal("0")
ONE = Decimal("1")
MAX_SIZE = Decimal("1e20")
MAX_VOLUME_24H = Decimal("1e30") - Decimal("0.00000001")
MIDPOINT_QUANTUM = Decimal("0.000000000001")
EARLIEST_SOURCE_TIME = datetime(2020, 1, 1, tzinfo=UTC)
UINT256_MAX = 2**256 - 1
MAX_RAW_PAYLOAD_CHARS = 16_384
HEX_ID = re.compile(r"^0x[0-9a-fA-F]+$")


def fixed_hex(value: Any, digits: int, field: str) -> str:
    text = str(value or "")
    if len(text) != digits + 2 or not HEX_ID.fullmatch(text):
        raise ValueError(f"{field} must be a 0x-prefixed {digits}-digit hex value")
    return text


def uint256(value: Any, field: str = "token_id") -> int:
    try:
        number = int(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if not 0 < number <= UINT256_MAX:
        raise ValueError(f"{field} must be between 1 and 2^256 - 1")
    return number


def uint64(value: Any, field: str) -> int:
    try:
        number = int(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if not 0 <= number <= 2**64 - 1:
        raise ValueError(f"{field} must be between 0 and 2^64 - 1")
    return number


def decimal_text(value: Any, default: str = "0", max_scale: int | None = None) -> str:
    if value in {None, ""}:
        return default
    raw = str(value)
    if len(raw) > 128:
        raise ValueError("decimal input is too long")
    try:
        normalized = Decimal(raw).normalize()
    except (DecimalException, ValueError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc
    if not normalized.is_finite():
        raise ValueError(f"decimal value must be finite: {value!r}")
    if abs(normalized.adjusted()) > 30 or len(normalized.as_tuple().digits) > 38:
        raise ValueError("decimal value exceeds the supported precision")
    if max_scale is not None and -normalized.as_tuple().exponent > max_scale:
        raise ValueError(f"decimal value exceeds scale {max_scale}")
    return format(normalized, "f")


def parse_timestamp(value: Any) -> datetime:
    if value in {None, ""}:
        raise ValueError("source timestamp is required")
    text = str(value)
    try:
        if text.isdigit():
            number = int(text)
            if number > 10_000_000_000:
                number /= 1000
            parsed = datetime.fromtimestamp(number, UTC)
        else:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("source timestamp must include a timezone")
            parsed = parsed.astimezone(UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError(f"invalid source timestamp: {value!r}") from exc
    if parsed < EARLIEST_SOURCE_TIME or parsed > datetime.now(UTC) + timedelta(minutes=5):
        raise ValueError("source timestamp is outside the accepted time window")
    return parsed


def stable_id(*parts: Any) -> str:
    canonical = "\x1f".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def stable_batch_token(ids: Iterable[str]) -> str:
    return stable_id(*sorted(ids))


def _json_list(value: Any, field: str) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    raise ValueError(f"{field} must be a JSON array")


@dataclass(frozen=True)
class MarketToken:
    market_id: int
    condition_id: str
    token_id: int
    outcome: str
    question: str
    slug: str
    active: bool
    accepting_orders: bool
    volume_24h: str


def normalize_markets(payload: list[dict[str, Any]], limit: int) -> list[MarketToken]:
    tokens: list[MarketToken] = []
    for market in payload:
        try:
            if not isinstance(market, dict):
                continue
            if (
                not market.get("active")
                or market.get("closed")
                or not market.get("acceptingOrders")
            ):
                continue
            token_ids = _json_list(market.get("clobTokenIds"), "clobTokenIds")
            outcomes = _json_list(market.get("outcomes"), "outcomes")
            condition_id = fixed_hex(market.get("conditionId"), 64, "conditionId")
            if not token_ids or len(token_ids) != len(outcomes):
                continue
            normalized_token_ids = [uint256(token_id) for token_id in token_ids]
            market_id = uint64(market["id"], "market id")
            volume_24h = Decimal(
                decimal_text(market.get("volume24hr"), max_scale=8)
            )
            if not ZERO <= volume_24h <= MAX_VOLUME_24H:
                raise ValueError("volume24hr exceeds Decimal128(8)")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
        for token_id, outcome in zip(normalized_token_ids, outcomes, strict=True):
            tokens.append(
                MarketToken(
                    market_id=market_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    outcome=str(outcome),
                    question=str(market.get("question", "")),
                    slug=str(market.get("slug", "")),
                    active=True,
                    accepting_orders=bool(market.get("acceptingOrders", False)),
                    volume_24h=format(volume_24h, "f"),
                )
            )
        if len({token.condition_id for token in tokens}) >= limit:
            break
    selected = {token.condition_id for token in tokens[: limit * 2]}
    return [token for token in tokens if token.condition_id in selected][: limit * 2]


def build_tick(
    *,
    event_kind: str,
    source: str,
    condition_id: str,
    token_id: Any,
    timestamp: Any,
    price: Any = None,
    size: Any = None,
    side: Any = None,
    best_bid: Any = None,
    best_ask: Any = None,
    source_hash: Any = None,
    raw: Any,
) -> dict[str, Any]:
    condition_id = fixed_hex(condition_id, 64, "condition_id")
    normalized_token_id = uint256(token_id)
    event_at = parse_timestamp(timestamp)
    price_text = decimal_text(price, max_scale=12)
    size_text = decimal_text(size, max_scale=8)
    bid_text = decimal_text(best_bid, max_scale=12)
    ask_text = decimal_text(best_ask, max_scale=12)
    bid = Decimal(bid_text)
    ask = Decimal(ask_text)
    normalized_price = Decimal(price_text)
    normalized_size = Decimal(size_text)
    if not ZERO <= normalized_price <= ONE:
        raise ValueError("price must be between 0 and 1")
    if not ZERO <= bid <= ONE or not ZERO <= ask <= ONE:
        raise ValueError("best bid and ask must be between 0 and 1")
    if bid > ZERO and ask > ZERO and bid > ask:
        raise ValueError("best bid cannot exceed best ask")
    if not ZERO <= normalized_size <= MAX_SIZE:
        raise ValueError("size must be between 0 and 1e20")
    midpoint = (
        decimal_text(
            ((bid + ask) / 2).quantize(MIDPOINT_QUANTUM, rounding=ROUND_HALF_EVEN),
            max_scale=12,
        )
        if bid > ZERO and ask > ZERO
        else "0"
    )
    normalized_side = str(side or "UNKNOWN").upper()
    if normalized_side not in {"BUY", "SELL"}:
        normalized_side = "UNKNOWN"
    event_id = stable_id(
        condition_id,
        normalized_token_id,
        event_kind,
        event_at.isoformat(),
        price_text,
        size_text,
        normalized_side,
        bid_text,
        ask_text,
        source_hash or "",
    )
    return {
        "event_id": event_id,
        "condition_id": condition_id,
        "token_id": normalized_token_id,
        "event_at": event_at,
        "observed_at": datetime.now(UTC),
        "event_kind": event_kind,
        "source": source,
        "price": price_text,
        "size": size_text,
        "side": normalized_side,
        "best_bid": bid_text,
        "best_ask": ask_text,
        "midpoint": midpoint,
        "source_hash": str(source_hash or "")[:256],
        "raw_payload": json.dumps(raw, separators=(",", ":"), sort_keys=True)[
            :MAX_RAW_PAYLOAD_CHARS
        ],
    }


def normalize_ws_message(message: dict[str, Any]) -> list[dict[str, Any]]:
    event_type = message.get("event_type") or message.get("type")
    payload = message.get("payload") if isinstance(message.get("payload"), dict) else message
    condition_id = str(payload.get("market", ""))
    timestamp = payload.get("timestamp")
    if event_type == "book":
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        if not isinstance(bids, list) or not all(isinstance(row, dict) for row in bids):
            raise ValueError("book bids must be an array of objects")
        if not isinstance(asks, list) or not all(isinstance(row, dict) for row in asks):
            raise ValueError("book asks must be an array of objects")
        best_bid = max((Decimal(decimal_text(row.get("price"))) for row in bids), default=ZERO)
        best_ask = min((Decimal(decimal_text(row.get("price"))) for row in asks), default=ZERO)
        return [
            build_tick(
                event_kind="book_snapshot",
                source="WEBSOCKET",
                condition_id=condition_id,
                token_id=payload.get("asset_id") or payload.get("tokenId") or payload.get("token_id"),
                timestamp=timestamp,
                best_bid=best_bid,
                best_ask=best_ask,
                source_hash=payload.get("hash"),
                raw=message,
            )
        ]
    if event_type == "price_change":
        changes = payload.get("price_changes") or payload.get("priceChanges") or []
        if not isinstance(changes, list) or not all(
            isinstance(change, dict) for change in changes
        ):
            raise ValueError("price changes must be an array of objects")
        return [
            build_tick(
                event_kind="price_change",
                source="WEBSOCKET",
                condition_id=condition_id,
                token_id=change.get("asset_id") or change.get("tokenId"),
                timestamp=timestamp,
                price=change.get("price"),
                size=change.get("size"),
                side=change.get("side"),
                best_bid=change.get("best_bid") or change.get("bestBid"),
                best_ask=change.get("best_ask") or change.get("bestAsk"),
                source_hash=change.get("hash"),
                raw={
                    "event_type": "price_change",
                    "market": condition_id,
                    "timestamp": timestamp,
                    "change": change,
                },
            )
            for change in changes
        ]
    if event_type == "last_trade_price":
        return [
            build_tick(
                event_kind="last_trade_price",
                source="WEBSOCKET",
                condition_id=condition_id,
                token_id=payload.get("asset_id") or payload.get("tokenId") or payload.get("token_id"),
                timestamp=timestamp,
                price=payload.get("price"),
                size=payload.get("size"),
                side=payload.get("side"),
                source_hash=payload.get("transaction_hash") or payload.get("transactionHash"),
                raw=message,
            )
        ]
    if event_type == "best_bid_ask":
        return [
            build_tick(
                event_kind="best_bid_ask",
                source="WEBSOCKET",
                condition_id=condition_id,
                token_id=payload.get("asset_id") or payload.get("tokenId") or payload.get("token_id"),
                timestamp=timestamp,
                best_bid=payload.get("best_bid") or payload.get("bestBid"),
                best_ask=payload.get("best_ask") or payload.get("bestAsk"),
                raw=message,
            )
        ]
    return []


def normalize_book(payload: dict[str, Any], condition_id: str) -> dict[str, Any]:
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    if not isinstance(bids, list) or not all(isinstance(row, dict) for row in bids):
        raise ValueError("book bids must be an array of objects")
    if not isinstance(asks, list) or not all(isinstance(row, dict) for row in asks):
        raise ValueError("book asks must be an array of objects")
    best_bid = max((Decimal(decimal_text(row.get("price"))) for row in bids), default=ZERO)
    best_ask = min((Decimal(decimal_text(row.get("price"))) for row in asks), default=ZERO)
    return build_tick(
        event_kind="rest_book",
        source="CLOB_REST",
        condition_id=condition_id,
        token_id=payload.get("asset_id") or payload.get("token_id"),
        timestamp=payload.get("timestamp"),
        best_bid=best_bid,
        best_ask=best_ask,
        source_hash=payload.get("hash"),
        raw=payload,
    )


def normalize_trade(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("trade must be an object")
    event_at = parse_timestamp(payload.get("timestamp"))
    condition_id = fixed_hex(payload.get("conditionId"), 64, "conditionId")
    token_id = uint256(payload.get("asset"), "asset")
    proxy_wallet = fixed_hex(payload.get("proxyWallet"), 40, "proxyWallet")
    transaction_hash = fixed_hex(
        payload.get("transactionHash"),
        64,
        "transactionHash",
    )
    price = decimal_text(payload.get("price"), max_scale=12)
    size = decimal_text(payload.get("size"), max_scale=8)
    if not ZERO <= Decimal(price) <= ONE:
        raise ValueError("trade price must be between 0 and 1")
    if not ZERO < Decimal(size) <= MAX_SIZE:
        raise ValueError("trade size must be greater than 0 and at most 1e20")
    side = str(payload.get("side") or "UNKNOWN").upper()
    if side not in {"BUY", "SELL"}:
        side = "UNKNOWN"
    trade_id = stable_id(
        transaction_hash,
        token_id,
        proxy_wallet,
        side,
        price,
        size,
        int(event_at.timestamp()),
    )
    return {
        "trade_id": trade_id,
        "condition_id": condition_id,
        "token_id": token_id,
        "event_at": event_at,
        "observed_at": datetime.now(UTC),
        "proxy_wallet": proxy_wallet,
        "side": side,
        "price": price,
        "size": size,
        "outcome": str(payload.get("outcome", "")),
        "transaction_hash": transaction_hash,
        "title": str(payload.get("title", "")),
    }
