from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .fixed_point import dollars_to_cents, format_decimal, quantize_count


def _normalize_price_level(price: Any, size: Any, *, prices_are_dollars: bool) -> tuple[int, Decimal]:
    if prices_are_dollars:
        price_cents = dollars_to_cents(price)
    else:
        price_cents = int(price)
    return price_cents, quantize_count(size)


def _build_side_map(levels: Any, *, prices_are_dollars: bool) -> dict[int, Decimal]:
    side: dict[int, Decimal] = {}
    for level in levels or []:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price_cents, size = _normalize_price_level(level[0], level[1], prices_are_dollars=prices_are_dollars)
        if size > 0:
            side[price_cents] = size
    return side


def extract_orderbook_levels(payload: dict[str, Any]) -> tuple[dict[int, Decimal], dict[int, Decimal]]:
    if "orderbook_fp" in payload and isinstance(payload["orderbook_fp"], dict):
        book = payload["orderbook_fp"]
        return (
            _build_side_map(book.get("yes_dollars"), prices_are_dollars=True),
            _build_side_map(book.get("no_dollars"), prices_are_dollars=True),
        )

    if "orderbook" in payload and isinstance(payload["orderbook"], dict):
        book = payload["orderbook"]
        return (
            _build_side_map(book.get("yes"), prices_are_dollars=False),
            _build_side_map(book.get("no"), prices_are_dollars=False),
        )

    if "msg" in payload and isinstance(payload["msg"], dict):
        return extract_orderbook_levels(payload["msg"])

    return (
        _build_side_map(payload.get("yes_dollars_fp"), prices_are_dollars=True),
        _build_side_map(payload.get("no_dollars_fp"), prices_are_dollars=True),
    )


def serialize_orderbook_side(levels: dict[int, Decimal]) -> list[dict[str, int | str]]:
    return [
        {
            "price_cents": price_cents,
            "count_fp": format_decimal(size, places=2),
        }
        for price_cents, size in sorted(levels.items(), reverse=True)
    ]


@dataclass
class OrderbookState:
    market_ticker: str | None = None
    market_id: str | None = None
    yes_levels: dict[int, Decimal] = field(default_factory=dict)
    no_levels: dict[int, Decimal] = field(default_factory=dict)

    def apply_snapshot(self, payload: dict[str, Any]) -> None:
        msg = payload.get("msg", payload)
        self.market_ticker = msg.get("market_ticker", self.market_ticker)
        self.market_id = msg.get("market_id", self.market_id)
        self.yes_levels, self.no_levels = extract_orderbook_levels(msg)

    def apply_delta(self, payload: dict[str, Any]) -> None:
        msg = payload.get("msg", payload)
        self.market_ticker = msg.get("market_ticker", self.market_ticker)
        self.market_id = msg.get("market_id", self.market_id)

        side = msg["side"]
        price_cents = dollars_to_cents(msg["price_dollars"])
        delta = quantize_count(msg["delta_fp"])
        target = self.yes_levels if side == "yes" else self.no_levels
        updated_size = target.get(price_cents, Decimal("0.00")) + delta
        if updated_size <= 0:
            target.pop(price_cents, None)
            return
        target[price_cents] = updated_size

    @property
    def best_yes_bid_cents(self) -> int | None:
        return max(self.yes_levels) if self.yes_levels else None

    @property
    def best_no_bid_cents(self) -> int | None:
        return max(self.no_levels) if self.no_levels else None

    @property
    def best_yes_ask_cents(self) -> int | None:
        if not self.no_levels:
            return None
        return 100 - max(self.no_levels)

    @property
    def best_no_ask_cents(self) -> int | None:
        if not self.yes_levels:
            return None
        return 100 - max(self.yes_levels)

    def serialized_yes_levels(self) -> list[dict[str, int | str]]:
        return serialize_orderbook_side(self.yes_levels)

    def serialized_no_levels(self) -> list[dict[str, int | str]]:
        return serialize_orderbook_side(self.no_levels)
