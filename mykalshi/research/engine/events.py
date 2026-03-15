from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from ...fixed_point import quantize_count


SUPPORTED_ACTIONS = {"buy_yes", "sell_yes", "buy_no", "sell_no"}


def normalize_action(action: str) -> str:
    normalized = action.lower()
    if normalized not in SUPPORTED_ACTIONS:
        raise ValueError(f"Unsupported Kalshi action: {action!r}")
    return normalized


def midpoint_cents(bid_cents: int, ask_cents: int) -> int:
    return int(
        (Decimal(bid_cents) + Decimal(ask_cents))
        .__truediv__(Decimal("2"))
        .quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )


@dataclass(frozen=True)
class EngineEvent:
    timestamp: str
    event_type: str


@dataclass(frozen=True)
class MarketEvent(EngineEvent):
    market_ticker: str
    sequence: int | None = None
    raw_data: dict[str, Any] | None = None


@dataclass(frozen=True)
class TradeMarketEvent(MarketEvent):
    yes_price_cents: int = 0
    no_price_cents: int = 0
    trade_quantity: Decimal = field(default_factory=lambda: Decimal("1.00"))
    trade_id: str | None = None
    taker_side: str | None = None


@dataclass(frozen=True)
class TickerMarketEvent(MarketEvent):
    last_price_cents: int | None = None
    yes_bid_cents: int | None = None
    yes_ask_cents: int | None = None
    yes_bid_size: Decimal | None = None
    yes_ask_size: Decimal | None = None
    volume: Decimal | None = None
    open_interest: Decimal | None = None


@dataclass(frozen=True)
class OrderbookMarketEvent(MarketEvent):
    best_yes_bid_cents: int | None = None
    best_yes_ask_cents: int | None = None
    best_no_bid_cents: int | None = None
    best_no_ask_cents: int | None = None
    yes_levels: tuple[tuple[int, Decimal], ...] = ()
    no_levels: tuple[tuple[int, Decimal], ...] = ()
    previous_yes_levels: tuple[tuple[int, Decimal], ...] = ()
    previous_no_levels: tuple[tuple[int, Decimal], ...] = ()
    best_yes_bid_size: Decimal | None = None
    best_yes_ask_size: Decimal | None = None
    previous_best_yes_bid_size: Decimal | None = None
    previous_best_yes_ask_size: Decimal | None = None
    previous_best_yes_bid_cents: int | None = None
    previous_best_yes_ask_cents: int | None = None
    previous_best_no_bid_cents: int | None = None
    previous_best_no_ask_cents: int | None = None


@dataclass(frozen=True)
class SettlementEvent(MarketEvent):
    yes_payout_cents: int | None = None
    no_payout_cents: int | None = None
    reason: str | None = None


@dataclass(frozen=True)
class OrderRequest(EngineEvent):
    market_ticker: str
    action: str
    quantity: Decimal
    order_type: str = "market"
    limit_price_cents: int | None = None
    slippage_cents: int = 0
    latency_events: int = 0
    tag: str | None = None
    note: str | None = None


@dataclass(frozen=True)
class CancelRequest(EngineEvent):
    order_id: str


@dataclass(frozen=True)
class OrderEvent(EngineEvent):
    order_id: str
    market_ticker: str
    action: str
    status: str
    quantity: Decimal
    remaining_quantity: Decimal
    order_type: str
    limit_price_cents: int | None = None
    average_fill_price_cents: int | None = None
    reason: str | None = None
    tag: str | None = None
    note: str | None = None
    reserved_cash_cents: Decimal = Decimal("0.00")
    reserved_yes_quantity: Decimal = Decimal("0.00")
    reserved_no_quantity: Decimal = Decimal("0.00")
    liquidity_intent: str | None = None
    resting_price_cents: int | None = None
    queue_ahead_quantity: Decimal = Decimal("0.00")
    remaining_latency_events: int = 0


@dataclass(frozen=True)
class FillEvent(EngineEvent):
    order_id: str
    market_ticker: str
    action: str
    quantity: Decimal
    price_cents: int
    fee_cents: Decimal
    order_status: str
    liquidity_role: str | None = None
    tag: str | None = None
    note: str | None = None
    cash_after_cents: Decimal | None = None
    yes_position: Decimal | None = None
    no_position: Decimal | None = None


@dataclass(frozen=True)
class MarkEvent(EngineEvent):
    market_ticker: str
    yes_price_cents: int | None
    no_price_cents: int | None
    market_equity_cents: Decimal
    total_equity_cents: Decimal
    cash_cents: Decimal


@dataclass
class MarketState:
    market_ticker: str
    last_event_type: str | None = None
    last_update_timestamp: str | None = None
    last_trade_yes_price_cents: int | None = None
    last_trade_no_price_cents: int | None = None
    last_trade_quantity: Decimal = field(default_factory=lambda: Decimal("0.00"))
    best_yes_bid_cents: int | None = None
    best_yes_ask_cents: int | None = None
    best_no_bid_cents: int | None = None
    best_no_ask_cents: int | None = None
    yes_levels: tuple[tuple[int, Decimal], ...] = ()
    no_levels: tuple[tuple[int, Decimal], ...] = ()
    previous_yes_levels: tuple[tuple[int, Decimal], ...] = ()
    previous_no_levels: tuple[tuple[int, Decimal], ...] = ()
    best_yes_bid_size: Decimal | None = None
    best_yes_ask_size: Decimal | None = None
    previous_best_yes_bid_size: Decimal | None = None
    previous_best_yes_ask_size: Decimal | None = None
    previous_best_yes_bid_cents: int | None = None
    previous_best_yes_ask_cents: int | None = None
    previous_best_no_bid_cents: int | None = None
    previous_best_no_ask_cents: int | None = None
    settled: bool = False
    settlement_pending: bool = False
    yes_payout_cents: int | None = None
    no_payout_cents: int | None = None

    def update(self, event: MarketEvent) -> None:
        self.last_event_type = event.event_type
        self.last_update_timestamp = event.timestamp
        if isinstance(event, TradeMarketEvent):
            self.last_trade_yes_price_cents = event.yes_price_cents
            self.last_trade_no_price_cents = event.no_price_cents
            self.last_trade_quantity = quantize_count(event.trade_quantity)
            return

        if isinstance(event, TickerMarketEvent):
            self.previous_best_yes_bid_cents = self.best_yes_bid_cents
            self.previous_best_yes_ask_cents = self.best_yes_ask_cents
            self.previous_best_no_bid_cents = self.best_no_bid_cents
            self.previous_best_no_ask_cents = self.best_no_ask_cents
            self.previous_best_yes_bid_size = self.best_yes_bid_size
            self.previous_best_yes_ask_size = self.best_yes_ask_size

            self.best_yes_bid_cents = event.yes_bid_cents
            self.best_yes_ask_cents = event.yes_ask_cents
            self.best_yes_bid_size = quantize_count(event.yes_bid_size) if event.yes_bid_size is not None else None
            self.best_yes_ask_size = quantize_count(event.yes_ask_size) if event.yes_ask_size is not None else None
            if event.yes_bid_cents is not None:
                self.best_no_ask_cents = 100 - event.yes_bid_cents
            if event.yes_ask_cents is not None:
                self.best_no_bid_cents = 100 - event.yes_ask_cents
            if event.last_price_cents is not None:
                self.last_trade_yes_price_cents = event.last_price_cents
                self.last_trade_no_price_cents = 100 - event.last_price_cents
            return

        if isinstance(event, OrderbookMarketEvent):
            self.previous_best_yes_bid_cents = self.best_yes_bid_cents
            self.previous_best_yes_ask_cents = self.best_yes_ask_cents
            self.previous_best_no_bid_cents = self.best_no_bid_cents
            self.previous_best_no_ask_cents = self.best_no_ask_cents
            self.previous_yes_levels = self.yes_levels
            self.previous_no_levels = self.no_levels

            self.best_yes_bid_cents = event.best_yes_bid_cents
            self.best_yes_ask_cents = event.best_yes_ask_cents
            self.best_no_bid_cents = event.best_no_bid_cents
            self.best_no_ask_cents = event.best_no_ask_cents
            self.yes_levels = event.yes_levels
            self.no_levels = event.no_levels
            return

        if isinstance(event, SettlementEvent):
            self.settlement_pending = event.yes_payout_cents is None or event.no_payout_cents is None
            self.settled = not self.settlement_pending
            self.yes_payout_cents = event.yes_payout_cents
            self.no_payout_cents = event.no_payout_cents
            if event.yes_payout_cents is not None:
                self.last_trade_yes_price_cents = event.yes_payout_cents
            if event.no_payout_cents is not None:
                self.last_trade_no_price_cents = event.no_payout_cents

    @property
    def top_yes_bid_size(self) -> Decimal | None:
        return self.yes_levels[0][1] if self.yes_levels else None

    @property
    def top_no_bid_size(self) -> Decimal | None:
        return self.no_levels[0][1] if self.no_levels else None

    def executable_quote(self, action: str) -> tuple[int | None, Decimal | None]:
        normalized = normalize_action(action)
        if normalized == "buy_yes":
            if self.best_yes_ask_cents is not None:
                return self.best_yes_ask_cents, self.top_no_bid_size
            return self.last_trade_yes_price_cents, self.last_trade_quantity
        if normalized == "sell_yes":
            if self.best_yes_bid_cents is not None:
                return self.best_yes_bid_cents, self.top_yes_bid_size
            return self.last_trade_yes_price_cents, self.last_trade_quantity
        if normalized == "buy_no":
            if self.best_no_ask_cents is not None:
                return self.best_no_ask_cents, self.top_yes_bid_size
            return self.last_trade_no_price_cents, self.last_trade_quantity
        if self.best_no_bid_cents is not None:
            return self.best_no_bid_cents, self.top_no_bid_size
        return self.last_trade_no_price_cents, self.last_trade_quantity


    @staticmethod
    def _level_size(levels: tuple[tuple[int, Decimal], ...], price_cents: int) -> Decimal:
        for level_price, level_size in levels:
            if int(level_price) == int(price_cents):
                return quantize_count(level_size)
        return Decimal("0.00")

    def _consumption_from_orderbook(self, action: str, resting_price_cents: int) -> Decimal:
        if action in {"sell_yes", "buy_no"}:
            previous = self._level_size(self.previous_yes_levels, resting_price_cents)
            current = self._level_size(self.yes_levels, resting_price_cents)
        else:
            previous = self._level_size(self.previous_no_levels, resting_price_cents)
            current = self._level_size(self.no_levels, resting_price_cents)
        return max(Decimal("0.00"), quantize_count(previous - current))

    def _consumption_from_ticker(self, action: str, resting_price_cents: int) -> Decimal:
        if action == "sell_yes":
            if self.previous_best_yes_bid_cents != resting_price_cents or self.best_yes_bid_cents != resting_price_cents:
                return Decimal("0.00")
            previous_size = self.previous_best_yes_bid_size
            current_size = self.best_yes_bid_size
        elif action == "buy_yes":
            if self.previous_best_yes_bid_cents != resting_price_cents or self.best_yes_bid_cents != resting_price_cents:
                return Decimal("0.00")
            previous_size = self.previous_best_yes_bid_size
            current_size = self.best_yes_bid_size
        elif action == "buy_no":
            target_yes_bid = 100 - resting_price_cents
            if self.previous_best_yes_bid_cents != target_yes_bid or self.best_yes_bid_cents != target_yes_bid:
                return Decimal("0.00")
            previous_size = self.previous_best_yes_bid_size
            current_size = self.best_yes_bid_size
        else:
            target_yes_ask = 100 - resting_price_cents
            if self.previous_best_yes_ask_cents != target_yes_ask or self.best_yes_ask_cents != target_yes_ask:
                return Decimal("0.00")
            previous_size = self.previous_best_yes_ask_size
            current_size = self.best_yes_ask_size

        if previous_size is None or current_size is None:
            return Decimal("0.00")
        return max(Decimal("0.00"), quantize_count(previous_size - current_size))

    def estimated_queue_consumption(self, action: str, resting_price_cents: int, event: MarketEvent) -> Decimal:
        normalized = normalize_action(action)
        if isinstance(event, TradeMarketEvent):
            traded_price = event.yes_price_cents if normalized.endswith("yes") else event.no_price_cents
            if traded_price != resting_price_cents:
                return Decimal("0.00")
            return quantize_count(event.trade_quantity)
        if isinstance(event, OrderbookMarketEvent):
            return self._consumption_from_orderbook(normalized, resting_price_cents)
        if isinstance(event, TickerMarketEvent):
            return self._consumption_from_ticker(normalized, resting_price_cents)
        return Decimal("0.00")

    def mark_prices(self) -> tuple[int | None, int | None]:
        if self.last_trade_yes_price_cents is not None and self.last_trade_no_price_cents is not None:
            return self.last_trade_yes_price_cents, self.last_trade_no_price_cents

        if self.best_yes_bid_cents is not None and self.best_yes_ask_cents is not None:
            yes_price = midpoint_cents(self.best_yes_bid_cents, self.best_yes_ask_cents)
            return yes_price, 100 - yes_price

        if self.best_no_bid_cents is not None and self.best_no_ask_cents is not None:
            no_price = midpoint_cents(self.best_no_bid_cents, self.best_no_ask_cents)
            return 100 - no_price, no_price

        if self.best_yes_bid_cents is not None:
            return self.best_yes_bid_cents, 100 - self.best_yes_bid_cents

        if self.best_yes_ask_cents is not None:
            return self.best_yes_ask_cents, 100 - self.best_yes_ask_cents

        if self.best_no_bid_cents is not None:
            return 100 - self.best_no_bid_cents, self.best_no_bid_cents

        if self.best_no_ask_cents is not None:
            return 100 - self.best_no_ask_cents, self.best_no_ask_cents

        return None, None
