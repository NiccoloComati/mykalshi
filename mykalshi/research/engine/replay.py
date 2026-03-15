from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

from ...fixed_point import dollars_to_cents, quantize_count
from .events import MarketEvent, OrderbookMarketEvent, SettlementEvent, TickerMarketEvent, TradeMarketEvent


def _event_sort_key(event: MarketEvent) -> tuple[str, str, int]:
    return (
        event.timestamp,
        event.market_ticker,
        int(event.sequence or 0),
    )


def _trade_prices(trade: dict[str, Any]) -> tuple[int, int]:
    yes_price = trade.get("yes_price_dollars")
    no_price = trade.get("no_price_dollars")
    if yes_price is None and trade.get("price") is not None:
        return int(trade["price"]), 100 - int(trade["price"])
    if yes_price is None and no_price is None:
        raise ValueError(f"Trade payload is missing price fields: {trade!r}")
    yes_cents = dollars_to_cents(yes_price) if yes_price is not None else 100 - dollars_to_cents(no_price)
    return yes_cents, 100 - yes_cents


def _trade_quantity(payload: dict[str, Any]) -> Decimal:
    for key in ("count_fp", "count", "size", "quantity"):
        if payload.get(key) is not None:
            return quantize_count(payload[key])
    return Decimal("1.00")


def _level_tuples(levels: Any) -> tuple[tuple[int, Decimal], ...]:
    normalized: list[tuple[int, Decimal]] = []
    for level in levels or []:
        if isinstance(level, dict):
            price = level.get("price_cents")
            size = level.get("count_fp") or level.get("size") or level.get("quantity")
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price, size = level[0], level[1]
        else:
            continue
        if price is None or size is None:
            continue
        normalized.append((int(price), quantize_count(size)))
    return tuple(normalized)


def historical_trade_to_event(trade: dict[str, Any]) -> TradeMarketEvent:
    yes_price_cents, no_price_cents = _trade_prices(trade)
    return TradeMarketEvent(
        timestamp=str(trade.get("created_time") or trade.get("ts") or ""),
        event_type="trade",
        market_ticker=str(trade.get("ticker") or trade.get("market_ticker") or ""),
        sequence=None,
        raw_data=trade,
        yes_price_cents=yes_price_cents,
        no_price_cents=no_price_cents,
        trade_quantity=_trade_quantity(trade),
        trade_id=trade.get("trade_id"),
        taker_side=trade.get("taker_side"),
    )


def market_data_event_to_engine_event(event: dict[str, Any]) -> MarketEvent | None:
    timestamp = str(event.get("event_ts") or event.get("captured_at") or "")
    market_ticker = str(event.get("market_ticker") or "")
    sequence = event.get("sequence")
    raw_data = event.get("raw_message") or event
    channel = event.get("channel")
    event_type = str(event.get("event_type") or channel or "")

    if channel == "trade" or event_type == "trade":
        yes_price_cents = event.get("yes_price_cents")
        no_price_cents = event.get("no_price_cents")
        if yes_price_cents is None and event.get("price_cents") is not None:
            yes_price_cents = int(event["price_cents"])
            no_price_cents = 100 - int(event["price_cents"])
        if yes_price_cents is None or no_price_cents is None:
            return None
        return TradeMarketEvent(
            timestamp=timestamp,
            event_type="trade",
            market_ticker=market_ticker,
            sequence=sequence,
            raw_data=raw_data,
            yes_price_cents=int(yes_price_cents),
            no_price_cents=int(no_price_cents),
            trade_quantity=_trade_quantity(event),
            trade_id=event.get("trade_id"),
            taker_side=event.get("taker_side"),
        )

    if channel == "ticker" or event_type == "ticker":
        return TickerMarketEvent(
            timestamp=timestamp,
            event_type="ticker",
            market_ticker=market_ticker,
            sequence=sequence,
            raw_data=raw_data,
            last_price_cents=event.get("price_cents"),
            yes_bid_cents=event.get("yes_bid_cents"),
            yes_ask_cents=event.get("yes_ask_cents"),
            yes_bid_size=quantize_count(event["yes_bid_size_fp"]) if event.get("yes_bid_size_fp") is not None else None,
            yes_ask_size=quantize_count(event["yes_ask_size_fp"]) if event.get("yes_ask_size_fp") is not None else None,
            volume=quantize_count(event["volume_fp"]) if event.get("volume_fp") is not None else None,
            open_interest=quantize_count(event["open_interest_fp"]) if event.get("open_interest_fp") is not None else None,
        )

    if channel == "orderbook_delta" or event_type in {"orderbook_snapshot", "orderbook_delta"}:
        return OrderbookMarketEvent(
            timestamp=timestamp,
            event_type=event_type,
            market_ticker=market_ticker,
            sequence=sequence,
            raw_data=raw_data,
            best_yes_bid_cents=event.get("best_yes_bid_cents"),
            best_yes_ask_cents=event.get("best_yes_ask_cents"),
            best_no_bid_cents=event.get("best_no_bid_cents"),
            best_no_ask_cents=event.get("best_no_ask_cents"),
            yes_levels=_level_tuples(event.get("yes_levels")),
            no_levels=_level_tuples(event.get("no_levels")),
        )

    if channel == "settlement" or event_type == "settlement":
        return SettlementEvent(
            timestamp=timestamp,
            event_type="settlement",
            market_ticker=market_ticker,
            sequence=sequence,
            raw_data=raw_data,
            yes_payout_cents=event.get("yes_payout_cents"),
            no_payout_cents=event.get("no_payout_cents"),
            reason=event.get("reason"),
        )

    return None


@dataclass
class HistoricalTradeReplay:
    events: list[TradeMarketEvent]

    @classmethod
    def from_trade_dicts(cls, trades: Iterable[dict[str, Any]]) -> "HistoricalTradeReplay":
        events = sorted((historical_trade_to_event(trade) for trade in trades), key=_event_sort_key)
        return cls(events=events)

    def __iter__(self):
        return iter(self.events)


@dataclass
class MarketDataReplay:
    events: list[MarketEvent]

    @classmethod
    def from_market_data_events(cls, events: Iterable[dict[str, Any]]) -> "MarketDataReplay":
        normalized = [event for event in (market_data_event_to_engine_event(item) for item in events) if event is not None]
        return cls(events=sorted(normalized, key=_event_sort_key))

    def __iter__(self):
        return iter(self.events)
