from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Callable

from ...fixed_point import quantize_count
from .events import (
    CancelRequest,
    FillEvent,
    MarketEvent,
    MarketState,
    MarkEvent,
    OrderEvent,
    OrderRequest,
    OrderbookMarketEvent,
    SettlementEvent,
    TickerMarketEvent,
    TradeMarketEvent,
)

if TYPE_CHECKING:
    from .orders import SimulatedOrder
    from .portfolio import PortfolioState, PositionState


@dataclass
class StrategyContext:
    portfolio: "PortfolioState"
    market_states: dict[str, MarketState]
    open_order_provider: Callable[[str | None], list["SimulatedOrder"]]
    request_sink: list[OrderRequest | CancelRequest]
    log_sink: list[dict[str, str]]
    current_event: MarketEvent | None = None

    def set_current_event(self, event: MarketEvent) -> None:
        self.current_event = event

    def market(self, market_ticker: str) -> MarketState:
        return self.market_states.setdefault(market_ticker, MarketState(market_ticker=market_ticker))

    def position(self, market_ticker: str) -> "PositionState":
        return self.portfolio.position(market_ticker)

    def open_orders(self, market_ticker: str | None = None) -> list["SimulatedOrder"]:
        return self.open_order_provider(market_ticker)

    def log(self, message: str, *, level: str = "INFO") -> None:
        timestamp = self.current_event.timestamp if self.current_event is not None else ""
        self.log_sink.append({"timestamp": timestamp, "level": level, "message": message})

    def submit_order(
        self,
        market_ticker: str,
        *,
        action: str,
        quantity: int | float | str | Decimal,
        limit_price_cents: int | None = None,
        slippage_cents: int = 0,
        latency_events: int = 0,
        tag: str | None = None,
        note: str | None = None,
    ) -> OrderRequest:
        if self.current_event is None:
            raise RuntimeError("Strategies can only submit orders while the engine is processing an event")
        request = OrderRequest(
            timestamp=self.current_event.timestamp,
            event_type="order_request",
            market_ticker=market_ticker,
            action=action,
            quantity=quantize_count(quantity),
            order_type="limit" if limit_price_cents is not None else "market",
            limit_price_cents=limit_price_cents,
            slippage_cents=slippage_cents,
            latency_events=int(latency_events or 0),
            tag=tag,
            note=note,
        )
        self.request_sink.append(request)
        return request

    def cancel(self, order_id: str) -> CancelRequest:
        if self.current_event is None:
            raise RuntimeError("Strategies can only cancel orders while the engine is processing an event")
        request = CancelRequest(
            timestamp=self.current_event.timestamp,
            event_type="cancel_request",
            order_id=order_id,
        )
        self.request_sink.append(request)
        return request

    def buy_yes(self, market_ticker: str, quantity=1, **kwargs) -> OrderRequest:
        return self.submit_order(market_ticker, action="buy_yes", quantity=quantity, **kwargs)

    def sell_yes(self, market_ticker: str, quantity=1, **kwargs) -> OrderRequest:
        return self.submit_order(market_ticker, action="sell_yes", quantity=quantity, **kwargs)

    def buy_no(self, market_ticker: str, quantity=1, **kwargs) -> OrderRequest:
        return self.submit_order(market_ticker, action="buy_no", quantity=quantity, **kwargs)

    def sell_no(self, market_ticker: str, quantity=1, **kwargs) -> OrderRequest:
        return self.submit_order(market_ticker, action="sell_no", quantity=quantity, **kwargs)


class KalshiStrategy:
    """Simple Backtrader-style user surface over the event-driven core."""

    def on_start(self, context: StrategyContext) -> None:
        return None

    def on_event(self, context: StrategyContext, event: MarketEvent) -> None:
        return None

    def on_trade(self, context: StrategyContext, event: TradeMarketEvent) -> None:
        return None

    def on_ticker(self, context: StrategyContext, event: TickerMarketEvent) -> None:
        return None

    def on_orderbook(self, context: StrategyContext, event: OrderbookMarketEvent) -> None:
        return None

    def on_order(self, context: StrategyContext, event: OrderEvent) -> None:
        return None

    def on_fill(self, context: StrategyContext, event: FillEvent) -> None:
        return None

    def on_mark(self, context: StrategyContext, event: MarkEvent) -> None:
        return None

    def on_settlement(self, context: StrategyContext, event: SettlementEvent) -> None:
        return None

    def on_finish(self, context: StrategyContext) -> None:
        return None
