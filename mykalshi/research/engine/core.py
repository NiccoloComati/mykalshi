from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable

from ...fixed_point import quantize_count
from .events import (
    CancelRequest,
    FillEvent,
    MarketEvent,
    MarkEvent,
    MarketState,
    OrderEvent,
    OrderRequest,
    SettlementEvent,
    TickerMarketEvent,
    TradeMarketEvent,
    OrderbookMarketEvent,
)
from .execution import ExecutionDecision, KalshiBinaryFillModel
from .orders import OrderManager, SimulatedOrder
from .portfolio import PortfolioState
from .reporting import BacktestRunResult, PerformanceTracker
from .strategy import KalshiStrategy, StrategyContext


class EventDrivenBacktestEngine:
    """Deterministic event loop for Kalshi-style replayed backtests.

    The engine is deliberately modular:
    - replay feeds ordered MarketEvent objects
    - strategy reacts to explicit events through StrategyContext
    - order manager owns order state transitions
    - fill model decides whether current market state can execute open orders
    - portfolio owns cash, positions, and valuation
    - performance tracker owns outputs and event logs
    """

    def __init__(
        self,
        *,
        initial_cash_cents: int | float | str | Decimal = 0,
        fill_model: Any | None = None,
        fee_model: Any | None = None,
    ) -> None:
        self.initial_cash_cents = quantize_count(initial_cash_cents)
        self.fill_model = fill_model or KalshiBinaryFillModel()
        self.fee_model = fee_model

    @staticmethod
    def _call_fee_model(
        fee_model: Any | None,
        order: SimulatedOrder,
        market_event: MarketEvent,
        execution_price_cents: int,
        quantity: Decimal,
    ) -> Decimal:
        if fee_model is None:
            return Decimal("0.00")
        try:
            return quantize_count(fee_model(order, market_event, execution_price_cents, quantity))
        except TypeError:
            return quantize_count(fee_model(order, market_event, execution_price_cents))

    @staticmethod
    def _dispatch_market_event(strategy: KalshiStrategy, context: StrategyContext, event: MarketEvent) -> None:
        strategy.on_event(context, event)
        if isinstance(event, TradeMarketEvent):
            strategy.on_trade(context, event)
        elif isinstance(event, TickerMarketEvent):
            strategy.on_ticker(context, event)
        elif isinstance(event, OrderbookMarketEvent):
            strategy.on_orderbook(context, event)
        elif isinstance(event, SettlementEvent):
            strategy.on_settlement(context, event)

    @staticmethod
    def _mark_event(timestamp: str, market_ticker: str, portfolio: PortfolioState, market_state: MarketState) -> MarkEvent | None:
        yes_price_cents, no_price_cents = market_state.mark_prices()
        if yes_price_cents is None and no_price_cents is None:
            return None
        portfolio.mark_market(market_ticker, yes_price_cents, no_price_cents)
        return MarkEvent(
            timestamp=timestamp,
            event_type="mark",
            market_ticker=market_ticker,
            yes_price_cents=yes_price_cents,
            no_price_cents=no_price_cents,
            market_equity_cents=portfolio.market_equity_cents(market_ticker),
            total_equity_cents=portfolio.total_equity_cents(),
            cash_cents=portfolio.cash_cents,
        )

    def _process_requests(
        self,
        pending_requests: list[OrderRequest | CancelRequest],
        *,
        order_manager: OrderManager,
        tracker: PerformanceTracker,
        strategy: KalshiStrategy,
        context: StrategyContext,
    ) -> None:
        while pending_requests:
            request = pending_requests.pop(0)
            if isinstance(request, CancelRequest):
                order_event = order_manager.cancel(request)
            else:
                order_event = order_manager.submit(request)
            tracker.record_order_event(order_event)
            strategy.on_order(context, order_event)

    def _can_apply_fill(
        self,
        portfolio: PortfolioState,
        order: SimulatedOrder,
        *,
        quantity: Decimal,
        price_cents: int,
        fee_cents: Decimal,
    ) -> tuple[bool, str | None]:
        position = portfolio.position(order.market_ticker)
        notional = Decimal(price_cents) * quantity
        action = order.action.lower()
        if action == "buy_yes" or action == "buy_no":
            if (notional + fee_cents) > portfolio.cash_cents:
                return False, "Insufficient cash to execute fill"
            return True, None
        if action == "sell_yes" and quantity > position.yes_quantity:
            return False, "Insufficient yes position to execute fill"
        if action == "sell_no" and quantity > position.no_quantity:
            return False, "Insufficient no position to execute fill"
        return True, None

    def _process_fills(
        self,
        market_event: MarketEvent,
        *,
        market_state: MarketState,
        portfolio: PortfolioState,
        order_manager: OrderManager,
        tracker: PerformanceTracker,
        strategy: KalshiStrategy,
        context: StrategyContext,
    ) -> None:
        for order in list(order_manager.open_orders(market_event.market_ticker)):
            decision: ExecutionDecision | None = self.fill_model.evaluate(order, market_event, market_state)
            if decision is None:
                continue

            fee_cents = self._call_fee_model(
                self.fee_model,
                order,
                market_event,
                decision.price_cents,
                decision.quantity,
            )
            can_fill, rejection_reason = self._can_apply_fill(
                portfolio,
                order,
                quantity=decision.quantity,
                price_cents=decision.price_cents,
                fee_cents=fee_cents,
            )
            if not can_fill:
                order.status = "rejected"
                order.reason = rejection_reason
                order.updated_at = market_event.timestamp
                rejection_event = OrderEvent(
                    timestamp=market_event.timestamp,
                    event_type="order",
                    order_id=order.order_id,
                    market_ticker=order.market_ticker,
                    action=order.action,
                    status=order.status,
                    quantity=order.quantity,
                    remaining_quantity=order.remaining_quantity,
                    order_type=order.order_type,
                    limit_price_cents=order.limit_price_cents,
                    average_fill_price_cents=None,
                    reason=order.reason,
                    tag=order.tag,
                    note=order.note,
                )
                tracker.record_order_event(rejection_event)
                strategy.on_order(context, rejection_event)
                continue

            order_event = order_manager.record_fill(
                order.order_id,
                quantity=decision.quantity,
                price_cents=decision.price_cents,
                timestamp=market_event.timestamp,
            )
            fill_event = FillEvent(
                timestamp=market_event.timestamp,
                event_type="fill",
                order_id=order.order_id,
                market_ticker=order.market_ticker,
                action=order.action,
                quantity=decision.quantity,
                price_cents=decision.price_cents,
                fee_cents=fee_cents,
                order_status=order_event.status,
                tag=order.tag,
                note=order.note,
            )
            portfolio.apply_fill(fill_event)
            tracker.record_fill(fill_event)
            tracker.record_order_event(order_event)
            strategy.on_fill(context, fill_event)
            strategy.on_order(context, order_event)

    def run(
        self,
        replay: Iterable[MarketEvent],
        strategy: KalshiStrategy,
        *,
        initial_cash_cents: int | float | str | Decimal | None = None,
    ) -> BacktestRunResult:
        portfolio = PortfolioState(initial_cash_cents=self.initial_cash_cents if initial_cash_cents is None else initial_cash_cents)
        order_manager = OrderManager()
        tracker = PerformanceTracker()
        market_states: dict[str, MarketState] = {}
        pending_requests: list[OrderRequest | CancelRequest] = []
        context = StrategyContext(
            portfolio=portfolio,
            market_states=market_states,
            open_order_provider=order_manager.open_orders,
            request_sink=pending_requests,
            log_sink=tracker.logs,
        )

        strategy.on_start(context)
        for market_event in replay:
            tracker.record_event(market_event)
            context.set_current_event(market_event)
            market_state = market_states.setdefault(
                market_event.market_ticker,
                MarketState(market_ticker=market_event.market_ticker),
            )
            market_state.update(market_event)
            if isinstance(market_event, SettlementEvent):
                portfolio.apply_settlement(market_event)

            self._dispatch_market_event(strategy, context, market_event)
            self._process_requests(
                pending_requests,
                order_manager=order_manager,
                tracker=tracker,
                strategy=strategy,
                context=context,
            )
            self._process_fills(
                market_event,
                market_state=market_state,
                portfolio=portfolio,
                order_manager=order_manager,
                tracker=tracker,
                strategy=strategy,
                context=context,
            )
            self._process_requests(
                pending_requests,
                order_manager=order_manager,
                tracker=tracker,
                strategy=strategy,
                context=context,
            )

            mark_event = self._mark_event(market_event.timestamp, market_event.market_ticker, portfolio, market_state)
            if mark_event is not None:
                tracker.record_mark(mark_event)
                strategy.on_mark(context, mark_event)

        strategy.on_finish(context)
        return tracker.build_result(portfolio, order_manager)
