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
    OrderbookMarketEvent,
    SettlementEvent,
    TickerMarketEvent,
    TradeMarketEvent,
)
from .execution import ExecutionDecision, KalshiBinaryFillModel
from .orders import OrderManager, SimulatedOrder
from .portfolio import PortfolioState
from .reporting import BacktestRunResult, PerformanceTracker
from .strategy import KalshiStrategy, StrategyContext


class EventDrivenBacktestEngine:
    """Deterministic event loop for Kalshi-style replayed backtests."""

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

    @staticmethod
    def _validate_order_request(request: OrderRequest) -> str | None:
        if quantize_count(request.quantity) <= 0:
            return "Order quantity must be positive"
        if request.order_type not in {"market", "limit"}:
            return f"Unsupported order type: {request.order_type!r}"
        if request.limit_price_cents is not None and not 0 <= int(request.limit_price_cents) <= 100:
            return "Limit price must be between 0 and 100 cents"
        return None

    def _reservation_price_cents(self, order: SimulatedOrder, market_state: MarketState) -> int:
        if order.limit_price_cents is not None:
            return int(order.limit_price_cents)
        executable_price_cents, _ = market_state.executable_quote(order.action)
        if executable_price_cents is None:
            return 100
        price = executable_price_cents
        if order.action.startswith("buy"):
            price += int(order.slippage_cents or 0)
        return max(0, min(100, price))

    def _reservation_fee_cents(self, order: SimulatedOrder, market_event: MarketEvent, reservation_price_cents: int) -> Decimal:
        quantity = quantize_count(order.quantity)
        fee_at_reference = self._call_fee_model(
            self.fee_model,
            order,
            market_event,
            reservation_price_cents,
            quantity,
        )
        fee_at_mid = self._call_fee_model(
            self.fee_model,
            order,
            market_event,
            50,
            quantity,
        )
        return max(fee_at_reference, fee_at_mid).quantize(Decimal("0.01"))

    def _reserve_order(
        self,
        order: SimulatedOrder,
        *,
        market_event: MarketEvent,
        market_state: MarketState,
        portfolio: PortfolioState,
    ) -> tuple[bool, str | None]:
        action = order.action.lower()
        quantity = quantize_count(order.quantity)
        if action in {"buy_yes", "buy_no"}:
            reservation_price_cents = self._reservation_price_cents(order, market_state)
            reservation_fee_cents = self._reservation_fee_cents(order, market_event, reservation_price_cents)
            cash_to_reserve = ((Decimal(reservation_price_cents) * quantity) + reservation_fee_cents).quantize(Decimal("0.01"))
            cash_per_contract = (cash_to_reserve / quantity).quantize(Decimal("0.01"))
            return portfolio.reserve_order(
                order,
                cash_to_reserve_cents=cash_to_reserve,
                cash_per_contract_cents=cash_per_contract,
            )
        if action == "sell_yes":
            return portfolio.reserve_order(order, yes_quantity_to_reserve=quantity)
        return portfolio.reserve_order(order, no_quantity_to_reserve=quantity)

    def _record_order_event(
        self,
        event: OrderEvent,
        *,
        tracker: PerformanceTracker,
        strategy: KalshiStrategy,
        context: StrategyContext,
    ) -> None:
        tracker.record_order_event(event)
        strategy.on_order(context, event)

    def _process_submit_request(
        self,
        request: OrderRequest,
        *,
        market_state: MarketState,
        order_manager: OrderManager,
        portfolio: PortfolioState,
        tracker: PerformanceTracker,
        strategy: KalshiStrategy,
        context: StrategyContext,
        current_event: MarketEvent,
    ) -> None:
        order = order_manager.create(request)
        validation_error = self._validate_order_request(request)
        if validation_error is not None:
            self._record_order_event(
                order_manager.reject(order.order_id, timestamp=request.timestamp, reason=validation_error),
                tracker=tracker,
                strategy=strategy,
                context=context,
            )
            return
        if market_state.settled or market_state.settlement_pending:
            self._record_order_event(
                order_manager.reject(order.order_id, timestamp=request.timestamp, reason="Cannot submit orders to a settled market"),
                tracker=tracker,
                strategy=strategy,
                context=context,
            )
            return

        reserved, reason = self._reserve_order(
            order,
            market_event=current_event,
            market_state=market_state,
            portfolio=portfolio,
        )
        if not reserved:
            self._record_order_event(
                order_manager.reject(order.order_id, timestamp=request.timestamp, reason=str(reason)),
                tracker=tracker,
                strategy=strategy,
                context=context,
            )
            return

        self._record_order_event(
            order_manager.accept(order.order_id, timestamp=request.timestamp),
            tracker=tracker,
            strategy=strategy,
            context=context,
        )

    def _process_requests(
        self,
        pending_requests: list[OrderRequest | CancelRequest],
        *,
        current_event: MarketEvent,
        market_states: dict[str, MarketState],
        order_manager: OrderManager,
        portfolio: PortfolioState,
        tracker: PerformanceTracker,
        strategy: KalshiStrategy,
        context: StrategyContext,
    ) -> bool:
        processed_any = False
        while pending_requests:
            processed_any = True
            request = pending_requests.pop(0)
            if isinstance(request, CancelRequest):
                order = order_manager.get(request.order_id)
                if order is not None and order.is_open:
                    portfolio.release_order_reservation(order)
                order_event = order_manager.cancel(request)
                self._record_order_event(order_event, tracker=tracker, strategy=strategy, context=context)
                continue

            market_state = market_states.setdefault(
                request.market_ticker,
                MarketState(market_ticker=request.market_ticker),
            )
            self._process_submit_request(
                request,
                market_state=market_state,
                order_manager=order_manager,
                portfolio=portfolio,
                tracker=tracker,
                strategy=strategy,
                context=context,
                current_event=current_event,
            )
        return processed_any

    def _can_apply_fill(
        self,
        order: SimulatedOrder,
        *,
        quantity: Decimal,
        price_cents: int,
        fee_cents: Decimal,
    ) -> tuple[bool, str | None]:
        total_cost_cents = (Decimal(price_cents) * quantity + fee_cents).quantize(Decimal("0.01"))
        action = order.action.lower()
        if action in {"buy_yes", "buy_no"}:
            if total_cost_cents > order.reserved_cash_cents:
                return False, "Reserved cash is insufficient for this fill"
            return True, None
        if action == "sell_yes" and quantity > order.reserved_yes_quantity:
            return False, "Reserved yes inventory is insufficient for this fill"
        if action == "sell_no" and quantity > order.reserved_no_quantity:
            return False, "Reserved no inventory is insufficient for this fill"
        return True, None

    def _process_fills(
        self,
        market_event: MarketEvent,
        *,
        market_state: MarketState,
        processed_order_ids: set[str],
        portfolio: PortfolioState,
        order_manager: OrderManager,
        tracker: PerformanceTracker,
        strategy: KalshiStrategy,
        context: StrategyContext,
    ) -> bool:
        filled_any = False
        for order in list(order_manager.open_orders(market_event.market_ticker)):
            if order.order_id in processed_order_ids:
                continue
            processed_order_ids.add(order.order_id)
            decision: ExecutionDecision | None = self.fill_model.evaluate(order, market_event, market_state)
            if decision is None:
                continue
            if decision.status != "filled" or decision.quantity is None or decision.price_cents is None:
                filled_any = True
                portfolio.release_order_reservation(order)
                self._record_order_event(
                    order_manager.reject(
                        order.order_id,
                        timestamp=market_event.timestamp,
                        reason=decision.reason or "Order was rejected by the fill model",
                    ),
                    tracker=tracker,
                    strategy=strategy,
                    context=context,
                )
                continue

            fee_cents = self._call_fee_model(
                self.fee_model,
                order,
                market_event,
                decision.price_cents,
                decision.quantity,
            )
            can_fill, rejection_reason = self._can_apply_fill(
                order,
                quantity=decision.quantity,
                price_cents=decision.price_cents,
                fee_cents=fee_cents,
            )
            if not can_fill:
                filled_any = True
                portfolio.release_order_reservation(order)
                self._record_order_event(
                    order_manager.reject(order.order_id, timestamp=market_event.timestamp, reason=str(rejection_reason)),
                    tracker=tracker,
                    strategy=strategy,
                    context=context,
                )
                continue

            filled_any = True
            portfolio.consume_order_reservation_on_fill(order, decision.quantity)
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
            applied_fill = portfolio.apply_fill(fill_event)
            tracker.record_fill(applied_fill)
            self._record_order_event(order_event, tracker=tracker, strategy=strategy, context=context)
            strategy.on_fill(context, applied_fill)
        return filled_any

    def _expire_open_market_orders(
        self,
        market_event: MarketEvent,
        *,
        order_manager: OrderManager,
        portfolio: PortfolioState,
        tracker: PerformanceTracker,
        strategy: KalshiStrategy,
        context: StrategyContext,
    ) -> None:
        for order_event in order_manager.cancel_open_market_orders(
            market_event.market_ticker,
            timestamp=market_event.timestamp,
            reason="Unfilled market order expired at end of step",
        ):
            order = order_manager.get(order_event.order_id)
            if order is not None:
                portfolio.release_order_reservation(order)
            self._record_order_event(order_event, tracker=tracker, strategy=strategy, context=context)

    def _process_settlement_event(
        self,
        event: SettlementEvent,
        *,
        order_manager: OrderManager,
        portfolio: PortfolioState,
        tracker: PerformanceTracker,
        strategy: KalshiStrategy,
        context: StrategyContext,
    ) -> None:
        for order_event in order_manager.cancel_open_orders_for_market(
            event.market_ticker,
            timestamp=event.timestamp,
            reason="Market settled",
            status="canceled",
        ):
            order = order_manager.get(order_event.order_id)
            if order is not None:
                portfolio.release_order_reservation(order)
            self._record_order_event(order_event, tracker=tracker, strategy=strategy, context=context)

        if event.yes_payout_cents is None or event.no_payout_cents is None:
            tracker.logs.append(
                {
                    "timestamp": event.timestamp,
                    "level": "WARNING",
                    "message": f"Settlement data missing for {event.market_ticker}; positions remain open until payout arrives.",
                }
            )
            return

        portfolio.apply_settlement(event)

    def run(
        self,
        replay: Iterable[MarketEvent],
        strategy: KalshiStrategy,
        *,
        initial_cash_cents: int | float | str | Decimal | None = None,
        initial_positions: dict[str, dict[str, int | float | str | Decimal]] | None = None,
    ) -> BacktestRunResult:
        portfolio = PortfolioState(initial_cash_cents=self.initial_cash_cents if initial_cash_cents is None else initial_cash_cents)
        for market_ticker, payload in (initial_positions or {}).items():
            portfolio.seed_position(
                market_ticker,
                yes_quantity=payload.get("yes_quantity", 0),
                no_quantity=payload.get("no_quantity", 0),
                yes_average_cost_cents=payload.get("yes_average_cost_cents", 0),
                no_average_cost_cents=payload.get("no_average_cost_cents", 0),
            )
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
                self._process_settlement_event(
                    market_event,
                    order_manager=order_manager,
                    portfolio=portfolio,
                    tracker=tracker,
                    strategy=strategy,
                    context=context,
                )

            self._dispatch_market_event(strategy, context, market_event)
            self._process_requests(
                pending_requests,
                current_event=market_event,
                market_states=market_states,
                order_manager=order_manager,
                portfolio=portfolio,
                tracker=tracker,
                strategy=strategy,
                context=context,
            )

            if not isinstance(market_event, SettlementEvent):
                step_iterations = 0
                processed_order_ids: set[str] = set()
                while True:
                    step_iterations += 1
                    if step_iterations > 1000:
                        raise RuntimeError(
                            f"Exceeded per-event processing limit while handling {market_event.market_ticker} at {market_event.timestamp}"
                        )

                    progressed = self._process_fills(
                        market_event,
                        market_state=market_state,
                        processed_order_ids=processed_order_ids,
                        portfolio=portfolio,
                        order_manager=order_manager,
                        tracker=tracker,
                        strategy=strategy,
                        context=context,
                    )
                    progressed = (
                        self._process_requests(
                            pending_requests,
                            current_event=market_event,
                            market_states=market_states,
                            order_manager=order_manager,
                            portfolio=portfolio,
                            tracker=tracker,
                            strategy=strategy,
                            context=context,
                        )
                        or progressed
                    )
                    if not progressed:
                        break
                self._expire_open_market_orders(
                    market_event,
                    order_manager=order_manager,
                    portfolio=portfolio,
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
