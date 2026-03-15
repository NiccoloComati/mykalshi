from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from ...fixed_point import quantize_count
from .events import MarketEvent, MarketState, TradeMarketEvent, normalize_action
from .orders import SimulatedOrder


@dataclass(frozen=True)
class ExecutionDecision:
    status: str
    quantity: Decimal | None = None
    price_cents: int | None = None
    reason: str | None = None
    liquidity_role: str | None = None


class FillModel(Protocol):
    def evaluate(
        self,
        order: SimulatedOrder,
        market_event: MarketEvent,
        market_state: MarketState,
    ) -> ExecutionDecision | None:
        ...


class ImmediateCompatibilityFillModel:
    """Compatibility model: fill immediately against current executable quote."""

    @staticmethod
    def _clip_price(price_cents: int) -> int:
        return max(0, min(100, price_cents))

    @staticmethod
    def _limit_satisfied(action: str, price_cents: int, limit_price_cents: int | None) -> bool:
        if limit_price_cents is None:
            return True
        if action.startswith("buy"):
            return price_cents <= limit_price_cents
        return price_cents >= limit_price_cents

    def evaluate(
        self,
        order: SimulatedOrder,
        market_event: MarketEvent,
        market_state: MarketState,
    ) -> ExecutionDecision | None:
        action = normalize_action(order.action)
        reference_price_cents, visible_quantity = market_state.executable_quote(action)
        if reference_price_cents is None:
            return None

        slippage = int(order.slippage_cents or 0)
        if action.startswith("buy"):
            execution_price_cents = self._clip_price(reference_price_cents + slippage)
        else:
            execution_price_cents = self._clip_price(reference_price_cents - slippage)

        if order.order_type == "limit" and not self._limit_satisfied(action, execution_price_cents, order.limit_price_cents):
            return None

        quantity_available = quantize_count(visible_quantity or order.remaining_quantity)
        fill_quantity = min(order.remaining_quantity, quantity_available)
        if fill_quantity <= 0:
            return None
        role = "aggressive" if order.order_type == "market" else "passive"
        return ExecutionDecision(status="filled", quantity=fill_quantity, price_cents=execution_price_cents, liquidity_role=role)


class OrderbookAwareFillModel(ImmediateCompatibilityFillModel):
    """Orderbook-aware model with explicit aggressive/passive handling and queue approximation."""

    @staticmethod
    def _resting_price(order: SimulatedOrder, action: str) -> int:
        if order.order_type == "market":
            raise ValueError("Market orders do not have a resting price")
        if order.limit_price_cents is None:
            raise ValueError("Limit orders must provide limit_price_cents")
        return int(order.limit_price_cents) if action.endswith("yes") else 100 - int(order.limit_price_cents)

    @staticmethod
    def _book_level_size(levels: tuple[tuple[int, Decimal], ...], price_cents: int) -> Decimal:
        for level_price, level_size in levels:
            if int(level_price) == int(price_cents):
                return quantize_count(level_size)
        return Decimal("0.00")

    def _displayed_size_for_action(self, market_state: MarketState, action: str, price_cents: int) -> Decimal:
        if action in {"sell_yes", "buy_no"}:
            book_size = self._book_level_size(market_state.yes_levels, price_cents)
            if book_size > 0:
                return book_size
            if action == "sell_yes" and market_state.best_yes_bid_cents == price_cents and market_state.best_yes_bid_size is not None:
                return quantize_count(market_state.best_yes_bid_size)
            if action == "buy_no":
                target_yes_bid = 100 - price_cents
                if market_state.best_yes_bid_cents == target_yes_bid and market_state.best_yes_bid_size is not None:
                    return quantize_count(market_state.best_yes_bid_size)
            return Decimal("0.00")

        book_size = self._book_level_size(market_state.no_levels, price_cents)
        if book_size > 0:
            return book_size
        if action == "buy_yes" and market_state.best_yes_bid_cents == price_cents and market_state.best_yes_bid_size is not None:
            return quantize_count(market_state.best_yes_bid_size)
        if action == "sell_no":
            target_yes_ask = 100 - price_cents
            if market_state.best_yes_ask_cents == target_yes_ask and market_state.best_yes_ask_size is not None:
                return quantize_count(market_state.best_yes_ask_size)
        return Decimal("0.00")

    def _marketable_now(self, action: str, market_state: MarketState, resting_price_cents: int) -> bool:
        best_yes_bid = market_state.best_yes_bid_cents
        best_yes_ask = market_state.best_yes_ask_cents
        best_no_bid = market_state.best_no_bid_cents
        best_no_ask = market_state.best_no_ask_cents
        if action == "buy_yes":
            return best_yes_ask is not None and resting_price_cents >= best_yes_ask
        if action == "sell_yes":
            return best_yes_bid is not None and resting_price_cents <= best_yes_bid
        if action == "buy_no":
            return best_no_ask is not None and resting_price_cents >= best_no_ask
        return best_no_bid is not None and resting_price_cents <= best_no_bid

    def _event_consumption_capacity(self, market_event: MarketEvent, market_state: MarketState, action: str, resting_price_cents: int) -> Decimal:
        return market_state.estimated_queue_consumption(action, resting_price_cents, market_event)


    @staticmethod
    def _has_orderbook_state(market_state: MarketState) -> bool:
        return any(
            value is not None
            for value in (
                market_state.best_yes_bid_cents,
                market_state.best_yes_ask_cents,
                market_state.best_no_bid_cents,
                market_state.best_no_ask_cents,
            )
        ) or bool(market_state.yes_levels or market_state.no_levels)

    def _apply_queue(self, order: SimulatedOrder, *, capacity: Decimal) -> Decimal:
        if capacity <= 0:
            return Decimal("0.00")
        if order.queue_ahead_quantity > 0:
            consumed = min(order.queue_ahead_quantity, capacity)
            order.queue_ahead_quantity = quantize_count(order.queue_ahead_quantity - consumed)
            capacity = quantize_count(capacity - consumed)
        return max(Decimal("0.00"), quantize_count(capacity))

    def evaluate(self, order: SimulatedOrder, market_event: MarketEvent, market_state: MarketState) -> ExecutionDecision | None:
        action = normalize_action(order.action)

        if order.order_type == "market":
            quote_price, visible_quantity = market_state.executable_quote(action)
            if quote_price is None:
                return None
            slippage = int(order.slippage_cents or 0)
            execution_price = self._clip_price(quote_price + slippage if action.startswith("buy") else quote_price - slippage)
            qty = min(order.remaining_quantity, quantize_count(visible_quantity or order.remaining_quantity))
            if qty <= 0:
                return None
            order.liquidity_intent = "aggressive"
            return ExecutionDecision(status="filled", quantity=qty, price_cents=execution_price, liquidity_role="aggressive")

        if order.resting_price_cents is None:
            order.resting_price_cents = self._resting_price(order, action)

        if isinstance(market_event, TradeMarketEvent) and not self._has_orderbook_state(market_state):
            trade_price = market_event.yes_price_cents if action.endswith("yes") else market_event.no_price_cents
            if self._limit_satisfied(action, trade_price, order.limit_price_cents):
                qty = min(order.remaining_quantity, quantize_count(market_event.trade_quantity))
                if qty > 0:
                    order.liquidity_intent = "passive"
                    return ExecutionDecision(status="filled", quantity=qty, price_cents=trade_price, liquidity_role="passive")

        if self._marketable_now(action, market_state, order.resting_price_cents):
            quote_price, visible_quantity = market_state.executable_quote(action)
            if quote_price is None:
                return None
            execution_price = self._clip_price(quote_price)
            if not self._limit_satisfied(action, execution_price, order.limit_price_cents):
                return None
            qty = min(order.remaining_quantity, quantize_count(visible_quantity or order.remaining_quantity))
            if qty <= 0:
                return None
            order.liquidity_intent = "aggressive"
            order.queue_ahead_quantity = Decimal("0.00")
            return ExecutionDecision(status="filled", quantity=qty, price_cents=execution_price, liquidity_role="aggressive")

        if order.liquidity_intent is None:
            order.liquidity_intent = "passive"
            displayed = self._displayed_size_for_action(market_state, action, order.resting_price_cents)
            order.queue_ahead_quantity = quantize_count(displayed)

        capacity = self._event_consumption_capacity(market_event, market_state, action, order.resting_price_cents)
        executable_capacity = self._apply_queue(order, capacity=capacity)
        if executable_capacity <= 0:
            return None

        fill_qty = min(order.remaining_quantity, executable_capacity)
        if fill_qty <= 0:
            return None
        return ExecutionDecision(
            status="filled",
            quantity=fill_qty,
            price_cents=order.resting_price_cents,
            liquidity_role="passive",
        )


class KalshiBinaryFillModel(OrderbookAwareFillModel):
    """Default fill model for Kalshi binary contracts."""
