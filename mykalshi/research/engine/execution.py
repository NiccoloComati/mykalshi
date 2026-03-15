from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from ...fixed_point import quantize_count
from .events import MarketEvent, MarketState, normalize_action
from .orders import SimulatedOrder


@dataclass(frozen=True)
class ExecutionDecision:
    quantity: Decimal
    price_cents: int


class FillModel(Protocol):
    def evaluate(
        self,
        order: SimulatedOrder,
        market_event: MarketEvent,
        market_state: MarketState,
    ) -> ExecutionDecision | None:
        ...


class KalshiBinaryFillModel:
    """Simple microstructure-aware fill model for Kalshi-style binary contracts.

    First pass behavior:
    - trade events can consume up to the reported trade size
    - orderbook and ticker events can consume up to top-of-book visible size when present
    - marketable limit checks and slippage are applied deterministically
    """

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
        return ExecutionDecision(quantity=fill_quantity, price_cents=execution_price_cents)
