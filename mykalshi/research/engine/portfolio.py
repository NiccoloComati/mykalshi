from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ...fixed_point import quantize_count
from .events import FillEvent, SettlementEvent


CENT = Decimal("0.01")


def _cash(value: int | float | str | Decimal) -> Decimal:
    return quantize_count(value)


@dataclass
class PositionState:
    market_ticker: str
    yes_quantity: Decimal = field(default_factory=lambda: Decimal("0.00"))
    no_quantity: Decimal = field(default_factory=lambda: Decimal("0.00"))
    yes_average_cost_cents: Decimal = field(default_factory=lambda: Decimal("0.00"))
    no_average_cost_cents: Decimal = field(default_factory=lambda: Decimal("0.00"))
    realized_pnl_cents: Decimal = field(default_factory=lambda: Decimal("0.00"))
    last_yes_price_cents: int | None = None
    last_no_price_cents: int | None = None

    @property
    def market_value_cents(self) -> Decimal:
        yes_price = Decimal(self.last_yes_price_cents or 0)
        no_price = Decimal(self.last_no_price_cents or 0)
        return ((self.yes_quantity * yes_price) + (self.no_quantity * no_price)).quantize(CENT)


class PortfolioState:
    def __init__(self, initial_cash_cents: int | float | str | Decimal = 0) -> None:
        self.initial_cash_cents = _cash(initial_cash_cents)
        self.cash_cents = _cash(initial_cash_cents)
        self.total_fees_cents = Decimal("0.00")
        self._positions: dict[str, PositionState] = {}

    def position(self, market_ticker: str) -> PositionState:
        return self._positions.setdefault(market_ticker, PositionState(market_ticker=market_ticker))

    def positions(self) -> list[PositionState]:
        return sorted(self._positions.values(), key=lambda position: position.market_ticker)

    def mark_market(self, market_ticker: str, yes_price_cents: int | None, no_price_cents: int | None) -> None:
        position = self.position(market_ticker)
        position.last_yes_price_cents = yes_price_cents
        position.last_no_price_cents = no_price_cents

    def market_equity_cents(self, market_ticker: str) -> Decimal:
        return self.position(market_ticker).market_value_cents

    def total_equity_cents(self) -> Decimal:
        marked_value = sum((position.market_value_cents for position in self._positions.values()), Decimal("0.00"))
        return (self.cash_cents + marked_value).quantize(CENT)

    def total_realized_pnl_cents(self) -> Decimal:
        return sum((position.realized_pnl_cents for position in self._positions.values()), Decimal("0.00")).quantize(CENT)

    def apply_fill(self, fill: FillEvent) -> None:
        position = self.position(fill.market_ticker)
        quantity = quantize_count(fill.quantity)
        price_cents = Decimal(fill.price_cents)
        fee_cents = _cash(fill.fee_cents)
        action = fill.action.lower()

        if action == "buy_yes":
            prior_cost = position.yes_average_cost_cents * position.yes_quantity
            new_quantity = position.yes_quantity + quantity
            position.yes_average_cost_cents = ((prior_cost + (price_cents * quantity)) / new_quantity).quantize(CENT)
            position.yes_quantity = new_quantity
            self.cash_cents -= (price_cents * quantity) + fee_cents
        elif action == "sell_yes":
            if quantity > position.yes_quantity:
                raise ValueError("Cannot sell more yes contracts than are held")
            realized = ((price_cents - position.yes_average_cost_cents) * quantity) - fee_cents
            position.realized_pnl_cents += realized.quantize(CENT)
            position.yes_quantity -= quantity
            if position.yes_quantity <= 0:
                position.yes_quantity = Decimal("0.00")
                position.yes_average_cost_cents = Decimal("0.00")
            self.cash_cents += (price_cents * quantity) - fee_cents
        elif action == "buy_no":
            prior_cost = position.no_average_cost_cents * position.no_quantity
            new_quantity = position.no_quantity + quantity
            position.no_average_cost_cents = ((prior_cost + (price_cents * quantity)) / new_quantity).quantize(CENT)
            position.no_quantity = new_quantity
            self.cash_cents -= (price_cents * quantity) + fee_cents
        elif action == "sell_no":
            if quantity > position.no_quantity:
                raise ValueError("Cannot sell more no contracts than are held")
            realized = ((price_cents - position.no_average_cost_cents) * quantity) - fee_cents
            position.realized_pnl_cents += realized.quantize(CENT)
            position.no_quantity -= quantity
            if position.no_quantity <= 0:
                position.no_quantity = Decimal("0.00")
                position.no_average_cost_cents = Decimal("0.00")
            self.cash_cents += (price_cents * quantity) - fee_cents
        else:
            raise ValueError(f"Unsupported fill action: {fill.action!r}")

        self.cash_cents = self.cash_cents.quantize(CENT)
        self.total_fees_cents = (self.total_fees_cents + fee_cents).quantize(CENT)

    def apply_settlement(self, event: SettlementEvent) -> None:
        position = self.position(event.market_ticker)
        yes_payout = Decimal(event.yes_payout_cents)
        no_payout = Decimal(event.no_payout_cents)

        if position.yes_quantity > 0:
            realized = ((yes_payout - position.yes_average_cost_cents) * position.yes_quantity).quantize(CENT)
            position.realized_pnl_cents += realized
            self.cash_cents += (yes_payout * position.yes_quantity).quantize(CENT)
        if position.no_quantity > 0:
            realized = ((no_payout - position.no_average_cost_cents) * position.no_quantity).quantize(CENT)
            position.realized_pnl_cents += realized
            self.cash_cents += (no_payout * position.no_quantity).quantize(CENT)

        position.yes_quantity = Decimal("0.00")
        position.no_quantity = Decimal("0.00")
        position.yes_average_cost_cents = Decimal("0.00")
        position.no_average_cost_cents = Decimal("0.00")
        position.last_yes_price_cents = event.yes_payout_cents
        position.last_no_price_cents = event.no_payout_cents
        self.cash_cents = self.cash_cents.quantize(CENT)
