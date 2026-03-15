from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ...fixed_point import quantize_count
from .events import FillEvent, SettlementEvent
from .orders import SimulatedOrder


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
        self._reserved_cash_by_order: dict[str, Decimal] = {}
        self._reserved_yes_by_order: dict[str, tuple[str, Decimal]] = {}
        self._reserved_no_by_order: dict[str, tuple[str, Decimal]] = {}

    def position(self, market_ticker: str) -> PositionState:
        return self._positions.setdefault(market_ticker, PositionState(market_ticker=market_ticker))

    def positions(self) -> list[PositionState]:
        return sorted(self._positions.values(), key=lambda position: position.market_ticker)

    def seed_position(
        self,
        market_ticker: str,
        *,
        yes_quantity: int | float | str | Decimal = 0,
        no_quantity: int | float | str | Decimal = 0,
        yes_average_cost_cents: int | float | str | Decimal = 0,
        no_average_cost_cents: int | float | str | Decimal = 0,
    ) -> None:
        position = self.position(market_ticker)
        position.yes_quantity = quantize_count(yes_quantity)
        position.no_quantity = quantize_count(no_quantity)
        position.yes_average_cost_cents = _cash(yes_average_cost_cents)
        position.no_average_cost_cents = _cash(no_average_cost_cents)

    @property
    def reserved_cash_cents(self) -> Decimal:
        return sum(self._reserved_cash_by_order.values(), Decimal("0.00")).quantize(CENT)

    @property
    def available_cash_cents(self) -> Decimal:
        return (self.cash_cents - self.reserved_cash_cents).quantize(CENT)

    def reserved_yes_quantity(self, market_ticker: str) -> Decimal:
        total = Decimal("0.00")
        for order_market_ticker, quantity in self._reserved_yes_by_order.values():
            if order_market_ticker == market_ticker:
                total += quantity
        return total.quantize(CENT)

    def reserved_no_quantity(self, market_ticker: str) -> Decimal:
        total = Decimal("0.00")
        for order_market_ticker, quantity in self._reserved_no_by_order.values():
            if order_market_ticker == market_ticker:
                total += quantity
        return total.quantize(CENT)

    def available_yes_quantity(self, market_ticker: str) -> Decimal:
        position = self.position(market_ticker)
        return (position.yes_quantity - self.reserved_yes_quantity(market_ticker)).quantize(CENT)

    def available_no_quantity(self, market_ticker: str) -> Decimal:
        position = self.position(market_ticker)
        return (position.no_quantity - self.reserved_no_quantity(market_ticker)).quantize(CENT)

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

    def reserve_order(
        self,
        order: SimulatedOrder,
        *,
        cash_to_reserve_cents: int | float | str | Decimal = 0,
        yes_quantity_to_reserve: int | float | str | Decimal = 0,
        no_quantity_to_reserve: int | float | str | Decimal = 0,
        cash_per_contract_cents: int | float | str | Decimal = 0,
    ) -> tuple[bool, str | None]:
        cash_to_reserve = _cash(cash_to_reserve_cents)
        yes_to_reserve = quantize_count(yes_quantity_to_reserve)
        no_to_reserve = quantize_count(no_quantity_to_reserve)
        cash_per_contract = _cash(cash_per_contract_cents)

        if cash_to_reserve < 0 or yes_to_reserve < 0 or no_to_reserve < 0:
            return False, "Reservations cannot be negative"
        if self.available_cash_cents < cash_to_reserve:
            return False, "Insufficient available cash to reserve order"
        if yes_to_reserve > 0 and self.available_yes_quantity(order.market_ticker) < yes_to_reserve:
            return False, "Insufficient available yes inventory to reserve order"
        if no_to_reserve > 0 and self.available_no_quantity(order.market_ticker) < no_to_reserve:
            return False, "Insufficient available no inventory to reserve order"

        if cash_to_reserve > 0:
            self._reserved_cash_by_order[order.order_id] = cash_to_reserve
        if yes_to_reserve > 0:
            self._reserved_yes_by_order[order.order_id] = (order.market_ticker, yes_to_reserve)
        if no_to_reserve > 0:
            self._reserved_no_by_order[order.order_id] = (order.market_ticker, no_to_reserve)

        order.reserved_cash_cents = cash_to_reserve
        order.reserved_yes_quantity = yes_to_reserve
        order.reserved_no_quantity = no_to_reserve
        order.reservation_cash_per_contract_cents = cash_per_contract
        return True, None

    def release_order_reservation(self, order: SimulatedOrder) -> None:
        self._reserved_cash_by_order.pop(order.order_id, None)
        self._reserved_yes_by_order.pop(order.order_id, None)
        self._reserved_no_by_order.pop(order.order_id, None)
        order.reserved_cash_cents = Decimal("0.00")
        order.reserved_yes_quantity = Decimal("0.00")
        order.reserved_no_quantity = Decimal("0.00")
        order.reservation_cash_per_contract_cents = Decimal("0.00")

    def consume_order_reservation_on_fill(self, order: SimulatedOrder, fill_quantity: int | float | str | Decimal) -> None:
        quantity = quantize_count(fill_quantity)
        if quantity <= 0:
            return

        if order.reserved_cash_cents > 0:
            if quantity >= order.remaining_quantity:
                released_cash = order.reserved_cash_cents
            else:
                released_cash = (order.reservation_cash_per_contract_cents * quantity).quantize(CENT)
                released_cash = min(released_cash, order.reserved_cash_cents)
            updated_cash = (order.reserved_cash_cents - released_cash).quantize(CENT)
            if updated_cash <= 0:
                self._reserved_cash_by_order.pop(order.order_id, None)
                order.reserved_cash_cents = Decimal("0.00")
                order.reservation_cash_per_contract_cents = Decimal("0.00")
            else:
                self._reserved_cash_by_order[order.order_id] = updated_cash
                order.reserved_cash_cents = updated_cash

        if order.reserved_yes_quantity > 0:
            updated_yes = (order.reserved_yes_quantity - quantity).quantize(CENT)
            if updated_yes <= 0:
                self._reserved_yes_by_order.pop(order.order_id, None)
                order.reserved_yes_quantity = Decimal("0.00")
            else:
                self._reserved_yes_by_order[order.order_id] = (order.market_ticker, updated_yes)
                order.reserved_yes_quantity = updated_yes

        if order.reserved_no_quantity > 0:
            updated_no = (order.reserved_no_quantity - quantity).quantize(CENT)
            if updated_no <= 0:
                self._reserved_no_by_order.pop(order.order_id, None)
                order.reserved_no_quantity = Decimal("0.00")
            else:
                self._reserved_no_by_order[order.order_id] = (order.market_ticker, updated_no)
                order.reserved_no_quantity = updated_no

    def apply_fill(self, fill: FillEvent) -> FillEvent:
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
        position.realized_pnl_cents = position.realized_pnl_cents.quantize(CENT)
        return FillEvent(
            timestamp=fill.timestamp,
            event_type=fill.event_type,
            order_id=fill.order_id,
            market_ticker=fill.market_ticker,
            action=fill.action,
            quantity=fill.quantity,
            price_cents=fill.price_cents,
            fee_cents=fill.fee_cents,
            order_status=fill.order_status,
            liquidity_role=fill.liquidity_role,
            tag=fill.tag,
            note=fill.note,
            cash_after_cents=self.cash_cents,
            yes_position=position.yes_quantity,
            no_position=position.no_quantity,
        )

    def apply_settlement(self, event: SettlementEvent) -> None:
        if event.yes_payout_cents is None or event.no_payout_cents is None:
            return

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
        position.realized_pnl_cents = position.realized_pnl_cents.quantize(CENT)
        self.cash_cents = self.cash_cents.quantize(CENT)
