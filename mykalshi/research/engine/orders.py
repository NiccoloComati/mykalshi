from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP

from ...fixed_point import quantize_count
from .events import CancelRequest, OrderEvent, OrderRequest, normalize_action


OPEN_ORDER_STATUSES = {"accepted", "partially_filled"}


@dataclass
class SimulatedOrder:
    order_id: str
    market_ticker: str
    action: str
    quantity: Decimal
    remaining_quantity: Decimal
    order_type: str
    created_at: str
    updated_at: str
    limit_price_cents: int | None = None
    slippage_cents: int = 0
    status: str = "pending"
    filled_quantity: Decimal = field(default_factory=lambda: Decimal("0.00"))
    average_fill_price_cents: Decimal | None = None
    reason: str | None = None
    tag: str | None = None
    note: str | None = None
    reserved_cash_cents: Decimal = Decimal("0.00")
    reserved_yes_quantity: Decimal = Decimal("0.00")
    reserved_no_quantity: Decimal = Decimal("0.00")
    reservation_cash_per_contract_cents: Decimal = Decimal("0.00")
    replacement_for_order_id: str | None = None
    liquidity_intent: str | None = None
    resting_price_cents: int | None = None
    queue_ahead_quantity: Decimal = Decimal("0.00")
    latency_events: int = 0
    remaining_latency_events: int = 0

    @property
    def is_open(self) -> bool:
        return self.status in OPEN_ORDER_STATUSES and self.remaining_quantity > 0


class OrderManager:
    def __init__(self) -> None:
        self._next_id = 1
        self._orders: dict[str, SimulatedOrder] = {}

    @staticmethod
    def _average_fill_price_cents(order: SimulatedOrder) -> int | None:
        if order.average_fill_price_cents is None:
            return None
        return int(order.average_fill_price_cents.quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    def _build_order_event(self, order: SimulatedOrder) -> OrderEvent:
        return OrderEvent(
            timestamp=order.updated_at,
            event_type="order",
            order_id=order.order_id,
            market_ticker=order.market_ticker,
            action=order.action,
            status=order.status,
            quantity=order.quantity,
            remaining_quantity=order.remaining_quantity,
            order_type=order.order_type,
            limit_price_cents=order.limit_price_cents,
            average_fill_price_cents=self._average_fill_price_cents(order),
            reason=order.reason,
            tag=order.tag,
            note=order.note,
            reserved_cash_cents=order.reserved_cash_cents,
            reserved_yes_quantity=order.reserved_yes_quantity,
            reserved_no_quantity=order.reserved_no_quantity,
            liquidity_intent=order.liquidity_intent,
            resting_price_cents=order.resting_price_cents,
            queue_ahead_quantity=order.queue_ahead_quantity,
            remaining_latency_events=order.remaining_latency_events,
        )

    def create(self, request: OrderRequest) -> SimulatedOrder:
        action = normalize_action(request.action)
        quantity = quantize_count(request.quantity)
        order_id = f"sim-{self._next_id}"
        self._next_id += 1

        order = SimulatedOrder(
            order_id=order_id,
            market_ticker=request.market_ticker,
            action=action,
            quantity=quantity,
            remaining_quantity=quantity,
            order_type=request.order_type,
            created_at=request.timestamp,
            updated_at=request.timestamp,
            limit_price_cents=request.limit_price_cents,
            slippage_cents=int(request.slippage_cents or 0),
            tag=request.tag,
            note=request.note,
            replacement_for_order_id=getattr(request, "replacement_for_order_id", None),
            latency_events=max(0, int(getattr(request, "latency_events", 0) or 0)),
            remaining_latency_events=max(0, int(getattr(request, "latency_events", 0) or 0)),
        )
        self._orders[order_id] = order
        return order

    def reject(self, order_id: str, *, timestamp: str, reason: str) -> OrderEvent:
        order = self._orders[order_id]
        order.updated_at = timestamp
        order.status = "rejected"
        order.reason = reason
        order.remaining_quantity = Decimal("0.00")
        order.reserved_cash_cents = Decimal("0.00")
        order.reserved_yes_quantity = Decimal("0.00")
        order.reserved_no_quantity = Decimal("0.00")
        order.reservation_cash_per_contract_cents = Decimal("0.00")
        order.queue_ahead_quantity = Decimal("0.00")
        return self._build_order_event(order)

    def accept(self, order_id: str, *, timestamp: str) -> OrderEvent:
        order = self._orders[order_id]
        order.updated_at = timestamp
        order.status = "accepted"
        order.reason = None
        return self._build_order_event(order)

    def cancel(
        self,
        request: CancelRequest,
        *,
        reason: str | None = None,
        status: str = "canceled",
    ) -> OrderEvent:
        order = self._orders.get(request.order_id)
        if order is None:
            placeholder = SimulatedOrder(
                order_id=request.order_id,
                market_ticker="",
                action="buy_yes",
                quantity=Decimal("0.00"),
                remaining_quantity=Decimal("0.00"),
                order_type="market",
                created_at=request.timestamp,
                updated_at=request.timestamp,
                status="rejected",
                reason="Unknown order id",
            )
            return self._build_order_event(placeholder)

        order.updated_at = request.timestamp
        if not order.is_open:
            original_reason = order.reason
            event = OrderEvent(
                timestamp=request.timestamp,
                event_type="order",
                order_id=order.order_id,
                market_ticker=order.market_ticker,
                action=order.action,
                status=order.status,
                quantity=order.quantity,
                remaining_quantity=order.remaining_quantity,
                order_type=order.order_type,
                limit_price_cents=order.limit_price_cents,
                average_fill_price_cents=self._average_fill_price_cents(order),
                reason="Order is not cancelable in its current state",
                tag=order.tag,
                note=order.note,
                reserved_cash_cents=order.reserved_cash_cents,
                reserved_yes_quantity=order.reserved_yes_quantity,
                reserved_no_quantity=order.reserved_no_quantity,
                liquidity_intent=order.liquidity_intent,
                resting_price_cents=order.resting_price_cents,
                queue_ahead_quantity=order.queue_ahead_quantity,
            )
            order.reason = original_reason
            return event

        order.status = status
        order.reason = reason
        order.reserved_cash_cents = Decimal("0.00")
        order.reserved_yes_quantity = Decimal("0.00")
        order.reserved_no_quantity = Decimal("0.00")
        order.reservation_cash_per_contract_cents = Decimal("0.00")
        order.queue_ahead_quantity = Decimal("0.00")
        return self._build_order_event(order)

    def cancel_open_orders_for_market(
        self,
        market_ticker: str,
        *,
        timestamp: str,
        reason: str,
        status: str = "canceled",
    ) -> list[OrderEvent]:
        events: list[OrderEvent] = []
        for order in self.open_orders(market_ticker):
            events.append(
                self.cancel(
                    CancelRequest(timestamp=timestamp, event_type="cancel_request", order_id=order.order_id),
                    reason=reason,
                    status=status,
                )
            )
        return events

    def cancel_open_market_orders(
        self,
        market_ticker: str,
        *,
        timestamp: str,
        reason: str,
    ) -> list[OrderEvent]:
        events: list[OrderEvent] = []
        for order in self.open_orders(market_ticker):
            if order.order_type != "market":
                continue
            events.append(
                self.cancel(
                    CancelRequest(timestamp=timestamp, event_type="cancel_request", order_id=order.order_id),
                    reason=reason,
                    status="expired",
                )
            )
        return events

    def record_fill(
        self,
        order_id: str,
        *,
        quantity: Decimal,
        price_cents: int,
        timestamp: str,
    ) -> OrderEvent:
        order = self._orders[order_id]
        fill_quantity = quantize_count(quantity)
        if fill_quantity <= 0:
            raise ValueError("Fill quantity must be positive")
        if fill_quantity > order.remaining_quantity:
            raise ValueError("Fill quantity cannot exceed remaining order quantity")

        prior_filled = order.filled_quantity
        new_filled = prior_filled + fill_quantity
        total_notional = Decimal("0.00")
        if order.average_fill_price_cents is not None:
            total_notional += order.average_fill_price_cents * prior_filled
        total_notional += Decimal(price_cents) * fill_quantity

        order.filled_quantity = new_filled
        order.remaining_quantity = quantize_count(order.quantity - new_filled)
        order.average_fill_price_cents = total_notional / new_filled
        order.updated_at = timestamp
        order.reason = None
        order.status = "filled" if order.remaining_quantity <= 0 else "partially_filled"
        return self._build_order_event(order)

    def get(self, order_id: str) -> SimulatedOrder | None:
        return self._orders.get(order_id)

    def open_orders(self, market_ticker: str | None = None) -> list[SimulatedOrder]:
        orders = [order for order in self._orders.values() if order.is_open]
        if market_ticker is not None:
            orders = [order for order in orders if order.market_ticker == market_ticker]
        return sorted(orders, key=lambda order: (order.created_at, order.order_id))

    def all_orders(self) -> list[SimulatedOrder]:
        return sorted(self._orders.values(), key=lambda order: (order.created_at, order.order_id))
