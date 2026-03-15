from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ...fixed_point import format_decimal
from .events import EngineEvent, FillEvent, MarkEvent, OrderEvent
from .orders import OrderManager, SimulatedOrder
from .portfolio import PortfolioState


def _max_drawdown(marks: list[MarkEvent]) -> Decimal:
    peak: Decimal | None = None
    drawdown = Decimal("0.00")
    for mark in marks:
        peak = mark.total_equity_cents if peak is None else max(peak, mark.total_equity_cents)
        drawdown = max(drawdown, peak - mark.total_equity_cents)
    return drawdown.quantize(Decimal("0.01"))


@dataclass
class BacktestRunResult:
    initial_cash_cents: Decimal
    final_cash_cents: Decimal
    final_equity_cents: Decimal
    total_fees_cents: Decimal
    realized_pnl_cents: Decimal
    max_drawdown_cents: Decimal
    order_events: list[OrderEvent]
    fills: list[FillEvent]
    marks: list[MarkEvent]
    logs: list[dict[str, str]]
    event_log: list[EngineEvent]
    final_orders: list[SimulatedOrder]

    def summary(self) -> dict[str, Any]:
        return {
            "initial_cash_cents": format_decimal(self.initial_cash_cents),
            "final_cash_cents": format_decimal(self.final_cash_cents),
            "final_equity_cents": format_decimal(self.final_equity_cents),
            "total_fees_cents": format_decimal(self.total_fees_cents),
            "realized_pnl_cents": format_decimal(self.realized_pnl_cents),
            "max_drawdown_cents": format_decimal(self.max_drawdown_cents),
            "order_event_count": len(self.order_events),
            "final_order_count": len(self.final_orders),
            "fill_count": len(self.fills),
            "mark_count": len(self.marks),
            "log_count": len(self.logs),
        }


@dataclass
class PerformanceTracker:
    order_events: list[OrderEvent] = field(default_factory=list)
    fills: list[FillEvent] = field(default_factory=list)
    marks: list[MarkEvent] = field(default_factory=list)
    logs: list[dict[str, str]] = field(default_factory=list)
    event_log: list[EngineEvent] = field(default_factory=list)

    def record_event(self, event: EngineEvent) -> None:
        self.event_log.append(event)

    def record_order_event(self, event: OrderEvent) -> None:
        self.order_events.append(event)
        self.event_log.append(event)

    def record_fill(self, event: FillEvent) -> None:
        self.fills.append(event)
        self.event_log.append(event)

    def record_mark(self, event: MarkEvent) -> None:
        self.marks.append(event)
        self.event_log.append(event)

    def build_result(self, portfolio: PortfolioState, order_manager: OrderManager) -> BacktestRunResult:
        return BacktestRunResult(
            initial_cash_cents=portfolio.initial_cash_cents,
            final_cash_cents=portfolio.cash_cents,
            final_equity_cents=portfolio.total_equity_cents(),
            total_fees_cents=portfolio.total_fees_cents,
            realized_pnl_cents=portfolio.total_realized_pnl_cents(),
            max_drawdown_cents=_max_drawdown(self.marks),
            order_events=list(self.order_events),
            fills=list(self.fills),
            marks=list(self.marks),
            logs=list(self.logs),
            event_log=list(self.event_log),
            final_orders=order_manager.all_orders(),
        )
