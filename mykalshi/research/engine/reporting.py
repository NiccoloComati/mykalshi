from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from decimal import Decimal
from typing import Any

from ...exceptions import KalshiDependencyError
from ...fixed_point import format_decimal
from .events import EngineEvent, FillEvent, MarkEvent, OrderEvent
from .orders import OrderManager, SimulatedOrder
from .portfolio import PortfolioState, PositionState


CENT = Decimal("0.01")


def _max_drawdown(marks: list[MarkEvent]) -> Decimal:
    peak: Decimal | None = None
    drawdown = Decimal("0.00")
    for mark in marks:
        peak = mark.total_equity_cents if peak is None else max(peak, mark.total_equity_cents)
        drawdown = max(drawdown, peak - mark.total_equity_cents)
    return drawdown.quantize(CENT)


def _turnover_cents(fills: list[FillEvent]) -> Decimal:
    turnover = Decimal("0.00")
    for fill in fills:
        turnover += (Decimal(fill.price_cents) * fill.quantity).quantize(CENT)
    return turnover.quantize(CENT)


def _peak_exposure_cents(marks: list[MarkEvent]) -> Decimal:
    if not marks:
        return Decimal("0.00")
    return max(((mark.total_equity_cents - mark.cash_cents).quantize(CENT) for mark in marks), default=Decimal("0.00")).quantize(
        CENT
    )


def _percentage(numerator: int | Decimal, denominator: int | Decimal) -> Decimal | None:
    denominator_value = Decimal(str(denominator))
    if denominator_value == 0:
        return None
    return ((Decimal(str(numerator)) / denominator_value) * Decimal("100")).quantize(CENT)


def _format_optional_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format_decimal(value)


def _record_from_value(value: Any) -> Any:
    if is_dataclass(value):
        return {field.name: _record_from_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, tuple):
        return [_record_from_value(item) for item in value]
    if isinstance(value, list):
        return [_record_from_value(item) for item in value]
    return value


@dataclass(frozen=True)
class PositionSnapshot:
    market_ticker: str
    yes_quantity: Decimal
    no_quantity: Decimal
    yes_average_cost_cents: Decimal
    no_average_cost_cents: Decimal
    realized_pnl_cents: Decimal
    unrealized_pnl_cents: Decimal
    total_pnl_cents: Decimal
    market_value_cents: Decimal
    last_yes_price_cents: int | None
    last_no_price_cents: int | None

    @classmethod
    def from_position_state(cls, position: PositionState) -> "PositionSnapshot":
        yes_cost_basis = (position.yes_average_cost_cents * position.yes_quantity).quantize(CENT)
        no_cost_basis = (position.no_average_cost_cents * position.no_quantity).quantize(CENT)
        unrealized_pnl_cents = (position.market_value_cents - yes_cost_basis - no_cost_basis).quantize(CENT)
        realized_pnl_cents = position.realized_pnl_cents.quantize(CENT)
        return cls(
            market_ticker=position.market_ticker,
            yes_quantity=position.yes_quantity.quantize(CENT),
            no_quantity=position.no_quantity.quantize(CENT),
            yes_average_cost_cents=position.yes_average_cost_cents.quantize(CENT),
            no_average_cost_cents=position.no_average_cost_cents.quantize(CENT),
            realized_pnl_cents=realized_pnl_cents,
            unrealized_pnl_cents=unrealized_pnl_cents,
            total_pnl_cents=(realized_pnl_cents + unrealized_pnl_cents).quantize(CENT),
            market_value_cents=position.market_value_cents.quantize(CENT),
            last_yes_price_cents=position.last_yes_price_cents,
            last_no_price_cents=position.last_no_price_cents,
        )

    def summary(self) -> dict[str, Any]:
        return {
            "market_ticker": self.market_ticker,
            "yes_quantity": format_decimal(self.yes_quantity),
            "no_quantity": format_decimal(self.no_quantity),
            "yes_average_cost_cents": format_decimal(self.yes_average_cost_cents),
            "no_average_cost_cents": format_decimal(self.no_average_cost_cents),
            "realized_pnl_cents": format_decimal(self.realized_pnl_cents),
            "unrealized_pnl_cents": format_decimal(self.unrealized_pnl_cents),
            "total_pnl_cents": format_decimal(self.total_pnl_cents),
            "market_value_cents": format_decimal(self.market_value_cents),
            "last_yes_price_cents": self.last_yes_price_cents,
            "last_no_price_cents": self.last_no_price_cents,
        }


@dataclass(frozen=True)
class MarketPerformanceSummary:
    market_ticker: str
    realized_pnl_cents: Decimal
    unrealized_pnl_cents: Decimal
    total_pnl_cents: Decimal
    market_value_cents: Decimal
    peak_market_equity_cents: Decimal
    turnover_cents: Decimal
    buy_turnover_cents: Decimal
    sell_turnover_cents: Decimal
    fees_cents: Decimal
    order_count: int
    order_event_count: int
    orders_with_fill_count: int
    fully_filled_order_count: int
    partially_filled_order_count: int
    rejected_order_count: int
    canceled_order_count: int
    expired_order_count: int
    fill_count: int
    maker_fill_count: int
    taker_fill_count: int
    final_yes_quantity: Decimal
    final_no_quantity: Decimal

    def summary(self) -> dict[str, Any]:
        fill_rate = _percentage(self.orders_with_fill_count, self.order_count)
        return {
            "market_ticker": self.market_ticker,
            "realized_pnl_cents": format_decimal(self.realized_pnl_cents),
            "unrealized_pnl_cents": format_decimal(self.unrealized_pnl_cents),
            "total_pnl_cents": format_decimal(self.total_pnl_cents),
            "market_value_cents": format_decimal(self.market_value_cents),
            "peak_market_equity_cents": format_decimal(self.peak_market_equity_cents),
            "turnover_cents": format_decimal(self.turnover_cents),
            "buy_turnover_cents": format_decimal(self.buy_turnover_cents),
            "sell_turnover_cents": format_decimal(self.sell_turnover_cents),
            "fees_cents": format_decimal(self.fees_cents),
            "order_count": self.order_count,
            "order_event_count": self.order_event_count,
            "orders_with_fill_count": self.orders_with_fill_count,
            "fully_filled_order_count": self.fully_filled_order_count,
            "partially_filled_order_count": self.partially_filled_order_count,
            "rejected_order_count": self.rejected_order_count,
            "canceled_order_count": self.canceled_order_count,
            "expired_order_count": self.expired_order_count,
            "fill_count": self.fill_count,
            "maker_fill_count": self.maker_fill_count,
            "taker_fill_count": self.taker_fill_count,
            "fill_rate_pct": _format_optional_decimal(fill_rate),
            "final_yes_quantity": format_decimal(self.final_yes_quantity),
            "final_no_quantity": format_decimal(self.final_no_quantity),
        }


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
    final_positions: list[PositionSnapshot]

    @property
    def net_profit_cents(self) -> Decimal:
        return (self.final_equity_cents - self.initial_cash_cents).quantize(CENT)

    @property
    def unrealized_pnl_cents(self) -> Decimal:
        return (self.net_profit_cents - self.realized_pnl_cents).quantize(CENT)

    @property
    def turnover_cents(self) -> Decimal:
        return _turnover_cents(self.fills)

    @property
    def peak_exposure_cents(self) -> Decimal:
        return _peak_exposure_cents(self.marks)

    @property
    def return_pct(self) -> Decimal | None:
        return _percentage(self.net_profit_cents, self.initial_cash_cents)

    def position(self, market_ticker: str) -> PositionSnapshot | None:
        for position in self.final_positions:
            if position.market_ticker == market_ticker:
                return position
        return None

    def market_summaries(self) -> list[MarketPerformanceSummary]:
        market_tickers = {
            *[position.market_ticker for position in self.final_positions],
            *[fill.market_ticker for fill in self.fills],
            *[order.market_ticker for order in self.final_orders],
            *[mark.market_ticker for mark in self.marks],
        }
        summaries: list[MarketPerformanceSummary] = []
        for market_ticker in sorted(ticker for ticker in market_tickers if ticker):
            fills = [fill for fill in self.fills if fill.market_ticker == market_ticker]
            final_orders = [order for order in self.final_orders if order.market_ticker == market_ticker]
            order_events = [event for event in self.order_events if event.market_ticker == market_ticker]
            marks = [mark for mark in self.marks if mark.market_ticker == market_ticker]
            position = self.position(market_ticker)

            buy_turnover = sum(
                ((Decimal(fill.price_cents) * fill.quantity).quantize(CENT) for fill in fills if fill.action.startswith("buy")),
                Decimal("0.00"),
            ).quantize(CENT)
            sell_turnover = sum(
                ((Decimal(fill.price_cents) * fill.quantity).quantize(CENT) for fill in fills if fill.action.startswith("sell")),
                Decimal("0.00"),
            ).quantize(CENT)
            fees_cents = sum((fill.fee_cents for fill in fills), Decimal("0.00")).quantize(CENT)

            summaries.append(
                MarketPerformanceSummary(
                    market_ticker=market_ticker,
                    realized_pnl_cents=position.realized_pnl_cents if position is not None else Decimal("0.00"),
                    unrealized_pnl_cents=position.unrealized_pnl_cents if position is not None else Decimal("0.00"),
                    total_pnl_cents=position.total_pnl_cents if position is not None else Decimal("0.00"),
                    market_value_cents=position.market_value_cents if position is not None else Decimal("0.00"),
                    peak_market_equity_cents=max((mark.market_equity_cents for mark in marks), default=Decimal("0.00")).quantize(
                        CENT
                    ),
                    turnover_cents=(buy_turnover + sell_turnover).quantize(CENT),
                    buy_turnover_cents=buy_turnover,
                    sell_turnover_cents=sell_turnover,
                    fees_cents=fees_cents,
                    order_count=len(final_orders),
                    order_event_count=len(order_events),
                    orders_with_fill_count=sum(1 for order in final_orders if order.filled_quantity > 0),
                    fully_filled_order_count=sum(1 for order in final_orders if order.status == "filled"),
                    partially_filled_order_count=sum(
                        1 for order in final_orders if order.filled_quantity > 0 and order.remaining_quantity > 0
                    ),
                    rejected_order_count=sum(1 for order in final_orders if order.status == "rejected"),
                    canceled_order_count=sum(1 for order in final_orders if order.status == "canceled"),
                    expired_order_count=sum(1 for order in final_orders if order.status == "expired"),
                    fill_count=len(fills),
                    maker_fill_count=sum(1 for fill in fills if fill.liquidity_role == "passive"),
                    taker_fill_count=sum(1 for fill in fills if fill.liquidity_role == "aggressive"),
                    final_yes_quantity=position.yes_quantity if position is not None else Decimal("0.00"),
                    final_no_quantity=position.no_quantity if position is not None else Decimal("0.00"),
                )
            )
        return summaries

    def market_summary(self, market_ticker: str) -> MarketPerformanceSummary | None:
        for summary in self.market_summaries():
            if summary.market_ticker == market_ticker:
                return summary
        return None

    def order_records(self) -> list[dict[str, Any]]:
        return [_record_from_value(order) for order in self.final_orders]

    def order_event_records(self) -> list[dict[str, Any]]:
        return [_record_from_value(event) for event in self.order_events]

    def fill_records(self) -> list[dict[str, Any]]:
        return [_record_from_value(fill) for fill in self.fills]

    def mark_records(self) -> list[dict[str, Any]]:
        return [_record_from_value(mark) for mark in self.marks]

    def log_records(self) -> list[dict[str, Any]]:
        return [dict(log) for log in self.logs]

    def event_records(self) -> list[dict[str, Any]]:
        return [_record_from_value(event) for event in self.event_log]

    def position_records(self) -> list[dict[str, Any]]:
        return [_record_from_value(position) for position in self.final_positions]

    def market_summary_records(self) -> list[dict[str, Any]]:
        return [_record_from_value(summary) for summary in self.market_summaries()]

    def to_dataframes(self) -> dict[str, Any]:
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            raise KalshiDependencyError("pandas is required to export backtest results as DataFrames") from exc

        return {
            "orders": pd.DataFrame.from_records(self.order_records()),
            "order_events": pd.DataFrame.from_records(self.order_event_records()),
            "fills": pd.DataFrame.from_records(self.fill_records()),
            "marks": pd.DataFrame.from_records(self.mark_records()),
            "logs": pd.DataFrame.from_records(self.log_records()),
            "events": pd.DataFrame.from_records(self.event_records()),
            "positions": pd.DataFrame.from_records(self.position_records()),
            "markets": pd.DataFrame.from_records(self.market_summary_records()),
        }

    def summary(self) -> dict[str, Any]:
        rejected_order_count = sum(1 for order in self.final_orders if order.status == "rejected")
        canceled_order_count = sum(1 for order in self.final_orders if order.status == "canceled")
        expired_order_count = sum(1 for order in self.final_orders if order.status == "expired")
        orders_with_fill_count = sum(1 for order in self.final_orders if order.filled_quantity > 0)
        fully_filled_order_count = sum(1 for order in self.final_orders if order.status == "filled")
        maker_fill_count = sum(1 for fill in self.fills if fill.liquidity_role == "passive")
        taker_fill_count = sum(1 for fill in self.fills if fill.liquidity_role == "aggressive")
        return {
            "initial_cash_cents": format_decimal(self.initial_cash_cents),
            "final_cash_cents": format_decimal(self.final_cash_cents),
            "final_equity_cents": format_decimal(self.final_equity_cents),
            "total_fees_cents": format_decimal(self.total_fees_cents),
            "realized_pnl_cents": format_decimal(self.realized_pnl_cents),
            "unrealized_pnl_cents": format_decimal(self.unrealized_pnl_cents),
            "net_profit_cents": format_decimal(self.net_profit_cents),
            "max_drawdown_cents": format_decimal(self.max_drawdown_cents),
            "turnover_cents": format_decimal(self.turnover_cents),
            "peak_exposure_cents": format_decimal(self.peak_exposure_cents),
            "return_pct": _format_optional_decimal(self.return_pct),
            "order_event_count": len(self.order_events),
            "submitted_order_count": len(self.final_orders),
            "final_order_count": len(self.final_orders),
            "orders_with_fill_count": orders_with_fill_count,
            "fully_filled_order_count": fully_filled_order_count,
            "rejected_order_count": rejected_order_count,
            "canceled_order_count": canceled_order_count,
            "expired_order_count": expired_order_count,
            "fill_count": len(self.fills),
            "maker_fill_count": maker_fill_count,
            "taker_fill_count": taker_fill_count,
            "mark_count": len(self.marks),
            "log_count": len(self.logs),
            "market_count": len(self.final_positions),
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
            final_positions=[PositionSnapshot.from_position_state(position) for position in portfolio.positions()],
        )
