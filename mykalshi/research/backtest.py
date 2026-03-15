from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, replace
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Callable, Iterable, Protocol, Sequence

from .. import historical
from ..fixed_point import dollars_to_cents, format_decimal, quantize_count
from .datasets import load_replay_event_stream
from .engine import (
    BacktestRunResult,
    EventDrivenBacktestEngine,
    HistoricalTradeReplay,
    KalshiStrategy,
    MarketDataReplay,
    StrategyContext,
    TradeMarketEvent,
)
from .engine.events import FillEvent as EngineFillEvent
from .engine.events import MarkEvent as EngineMarkEvent
from .engine.events import OrderEvent as EngineOrderEvent
from .engine.execution import ExecutionDecision as EngineExecutionDecision


CENT = Decimal("0.01")
CENTICENT = Decimal("0.0001")


def _cash(value: int | float | str | Decimal) -> Decimal:
    return quantize_count(value)


def _trade_sort_key(trade: dict[str, Any]) -> str:
    return str(trade.get("created_time") or trade.get("ts") or "")


def _extract_trade_prices(trade: dict[str, Any]) -> tuple[int, int]:
    yes_price = trade.get("yes_price_dollars")
    no_price = trade.get("no_price_dollars")
    if yes_price is None and trade.get("price") is not None:
        return int(trade["price"]), 100 - int(trade["price"])
    if yes_price is None and no_price is None:
        raise ValueError(f"Trade is missing price fields: {trade!r}")

    yes_price_cents = dollars_to_cents(yes_price) if yes_price is not None else 100 - dollars_to_cents(no_price)
    no_price_cents = dollars_to_cents(no_price) if no_price is not None else 100 - yes_price_cents
    return yes_price_cents, no_price_cents


def _price_dollars(price_cents: int) -> Decimal:
    return (Decimal(price_cents) / Decimal("100")).quantize(Decimal("0.0001"))


def _calculate_max_drawdown(marks: Iterable["BacktestMark"]) -> Decimal:
    peak: Decimal | None = None
    max_drawdown = Decimal("0.00")
    for mark in marks:
        peak = mark.equity_cents if peak is None else max(peak, mark.equity_cents)
        max_drawdown = max(max_drawdown, peak - mark.equity_cents)
    return max_drawdown.quantize(CENT)


def load_historical_trades(
    ticker: str,
    *,
    min_ts: Any | None = None,
    max_ts: Any | None = None,
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    response = historical.get_all_historical_trades(
        ticker=ticker,
        min_ts=min_ts,
        max_ts=max_ts,
        batch_size=batch_size,
    )
    return sorted(response.get("trades", []), key=_trade_sort_key)


@dataclass(frozen=True)
class TradeSignal:
    action: str
    quantity: int | float | str | Decimal = 1
    limit_price_cents: int | None = None
    slippage_cents: int = 0
    tag: str | None = None
    note: str | None = None


@dataclass(frozen=True)
class PositionTargetSignal:
    side: str
    target_quantity: int | float | str | Decimal = 1
    entry_limit_price_cents: int | None = None
    exit_limit_price_cents: int | None = None
    max_trade_quantity: int | float | str | Decimal | None = None
    slippage_cents: int = 0
    tag: str | None = None
    note: str | None = None


@dataclass(frozen=True)
class BacktestFill:
    timestamp: str
    action: str
    quantity: Decimal
    price_cents: int
    fee_cents: Decimal
    cash_after_cents: Decimal
    yes_position: Decimal
    no_position: Decimal
    tag: str | None = None
    note: str | None = None


@dataclass(frozen=True)
class BacktestMark:
    timestamp: str
    yes_price_cents: int
    no_price_cents: int
    cash_cents: Decimal
    yes_position: Decimal
    no_position: Decimal
    equity_cents: Decimal


@dataclass(frozen=True)
class BacktestOrder:
    timestamp: str
    action: str
    quantity: Decimal
    requested_limit_price_cents: int | None
    execution_price_cents: int | None
    status: str
    fee_cents: Decimal = Decimal("0.00")
    reason: str | None = None
    tag: str | None = None
    note: str | None = None


@dataclass
class BacktestResult:
    ticker: str | None
    initial_cash_cents: Decimal
    final_cash_cents: Decimal
    final_equity_cents: Decimal
    total_fees_cents: Decimal
    net_profit_cents: Decimal
    max_drawdown_cents: Decimal
    yes_position: Decimal
    no_position: Decimal
    orders: list[BacktestOrder]
    fills: list[BacktestFill]
    marks: list[BacktestMark]

    def summary(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "initial_cash_cents": format_decimal(self.initial_cash_cents),
            "final_cash_cents": format_decimal(self.final_cash_cents),
            "final_equity_cents": format_decimal(self.final_equity_cents),
            "total_fees_cents": format_decimal(self.total_fees_cents),
            "net_profit_cents": format_decimal(self.net_profit_cents),
            "max_drawdown_cents": format_decimal(self.max_drawdown_cents),
            "yes_position": format_decimal(self.yes_position),
            "no_position": format_decimal(self.no_position),
            "order_count": len(self.orders),
            "filled_order_count": sum(1 for order in self.orders if order.status == "filled"),
            "rejected_order_count": sum(1 for order in self.orders if order.status != "filled"),
            "fill_count": len(self.fills),
            "mark_count": len(self.marks),
        }


FeeModel = Callable[..., int | float | str | Decimal]
StrategySignal = TradeSignal | PositionTargetSignal
StrategyResult = StrategySignal | Sequence[StrategySignal] | None


class StrategyProtocol(Protocol):
    def on_trade(self, context: "BacktestContext", trade: dict[str, Any]) -> StrategyResult:
        ...


@dataclass(frozen=True)
class ExecutionDecision:
    status: str
    execution_price_cents: int | None = None
    reason: str | None = None


class ImmediateTradeExecutionModel:
    def __init__(self, *, default_slippage_cents: int = 0) -> None:
        self.default_slippage_cents = default_slippage_cents

    @staticmethod
    def _clip_price(price_cents: int) -> int:
        return max(0, min(100, price_cents))

    @staticmethod
    def _limit_satisfied(action: str, price_cents: int, limit_price_cents: int | None) -> bool:
        if limit_price_cents is None:
            return True
        if action.startswith("buy"):
            return price_cents <= limit_price_cents
        if action.startswith("sell"):
            return price_cents >= limit_price_cents
        return False

    def evaluate(
        self,
        signal: TradeSignal,
        trade: dict[str, Any],
        *,
        yes_price_cents: int,
        no_price_cents: int,
    ) -> ExecutionDecision:
        action = signal.action.lower()
        base_price_cents = yes_price_cents if action.endswith("yes") else no_price_cents
        slippage_cents = max(0, int(signal.slippage_cents or self.default_slippage_cents))

        if action.startswith("buy"):
            execution_price_cents = self._clip_price(base_price_cents + slippage_cents)
        elif action.startswith("sell"):
            execution_price_cents = self._clip_price(base_price_cents - slippage_cents)
        else:
            return ExecutionDecision(status="rejected", reason=f"Unsupported action: {signal.action!r}")

        if not self._limit_satisfied(action, execution_price_cents, signal.limit_price_cents):
            return ExecutionDecision(
                status="rejected",
                reason=f"Limit price {signal.limit_price_cents} not met by execution price {execution_price_cents}",
            )

        return ExecutionDecision(status="filled", execution_price_cents=execution_price_cents)


class ZeroFeeModel:
    def __call__(
        self,
        signal: TradeSignal,
        trade: dict[str, Any],
        yes_price_cents: int,
        no_price_cents: int,
        execution_price_cents: int,
        liquidity_role: str | None = None,
    ) -> Decimal:
        return Decimal("0.00")


class FixedPerContractFeeModel:
    def __init__(self, cents_per_contract: int | float | str | Decimal) -> None:
        self.cents_per_contract = _cash(cents_per_contract)

    def __call__(
        self,
        signal: TradeSignal,
        trade: dict[str, Any],
        yes_price_cents: int,
        no_price_cents: int,
        execution_price_cents: int,
        liquidity_role: str | None = None,
    ) -> Decimal:
        quantity = quantize_count(signal.quantity)
        return (quantity * self.cents_per_contract).quantize(CENT)


class KalshiTakerFeeModel:
    def __init__(self, *, rate: int | float | str | Decimal = Decimal("0.07")) -> None:
        self.rate = Decimal(str(rate))

    def __call__(
        self,
        signal: TradeSignal,
        trade: dict[str, Any],
        yes_price_cents: int,
        no_price_cents: int,
        execution_price_cents: int,
        liquidity_role: str | None = None,
    ) -> Decimal:
        quantity = quantize_count(signal.quantity)
        price_dollars = _price_dollars(execution_price_cents)
        raw_trade_fee_dollars = self.rate * quantity * price_dollars * (Decimal("1") - price_dollars)
        trade_fee_dollars = raw_trade_fee_dollars.quantize(CENTICENT, rounding=ROUND_CEILING)

        revenue_dollars = quantity * price_dollars
        if signal.action.lower().startswith("buy"):
            revenue_dollars = -revenue_dollars

        net_balance_change_dollars = revenue_dollars - trade_fee_dollars
        rounded_balance_change_dollars = net_balance_change_dollars.quantize(CENT, rounding=ROUND_FLOOR)
        effective_fee_dollars = abs(revenue_dollars - rounded_balance_change_dollars)
        return (effective_fee_dollars * 100).quantize(CENT)


class KalshiMakerTakerFeeModel:
    """Fee model that differentiates passive (maker) and aggressive (taker) fills."""

    def __init__(
        self,
        *,
        taker_rate: int | float | str | Decimal = Decimal("0.07"),
        maker_rate: int | float | str | Decimal = Decimal("0.00"),
    ) -> None:
        self.taker_model = KalshiTakerFeeModel(rate=taker_rate)
        self.maker_model = KalshiTakerFeeModel(rate=maker_rate)

    def __call__(
        self,
        signal: TradeSignal,
        trade: dict[str, Any],
        yes_price_cents: int,
        no_price_cents: int,
        execution_price_cents: int,
        liquidity_role: str | None = None,
    ) -> Decimal:
        model = self.maker_model if liquidity_role == "passive" else self.taker_model
        return model(signal, trade, yes_price_cents, no_price_cents, execution_price_cents)


@dataclass
class BacktestContext:
    ticker: str | None
    cash_cents: Decimal
    yes_position: Decimal = field(default_factory=lambda: Decimal("0.00"))
    no_position: Decimal = field(default_factory=lambda: Decimal("0.00"))
    orders: list[BacktestOrder] = field(default_factory=list)
    fills: list[BacktestFill] = field(default_factory=list)
    marks: list[BacktestMark] = field(default_factory=list)

    def mark(self, timestamp: str, yes_price_cents: int, no_price_cents: int) -> BacktestMark:
        equity = (
            self.cash_cents
            + (self.yes_position * Decimal(yes_price_cents))
            + (self.no_position * Decimal(no_price_cents))
        ).quantize(Decimal("0.01"))
        mark = BacktestMark(
            timestamp=timestamp,
            yes_price_cents=yes_price_cents,
            no_price_cents=no_price_cents,
            cash_cents=self.cash_cents,
            yes_position=self.yes_position,
            no_position=self.no_position,
            equity_cents=equity,
        )
        self.marks.append(mark)
        return mark

    def execute(
        self,
        signal: TradeSignal,
        trade: dict[str, Any],
        *,
        execution_price_cents: int,
        fee_cents: Decimal,
    ) -> BacktestFill:
        quantity = quantize_count(signal.quantity)
        if quantity <= 0:
            raise ValueError("TradeSignal.quantity must be positive")

        action = signal.action.lower()
        gross_cash_change = quantity * Decimal(execution_price_cents)

        if action == "buy_yes":
            total_cost = gross_cash_change + fee_cents
            if total_cost > self.cash_cents:
                raise ValueError("Insufficient cash to buy yes contracts")
            self.cash_cents -= total_cost
            self.yes_position += quantity
        elif action == "sell_yes":
            if quantity > self.yes_position:
                raise ValueError("Cannot sell more yes contracts than are held")
            self.cash_cents += gross_cash_change - fee_cents
            self.yes_position -= quantity
        elif action == "buy_no":
            total_cost = gross_cash_change + fee_cents
            if total_cost > self.cash_cents:
                raise ValueError("Insufficient cash to buy no contracts")
            self.cash_cents -= total_cost
            self.no_position += quantity
        elif action == "sell_no":
            if quantity > self.no_position:
                raise ValueError("Cannot sell more no contracts than are held")
            self.cash_cents += gross_cash_change - fee_cents
            self.no_position -= quantity
        else:
            raise ValueError(f"Unsupported TradeSignal action: {signal.action!r}")

        self.cash_cents = self.cash_cents.quantize(Decimal("0.01"))
        fill = BacktestFill(
            timestamp=str(trade.get("created_time") or trade.get("ts") or ""),
            action=action,
            quantity=quantity,
            price_cents=execution_price_cents,
            fee_cents=fee_cents,
            cash_after_cents=self.cash_cents,
            yes_position=self.yes_position,
            no_position=self.no_position,
            tag=signal.tag,
            note=signal.note,
        )
        self.fills.append(fill)
        return fill


@dataclass
class _QueuedSignalPlan:
    signal: StrategySignal
    remaining_trade_quantity: Decimal | None = None


class _LegacyFillModel:
    """Compatibility shim that preserves the old immediate trade semantics."""

    def __init__(self, execution_model: ImmediateTradeExecutionModel) -> None:
        self.execution_model = execution_model

    @staticmethod
    def _trade_payload(event: TradeMarketEvent) -> dict[str, Any]:
        if isinstance(event.raw_data, dict):
            return event.raw_data
        return {
            "created_time": event.timestamp,
            "ticker": event.market_ticker,
            "price": event.yes_price_cents,
        }

    def evaluate(self, order, market_event, market_state) -> EngineExecutionDecision | None:
        if not isinstance(market_event, TradeMarketEvent):
            return None
        signal = TradeSignal(
            action=order.action,
            quantity=order.remaining_quantity,
            limit_price_cents=order.limit_price_cents,
            slippage_cents=order.slippage_cents,
            tag=order.tag,
            note=order.note,
        )
        trade = self._trade_payload(market_event)
        decision = self.execution_model.evaluate(
            signal,
            trade,
            yes_price_cents=market_event.yes_price_cents,
            no_price_cents=market_event.no_price_cents,
        )
        if decision.status != "filled" or decision.execution_price_cents is None:
            return EngineExecutionDecision(status="rejected", reason=decision.reason or "Order rejected by execution model")
        return EngineExecutionDecision(
            status="filled",
            quantity=quantize_count(order.remaining_quantity),
            price_cents=decision.execution_price_cents,
            liquidity_role="aggressive",
        )


class _LegacyFeeModelAdapter:
    def __init__(self, fee_model: FeeModel | None) -> None:
        self.fee_model = fee_model

    def __call__(
        self,
        order,
        market_event,
        execution_price_cents: int,
        quantity: Decimal,
        liquidity_role: str | None = None,
    ) -> Decimal:
        if self.fee_model is None:
            return Decimal("0.00")
        if isinstance(market_event, TradeMarketEvent) and isinstance(market_event.raw_data, dict):
            trade = market_event.raw_data
            yes_price_cents = market_event.yes_price_cents
            no_price_cents = market_event.no_price_cents
        else:
            trade = {
                "created_time": market_event.timestamp,
                "ticker": market_event.market_ticker,
                "price": execution_price_cents,
            }
            yes_price_cents = execution_price_cents
            no_price_cents = 100 - execution_price_cents

        signal = TradeSignal(
            action=order.action,
            quantity=quantity,
            limit_price_cents=order.limit_price_cents,
            slippage_cents=order.slippage_cents,
            tag=order.tag,
            note=order.note,
        )
        return TradeBacktester._call_fee_model(
            self.fee_model,
            signal,
            trade,
            yes_price_cents,
            no_price_cents,
            execution_price_cents,
            liquidity_role=liquidity_role or order.liquidity_intent,
        )


class _CompatibilityStrategyAdapter(KalshiStrategy):
    """Runs the legacy strategy API over the event-driven core.

    The adapter serializes signal handling so the old wrapper semantics still hold:
    one requested order resolves before the next compatibility order is submitted.
    """

    def __init__(
        self,
        backtester: "TradeBacktester",
        strategy: StrategyProtocol | Callable[[BacktestContext, dict[str, Any]], StrategyResult],
        legacy_context: BacktestContext,
    ) -> None:
        self.backtester = backtester
        self.strategy = strategy
        self.legacy_context = legacy_context
        self._plan_queue: deque[_QueuedSignalPlan] = deque()
        self._waiting_for_resolution = False
        self._order_snapshots: dict[str, BacktestOrder] = {}
        self._fees_by_order_id: dict[str, Decimal] = {}
        self._primary_market_ticker = legacy_context.ticker

    def _trade_payload(self, event: TradeMarketEvent) -> dict[str, Any]:
        if isinstance(event.raw_data, dict):
            return event.raw_data
        return {
            "created_time": event.timestamp,
            "ticker": event.market_ticker,
            "price": event.yes_price_cents,
        }

    def _sync_context(self, context: StrategyContext, market_ticker: str | None = None) -> None:
        self.legacy_context.cash_cents = context.portfolio.cash_cents
        ticker = market_ticker or self._primary_market_ticker
        if ticker is None and context.current_event is not None:
            ticker = context.current_event.market_ticker
        if ticker is None:
            return
        position = context.position(ticker)
        self._primary_market_ticker = ticker
        self.legacy_context.ticker = self.legacy_context.ticker or ticker
        self.legacy_context.yes_position = position.yes_quantity
        self.legacy_context.no_position = position.no_quantity
        self.legacy_context.orders = list(self._order_snapshots.values())

    def _submit_trade_signal(self, context: StrategyContext, market_ticker: str, signal: TradeSignal) -> None:
        context.submit_order(
            market_ticker,
            action=signal.action,
            quantity=signal.quantity,
            limit_price_cents=signal.limit_price_cents,
            slippage_cents=signal.slippage_cents,
            tag=signal.tag,
            note=signal.note,
        )
        self._waiting_for_resolution = True

    def _advance_signal_queue(self, context: StrategyContext) -> None:
        if self._waiting_for_resolution or context.current_event is None:
            return

        while self._plan_queue:
            plan = self._plan_queue[0]
            current_event = context.current_event
            if isinstance(plan.signal, TradeSignal):
                self._submit_trade_signal(context, current_event.market_ticker, plan.signal)
                return

            next_signal = self.backtester._next_trade_signal_for_target(
                plan.signal,
                self.legacy_context,
                plan.remaining_trade_quantity,
            )
            if next_signal is None:
                self._plan_queue.popleft()
                continue

            self._submit_trade_signal(context, current_event.market_ticker, next_signal)
            return

    def _resolve_after_non_fill(self, context: StrategyContext) -> None:
        if self._plan_queue:
            self._plan_queue.popleft()
        self._waiting_for_resolution = False
        self._advance_signal_queue(context)

    def _resolve_after_fill(self, context: StrategyContext, event: EngineFillEvent) -> None:
        if self._plan_queue:
            plan = self._plan_queue[0]
            if isinstance(plan.signal, PositionTargetSignal) and plan.remaining_trade_quantity is not None:
                plan.remaining_trade_quantity = quantize_count(plan.remaining_trade_quantity - event.quantity)

            if isinstance(plan.signal, TradeSignal):
                self._plan_queue.popleft()
            else:
                next_signal = self.backtester._next_trade_signal_for_target(
                    plan.signal,
                    self.legacy_context,
                    plan.remaining_trade_quantity,
                )
                if next_signal is None:
                    self._plan_queue.popleft()

        self._waiting_for_resolution = False
        self._advance_signal_queue(context)

    def _record_order_snapshot(self, event: EngineOrderEvent) -> None:
        reason = event.reason
        if reason == "Insufficient available cash to reserve order":
            reason = "Insufficient cash to reserve order"
        elif reason == "Insufficient available yes inventory to reserve order":
            reason = "Cannot sell more yes contracts than are held"
        elif reason == "Insufficient available no inventory to reserve order":
            reason = "Cannot sell more no contracts than are held"
        order = BacktestOrder(
            timestamp=event.timestamp,
            action=event.action,
            quantity=event.quantity,
            requested_limit_price_cents=event.limit_price_cents,
            execution_price_cents=event.average_fill_price_cents,
            status=event.status,
            fee_cents=self._fees_by_order_id.get(event.order_id, Decimal("0.00")).quantize(CENT),
            reason=reason,
            tag=event.tag,
            note=event.note,
        )
        self._order_snapshots[event.order_id] = order
        self.legacy_context.orders = list(self._order_snapshots.values())

    def on_start(self, context: StrategyContext) -> None:
        self._sync_context(context)
        self.backtester._call_optional_hook(self.strategy, "on_start", self.legacy_context)

    def on_trade(self, context: StrategyContext, event: TradeMarketEvent) -> None:
        self._sync_context(context, event.market_ticker)
        signals = self.backtester._normalize_strategy_signals(
            self.backtester._call_strategy(self.strategy, self.legacy_context, self._trade_payload(event))
        )
        for signal in signals:
            if isinstance(signal, PositionTargetSignal):
                remaining_trade_quantity = (
                    quantize_count(signal.max_trade_quantity) if signal.max_trade_quantity is not None else None
                )
                if remaining_trade_quantity is not None and remaining_trade_quantity <= 0:
                    raise ValueError("PositionTargetSignal.max_trade_quantity must be positive when provided")
                self._plan_queue.append(
                    _QueuedSignalPlan(signal=signal, remaining_trade_quantity=remaining_trade_quantity)
                )
                continue
            self._plan_queue.append(_QueuedSignalPlan(signal=signal))

        self._advance_signal_queue(context)

    def on_order(self, context: StrategyContext, event: EngineOrderEvent) -> None:
        self._sync_context(context, event.market_ticker or self._primary_market_ticker)
        self._record_order_snapshot(event)
        if event.status in {"rejected", "canceled", "expired"}:
            self._resolve_after_non_fill(context)

    def on_fill(self, context: StrategyContext, event: EngineFillEvent) -> None:
        self._fees_by_order_id[event.order_id] = (
            self._fees_by_order_id.get(event.order_id, Decimal("0.00")) + event.fee_cents
        ).quantize(CENT)
        if event.order_id in self._order_snapshots:
            self._order_snapshots[event.order_id] = replace(
                self._order_snapshots[event.order_id],
                fee_cents=self._fees_by_order_id[event.order_id],
            )
            self.legacy_context.orders = list(self._order_snapshots.values())

        self.legacy_context.cash_cents = event.cash_after_cents or self.legacy_context.cash_cents
        self.legacy_context.yes_position = event.yes_position or Decimal("0.00")
        self.legacy_context.no_position = event.no_position or Decimal("0.00")
        self.legacy_context.fills.append(
            BacktestFill(
                timestamp=event.timestamp,
                action=event.action,
                quantity=event.quantity,
                price_cents=event.price_cents,
                fee_cents=event.fee_cents,
                cash_after_cents=event.cash_after_cents or self.legacy_context.cash_cents,
                yes_position=event.yes_position or Decimal("0.00"),
                no_position=event.no_position or Decimal("0.00"),
                tag=event.tag,
                note=event.note,
            )
        )
        self._resolve_after_fill(context, event)

    def on_mark(self, context: StrategyContext, event: EngineMarkEvent) -> None:
        self._sync_context(context, event.market_ticker)
        if event.yes_price_cents is None or event.no_price_cents is None:
            return
        self.legacy_context.marks.append(
            BacktestMark(
                timestamp=event.timestamp,
                yes_price_cents=event.yes_price_cents,
                no_price_cents=event.no_price_cents,
                cash_cents=event.cash_cents,
                yes_position=self.legacy_context.yes_position,
                no_position=self.legacy_context.no_position,
                equity_cents=event.total_equity_cents,
            )
        )

    def on_finish(self, context: StrategyContext) -> None:
        self._sync_context(context)
        self.legacy_context.orders = list(self._order_snapshots.values())
        self.backtester._call_optional_hook(self.strategy, "on_finish", self.legacy_context)


class TradeBacktester:
    def __init__(
        self,
        *,
        fee_model: FeeModel | None = None,
        execution_model: ImmediateTradeExecutionModel | None = None,
    ) -> None:
        self.fee_model = fee_model or ZeroFeeModel()
        self.execution_model = execution_model or ImmediateTradeExecutionModel()

    @staticmethod
    def _normalize_strategy_signals(result: StrategyResult) -> list[StrategySignal]:
        if result is None:
            return []
        if isinstance(result, (TradeSignal, PositionTargetSignal)):
            return [result]
        signals = list(result)
        if not all(isinstance(signal, (TradeSignal, PositionTargetSignal)) for signal in signals):
            raise TypeError("Strategies must return TradeSignal, PositionTargetSignal, or sequences of those types")
        return signals

    @staticmethod
    def _build_trade_signal(
        action: str,
        quantity: Decimal,
        signal: PositionTargetSignal,
        *,
        limit_price_cents: int | None,
    ) -> TradeSignal | None:
        if quantity <= 0:
            return None
        return TradeSignal(
            action=action,
            quantity=quantity,
            limit_price_cents=limit_price_cents,
            slippage_cents=signal.slippage_cents,
            tag=signal.tag,
            note=signal.note,
        )

    @classmethod
    def _next_trade_signal_for_target(
        cls,
        signal: PositionTargetSignal,
        context: BacktestContext,
        remaining_trade_quantity: Decimal | None,
    ) -> TradeSignal | None:
        if remaining_trade_quantity is not None and remaining_trade_quantity <= 0:
            return None

        side = signal.side.lower()
        target_quantity = quantize_count(signal.target_quantity)
        if target_quantity < 0:
            raise ValueError("PositionTargetSignal.target_quantity must be non-negative")

        def cap(quantity: Decimal) -> Decimal:
            if remaining_trade_quantity is None:
                return quantity
            return min(quantity, remaining_trade_quantity)

        if side == "flat":
            return cls._build_trade_signal(
                "sell_yes",
                cap(context.yes_position),
                signal,
                limit_price_cents=signal.exit_limit_price_cents,
            ) or cls._build_trade_signal(
                "sell_no",
                cap(context.no_position),
                signal,
                limit_price_cents=signal.exit_limit_price_cents,
            )

        if side == "yes":
            if context.no_position > 0:
                return cls._build_trade_signal(
                    "sell_no",
                    cap(context.no_position),
                    signal,
                    limit_price_cents=signal.exit_limit_price_cents,
                )

            delta_yes = target_quantity - context.yes_position
            if delta_yes > 0:
                return cls._build_trade_signal(
                    "buy_yes",
                    cap(delta_yes),
                    signal,
                    limit_price_cents=signal.entry_limit_price_cents,
                )
            if delta_yes < 0:
                return cls._build_trade_signal(
                    "sell_yes",
                    cap(-delta_yes),
                    signal,
                    limit_price_cents=signal.exit_limit_price_cents,
                )
            return None

        if side == "no":
            if context.yes_position > 0:
                return cls._build_trade_signal(
                    "sell_yes",
                    cap(context.yes_position),
                    signal,
                    limit_price_cents=signal.exit_limit_price_cents,
                )

            delta_no = target_quantity - context.no_position
            if delta_no > 0:
                return cls._build_trade_signal(
                    "buy_no",
                    cap(delta_no),
                    signal,
                    limit_price_cents=signal.entry_limit_price_cents,
                )
            if delta_no < 0:
                return cls._build_trade_signal(
                    "sell_no",
                    cap(-delta_no),
                    signal,
                    limit_price_cents=signal.exit_limit_price_cents,
                )
            return None

        raise ValueError(f"Unsupported PositionTargetSignal side: {signal.side!r}")

    @staticmethod
    def _call_strategy(
        strategy: StrategyProtocol | Callable[[BacktestContext, dict[str, Any]], StrategyResult],
        context: BacktestContext,
        trade: dict[str, Any],
    ) -> StrategyResult:
        if hasattr(strategy, "on_trade"):
            return strategy.on_trade(context, trade)
        return strategy(context, trade)

    @staticmethod
    def _call_optional_hook(strategy: Any, hook_name: str, context: BacktestContext) -> None:
        hook = getattr(strategy, hook_name, None)
        if callable(hook):
            hook(context)

    @staticmethod
    def _call_fee_model(
        fee_model: FeeModel | None,
        signal: TradeSignal,
        trade: dict[str, Any],
        yes_price_cents: int,
        no_price_cents: int,
        execution_price_cents: int,
        liquidity_role: str | None = None,
    ) -> Decimal:
        if fee_model is None:
            return Decimal("0.00")
        try:
            return _cash(
                fee_model(
                    signal,
                    trade,
                    yes_price_cents,
                    no_price_cents,
                    execution_price_cents,
                    liquidity_role=liquidity_role,
                )
            )
        except TypeError:
            try:
                return _cash(
                    fee_model(
                        signal,
                        trade,
                        yes_price_cents,
                        no_price_cents,
                        execution_price_cents,
                        liquidity_role,
                    )
                )
            except TypeError:
                try:
                    return _cash(
                        fee_model(
                            signal,
                            trade,
                            yes_price_cents,
                            no_price_cents,
                            execution_price_cents,
                        )
                    )
                except TypeError:
                    return _cash(fee_model(signal, trade, yes_price_cents, no_price_cents))

    @staticmethod
    def _resolve_market_ticker(ticker: str | None, ordered_trades: list[dict[str, Any]]) -> str | None:
        if ticker:
            return ticker
        for trade in ordered_trades:
            market_ticker = trade.get("ticker") or trade.get("market_ticker")
            if market_ticker:
                return str(market_ticker)
        return None

    def run(
        self,
        trades: Iterable[dict[str, Any]],
        strategy: StrategyProtocol | Callable[[BacktestContext, dict[str, Any]], StrategyResult],
        *,
        ticker: str | None = None,
        initial_cash_cents: int | float | str | Decimal = 0,
        initial_yes_position: int | float | str | Decimal = 0,
        initial_no_position: int | float | str | Decimal = 0,
    ) -> BacktestResult:
        ordered_trades = sorted(trades, key=_trade_sort_key)
        resolved_market_ticker = self._resolve_market_ticker(ticker, ordered_trades)
        replay_trades: list[dict[str, Any]] = []
        for trade in ordered_trades:
            if resolved_market_ticker is not None and trade.get("ticker") is None and trade.get("market_ticker") is None:
                normalized_trade = dict(trade)
                normalized_trade["ticker"] = resolved_market_ticker
                replay_trades.append(normalized_trade)
            else:
                replay_trades.append(trade)
        yes_position = quantize_count(initial_yes_position)
        no_position = quantize_count(initial_no_position)
        if resolved_market_ticker is None and (yes_position > 0 or no_position > 0):
            raise ValueError("ticker is required when initial positions are provided")

        legacy_context = BacktestContext(
            ticker=ticker or resolved_market_ticker,
            cash_cents=_cash(initial_cash_cents),
            yes_position=yes_position,
            no_position=no_position,
        )
        adapter = _CompatibilityStrategyAdapter(self, strategy, legacy_context)
        engine = EventDrivenBacktestEngine(
            fill_model=_LegacyFillModel(self.execution_model),
            fee_model=_LegacyFeeModelAdapter(self.fee_model),
        )

        initial_positions: dict[str, dict[str, Decimal]] | None = None
        if resolved_market_ticker is not None and (yes_position > 0 or no_position > 0):
            initial_positions = {
                resolved_market_ticker: {
                    "yes_quantity": yes_position,
                    "no_quantity": no_position,
                    "yes_average_cost_cents": Decimal("0.00"),
                    "no_average_cost_cents": Decimal("0.00"),
                }
            }

        run_result = engine.run(
            HistoricalTradeReplay.from_trade_dicts(replay_trades),
            adapter,
            initial_cash_cents=initial_cash_cents,
            initial_positions=initial_positions,
        )

        total_fees_cents = sum((fill.fee_cents for fill in legacy_context.fills), Decimal("0.00")).quantize(CENT)
        return BacktestResult(
            ticker=ticker or resolved_market_ticker,
            initial_cash_cents=_cash(initial_cash_cents),
            final_cash_cents=legacy_context.cash_cents,
            final_equity_cents=run_result.final_equity_cents,
            total_fees_cents=total_fees_cents,
            net_profit_cents=(run_result.final_equity_cents - _cash(initial_cash_cents)).quantize(CENT),
            max_drawdown_cents=_calculate_max_drawdown(legacy_context.marks),
            yes_position=legacy_context.yes_position,
            no_position=legacy_context.no_position,
            orders=list(legacy_context.orders),
            fills=list(legacy_context.fills),
            marks=list(legacy_context.marks),
        )

    def run_on_historical_trades(
        self,
        ticker: str,
        strategy: StrategyProtocol | Callable[[BacktestContext, dict[str, Any]], StrategyResult],
        *,
        min_ts: Any | None = None,
        max_ts: Any | None = None,
        batch_size: int = 1000,
        initial_cash_cents: int | float | str | Decimal = 0,
        initial_yes_position: int | float | str | Decimal = 0,
        initial_no_position: int | float | str | Decimal = 0,
    ) -> BacktestResult:
        trades = load_historical_trades(
            ticker,
            min_ts=min_ts,
            max_ts=max_ts,
            batch_size=batch_size,
        )
        return self.run(
            trades,
            strategy,
            ticker=ticker,
            initial_cash_cents=initial_cash_cents,
            initial_yes_position=initial_yes_position,
            initial_no_position=initial_no_position,
        )


class ReplayBacktester:
    """High-level backtester for merged replay event streams and captured datasets."""

    def __init__(
        self,
        *,
        fill_model: Any | None = None,
        fee_model: Any | None = None,
    ) -> None:
        self.fill_model = fill_model
        self.fee_model = fee_model

    @staticmethod
    def _resolve_market_ticker(market_ticker: str | None, replay_events: Iterable[dict[str, Any]]) -> str | None:
        if market_ticker is not None:
            return market_ticker
        for event in replay_events:
            event_market_ticker = event.get("market_ticker")
            if event_market_ticker:
                return str(event_market_ticker)
        return None

    @staticmethod
    def _build_initial_positions(
        *,
        market_ticker: str | None,
        initial_positions: dict[str, dict[str, Any]] | None,
        initial_yes_position: int | float | str | Decimal,
        initial_no_position: int | float | str | Decimal,
    ) -> dict[str, dict[str, Any]] | None:
        yes_position = quantize_count(initial_yes_position)
        no_position = quantize_count(initial_no_position)
        if initial_positions is not None and (yes_position > 0 or no_position > 0):
            raise ValueError("Provide either initial_positions or initial_yes/no_position, not both")
        if initial_positions is not None:
            return initial_positions
        if yes_position <= 0 and no_position <= 0:
            return None
        if market_ticker is None:
            raise ValueError("market_ticker is required when initial_yes_position or initial_no_position is provided")
        return {
            market_ticker: {
                "yes_quantity": yes_position,
                "no_quantity": no_position,
                "yes_average_cost_cents": Decimal("0.00"),
                "no_average_cost_cents": Decimal("0.00"),
            }
        }

    def run_on_replay_event_stream(
        self,
        replay_events: Iterable[dict[str, Any]],
        strategy: KalshiStrategy,
        *,
        market_ticker: str | None = None,
        initial_cash_cents: int | float | str | Decimal = 0,
        initial_positions: dict[str, dict[str, Any]] | None = None,
        initial_yes_position: int | float | str | Decimal = 0,
        initial_no_position: int | float | str | Decimal = 0,
    ) -> BacktestRunResult:
        ordered_events = list(replay_events)
        resolved_market_ticker = self._resolve_market_ticker(market_ticker, ordered_events)
        built_initial_positions = self._build_initial_positions(
            market_ticker=resolved_market_ticker,
            initial_positions=initial_positions,
            initial_yes_position=initial_yes_position,
            initial_no_position=initial_no_position,
        )
        engine = EventDrivenBacktestEngine(
            initial_cash_cents=initial_cash_cents,
            fill_model=self.fill_model,
            fee_model=self.fee_model,
        )
        return engine.run(
            MarketDataReplay.from_market_data_events(ordered_events),
            strategy,
            initial_cash_cents=initial_cash_cents,
            initial_positions=built_initial_positions,
        )

    def run_on_captured_dataset(
        self,
        strategy: KalshiStrategy,
        *,
        market_data_source: str | Any | None = None,
        orderbook_source: str | Any | None = None,
        market_ticker: str | None = None,
        include_replayed_orderbook_levels: bool = True,
        limit: int | None = None,
        initial_cash_cents: int | float | str | Decimal = 0,
        initial_positions: dict[str, dict[str, Any]] | None = None,
        initial_yes_position: int | float | str | Decimal = 0,
        initial_no_position: int | float | str | Decimal = 0,
    ) -> BacktestRunResult:
        if market_data_source is None and orderbook_source is None:
            raise ValueError("At least one of market_data_source or orderbook_source must be provided")
        replay_events = load_replay_event_stream(
            market_data_source=market_data_source,
            orderbook_source=orderbook_source,
            market_ticker=market_ticker,
            include_replayed_orderbook_levels=include_replayed_orderbook_levels,
            limit=limit,
        )
        return self.run_on_replay_event_stream(
            replay_events,
            strategy,
            market_ticker=market_ticker,
            initial_cash_cents=initial_cash_cents,
            initial_positions=initial_positions,
            initial_yes_position=initial_yes_position,
            initial_no_position=initial_no_position,
        )
