from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Callable, Iterable, Protocol, Sequence

from .. import historical
from ..fixed_point import dollars_to_cents, format_decimal, quantize_count


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
                )
            )
        except TypeError:
            return _cash(fee_model(signal, trade, yes_price_cents, no_price_cents))

    def _process_trade_signal(
        self,
        context: BacktestContext,
        trade: dict[str, Any],
        signal: TradeSignal,
        *,
        yes_price_cents: int,
        no_price_cents: int,
    ) -> BacktestOrder:
        timestamp = str(trade.get("created_time") or trade.get("ts") or "")
        quantity = quantize_count(signal.quantity)
        if quantity <= 0:
            raise ValueError("TradeSignal.quantity must be positive")

        decision = self.execution_model.evaluate(
            signal,
            trade,
            yes_price_cents=yes_price_cents,
            no_price_cents=no_price_cents,
        )
        if decision.status != "filled" or decision.execution_price_cents is None:
            order = BacktestOrder(
                timestamp=timestamp,
                action=signal.action.lower(),
                quantity=quantity,
                requested_limit_price_cents=signal.limit_price_cents,
                execution_price_cents=None,
                status=decision.status,
                reason=decision.reason,
                tag=signal.tag,
                note=signal.note,
            )
            context.orders.append(order)
            return order

        fee_cents = self._call_fee_model(
            self.fee_model,
            signal,
            trade,
            yes_price_cents,
            no_price_cents,
            decision.execution_price_cents,
        )
        try:
            context.execute(
                signal,
                trade,
                execution_price_cents=decision.execution_price_cents,
                fee_cents=fee_cents,
            )
        except ValueError as exc:
            order = BacktestOrder(
                timestamp=timestamp,
                action=signal.action.lower(),
                quantity=quantity,
                requested_limit_price_cents=signal.limit_price_cents,
                execution_price_cents=decision.execution_price_cents,
                status="rejected",
                reason=str(exc),
                tag=signal.tag,
                note=signal.note,
            )
            context.orders.append(order)
            return order

        order = BacktestOrder(
            timestamp=timestamp,
            action=signal.action.lower(),
            quantity=quantity,
            requested_limit_price_cents=signal.limit_price_cents,
            execution_price_cents=decision.execution_price_cents,
            status=decision.status,
            fee_cents=fee_cents,
            reason=decision.reason,
            tag=signal.tag,
            note=signal.note,
        )
        context.orders.append(order)
        return order

    def _process_position_target_signal(
        self,
        context: BacktestContext,
        trade: dict[str, Any],
        signal: PositionTargetSignal,
        *,
        yes_price_cents: int,
        no_price_cents: int,
    ) -> None:
        remaining_trade_quantity = (
            quantize_count(signal.max_trade_quantity) if signal.max_trade_quantity is not None else None
        )
        if remaining_trade_quantity is not None and remaining_trade_quantity <= 0:
            raise ValueError("PositionTargetSignal.max_trade_quantity must be positive when provided")

        while True:
            next_signal = self._next_trade_signal_for_target(signal, context, remaining_trade_quantity)
            if next_signal is None:
                return

            order = self._process_trade_signal(
                context,
                trade,
                next_signal,
                yes_price_cents=yes_price_cents,
                no_price_cents=no_price_cents,
            )
            if order.status != "filled":
                return

            if remaining_trade_quantity is None:
                continue

            remaining_trade_quantity = quantize_count(remaining_trade_quantity - quantize_count(next_signal.quantity))
            if remaining_trade_quantity <= 0:
                return

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
        context = BacktestContext(
            ticker=ticker,
            cash_cents=_cash(initial_cash_cents),
            yes_position=quantize_count(initial_yes_position),
            no_position=quantize_count(initial_no_position),
        )

        self._call_optional_hook(strategy, "on_start", context)
        last_mark: BacktestMark | None = None
        for trade in ordered_trades:
            yes_price_cents, no_price_cents = _extract_trade_prices(trade)
            signals = self._normalize_strategy_signals(self._call_strategy(strategy, context, trade))
            for signal in signals:
                if isinstance(signal, PositionTargetSignal):
                    self._process_position_target_signal(
                        context,
                        trade,
                        signal,
                        yes_price_cents=yes_price_cents,
                        no_price_cents=no_price_cents,
                    )
                    continue

                self._process_trade_signal(
                    context,
                    trade,
                    signal,
                    yes_price_cents=yes_price_cents,
                    no_price_cents=no_price_cents,
                )
            last_mark = context.mark(
                str(trade.get("created_time") or trade.get("ts") or ""),
                yes_price_cents,
                no_price_cents,
            )

        self._call_optional_hook(strategy, "on_finish", context)
        final_equity = last_mark.equity_cents if last_mark is not None else context.cash_cents
        total_fees_cents = sum((fill.fee_cents for fill in context.fills), Decimal("0.00")).quantize(CENT)
        return BacktestResult(
            ticker=ticker,
            initial_cash_cents=_cash(initial_cash_cents),
            final_cash_cents=context.cash_cents,
            final_equity_cents=final_equity,
            total_fees_cents=total_fees_cents,
            net_profit_cents=(final_equity - _cash(initial_cash_cents)).quantize(CENT),
            max_drawdown_cents=_calculate_max_drawdown(context.marks),
            yes_position=context.yes_position,
            no_position=context.no_position,
            orders=list(context.orders),
            fills=list(context.fills),
            marks=list(context.marks),
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
