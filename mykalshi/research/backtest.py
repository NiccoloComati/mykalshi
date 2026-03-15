from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Iterable, Protocol, Sequence

from .. import historical
from ..fixed_point import dollars_to_cents, format_decimal, quantize_count


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


@dataclass
class BacktestResult:
    ticker: str | None
    initial_cash_cents: Decimal
    final_cash_cents: Decimal
    final_equity_cents: Decimal
    yes_position: Decimal
    no_position: Decimal
    fills: list[BacktestFill]
    marks: list[BacktestMark]

    def summary(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "initial_cash_cents": format_decimal(self.initial_cash_cents),
            "final_cash_cents": format_decimal(self.final_cash_cents),
            "final_equity_cents": format_decimal(self.final_equity_cents),
            "yes_position": format_decimal(self.yes_position),
            "no_position": format_decimal(self.no_position),
            "fill_count": len(self.fills),
            "mark_count": len(self.marks),
        }


FeeModel = Callable[[TradeSignal, dict[str, Any], int, int], int | float | str | Decimal]


class StrategyProtocol(Protocol):
    def on_trade(self, context: "BacktestContext", trade: dict[str, Any]) -> TradeSignal | Sequence[TradeSignal] | None:
        ...


@dataclass
class BacktestContext:
    ticker: str | None
    cash_cents: Decimal
    yes_position: Decimal = field(default_factory=lambda: Decimal("0.00"))
    no_position: Decimal = field(default_factory=lambda: Decimal("0.00"))
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
        yes_price_cents: int,
        no_price_cents: int,
        fee_model: FeeModel | None = None,
    ) -> BacktestFill:
        quantity = quantize_count(signal.quantity)
        if quantity <= 0:
            raise ValueError("TradeSignal.quantity must be positive")

        action = signal.action.lower()
        price_cents = yes_price_cents if action.endswith("yes") else no_price_cents
        fee_cents = _cash(fee_model(signal, trade, yes_price_cents, no_price_cents) if fee_model else 0)
        gross_cash_change = quantity * Decimal(price_cents)

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
            price_cents=price_cents,
            fee_cents=fee_cents,
            cash_after_cents=self.cash_cents,
            yes_position=self.yes_position,
            no_position=self.no_position,
            note=signal.note,
        )
        self.fills.append(fill)
        return fill


class TradeBacktester:
    def __init__(self, *, fee_model: FeeModel | None = None) -> None:
        self.fee_model = fee_model

    @staticmethod
    def _normalize_signals(result: TradeSignal | Sequence[TradeSignal] | None) -> list[TradeSignal]:
        if result is None:
            return []
        if isinstance(result, TradeSignal):
            return [result]
        return list(result)

    @staticmethod
    def _call_strategy(
        strategy: StrategyProtocol | Callable[[BacktestContext, dict[str, Any]], TradeSignal | Sequence[TradeSignal] | None],
        context: BacktestContext,
        trade: dict[str, Any],
    ) -> TradeSignal | Sequence[TradeSignal] | None:
        if hasattr(strategy, "on_trade"):
            return strategy.on_trade(context, trade)
        return strategy(context, trade)

    def run(
        self,
        trades: Iterable[dict[str, Any]],
        strategy: StrategyProtocol | Callable[[BacktestContext, dict[str, Any]], TradeSignal | Sequence[TradeSignal] | None],
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

        last_mark: BacktestMark | None = None
        for trade in ordered_trades:
            yes_price_cents, no_price_cents = _extract_trade_prices(trade)
            signals = self._normalize_signals(self._call_strategy(strategy, context, trade))
            for signal in signals:
                context.execute(
                    signal,
                    trade,
                    yes_price_cents=yes_price_cents,
                    no_price_cents=no_price_cents,
                    fee_model=self.fee_model,
                )
            last_mark = context.mark(
                str(trade.get("created_time") or trade.get("ts") or ""),
                yes_price_cents,
                no_price_cents,
            )

        final_equity = last_mark.equity_cents if last_mark is not None else context.cash_cents
        return BacktestResult(
            ticker=ticker,
            initial_cash_cents=_cash(initial_cash_cents),
            final_cash_cents=context.cash_cents,
            final_equity_cents=final_equity,
            yes_position=context.yes_position,
            no_position=context.no_position,
            fills=list(context.fills),
            marks=list(context.marks),
        )

    def run_on_historical_trades(
        self,
        ticker: str,
        strategy: StrategyProtocol | Callable[[BacktestContext, dict[str, Any]], TradeSignal | Sequence[TradeSignal] | None],
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
