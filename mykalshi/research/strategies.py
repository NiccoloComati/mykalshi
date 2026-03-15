from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from ..fixed_point import to_decimal
from .backtest import BacktestContext, PositionTargetSignal, _extract_trade_prices


SignalValue = int | float | str | Decimal
SignalFunction = Callable[[BacktestContext, dict[str, Any]], SignalValue]


def _signal_decimal(value: SignalValue) -> Decimal:
    return to_decimal(value).quantize(Decimal("0.0001"))


def _probability_to_cents(value: SignalValue) -> Decimal:
    decimal_value = _signal_decimal(value)
    if Decimal("0") <= decimal_value <= Decimal("1"):
        return (decimal_value * 100).quantize(Decimal("0.01"))
    if Decimal("0") <= decimal_value <= Decimal("100"):
        return decimal_value.quantize(Decimal("0.01"))
    raise ValueError("Probability estimates must be between 0 and 1 or between 0 and 100 cents")


def _signal_note(prefix: str | None, value: Decimal, *, label: str = "signal") -> str:
    base = f"{label}={format(value, 'f')}"
    if prefix:
        return f"{prefix} {base}"
    return base


def target_yes(
    quantity: SignalValue = 1,
    *,
    entry_limit_price_cents: int | None = None,
    exit_limit_price_cents: int | None = None,
    max_trade_quantity: SignalValue | None = None,
    slippage_cents: int = 0,
    tag: str | None = None,
    note: str | None = None,
) -> PositionTargetSignal:
    return PositionTargetSignal(
        side="yes",
        target_quantity=quantity,
        entry_limit_price_cents=entry_limit_price_cents,
        exit_limit_price_cents=exit_limit_price_cents,
        max_trade_quantity=max_trade_quantity,
        slippage_cents=slippage_cents,
        tag=tag,
        note=note,
    )


def target_no(
    quantity: SignalValue = 1,
    *,
    entry_limit_price_cents: int | None = None,
    exit_limit_price_cents: int | None = None,
    max_trade_quantity: SignalValue | None = None,
    slippage_cents: int = 0,
    tag: str | None = None,
    note: str | None = None,
) -> PositionTargetSignal:
    return PositionTargetSignal(
        side="no",
        target_quantity=quantity,
        entry_limit_price_cents=entry_limit_price_cents,
        exit_limit_price_cents=exit_limit_price_cents,
        max_trade_quantity=max_trade_quantity,
        slippage_cents=slippage_cents,
        tag=tag,
        note=note,
    )


def target_flat(
    *,
    exit_limit_price_cents: int | None = None,
    max_trade_quantity: SignalValue | None = None,
    slippage_cents: int = 0,
    tag: str | None = None,
    note: str | None = None,
) -> PositionTargetSignal:
    return PositionTargetSignal(
        side="flat",
        target_quantity=0,
        exit_limit_price_cents=exit_limit_price_cents,
        max_trade_quantity=max_trade_quantity,
        slippage_cents=slippage_cents,
        tag=tag,
        note=note,
    )


@dataclass
class ThresholdSignalStrategy:
    signal_fn: SignalFunction
    yes_threshold: SignalValue
    no_threshold: SignalValue
    target_quantity: SignalValue = 1
    max_trade_quantity: SignalValue | None = None
    entry_limit_price_cents: int | None = None
    exit_limit_price_cents: int | None = None
    slippage_cents: int = 0
    tag: str | None = None
    note_prefix: str | None = None

    def __post_init__(self) -> None:
        if _signal_decimal(self.yes_threshold) <= _signal_decimal(self.no_threshold):
            raise ValueError("yes_threshold must be greater than no_threshold")

    def on_trade(self, context: BacktestContext, trade: dict[str, Any]) -> PositionTargetSignal | None:
        signal_value = _signal_decimal(self.signal_fn(context, trade))
        note = _signal_note(self.note_prefix, signal_value)

        if signal_value >= _signal_decimal(self.yes_threshold):
            return target_yes(
                self.target_quantity,
                entry_limit_price_cents=self.entry_limit_price_cents,
                exit_limit_price_cents=self.exit_limit_price_cents,
                max_trade_quantity=self.max_trade_quantity,
                slippage_cents=self.slippage_cents,
                tag=self.tag,
                note=note,
            )
        if signal_value <= _signal_decimal(self.no_threshold):
            return target_no(
                self.target_quantity,
                entry_limit_price_cents=self.entry_limit_price_cents,
                exit_limit_price_cents=self.exit_limit_price_cents,
                max_trade_quantity=self.max_trade_quantity,
                slippage_cents=self.slippage_cents,
                tag=self.tag,
                note=note,
            )
        if context.yes_position == 0 and context.no_position == 0:
            return None
        return target_flat(
            exit_limit_price_cents=self.exit_limit_price_cents,
            max_trade_quantity=self.max_trade_quantity,
            slippage_cents=self.slippage_cents,
            tag=self.tag,
            note=note,
        )


@dataclass
class ProbabilityEdgeStrategy:
    probability_fn: SignalFunction
    enter_edge_cents: SignalValue
    exit_edge_cents: SignalValue = 0
    target_quantity: SignalValue = 1
    max_trade_quantity: SignalValue | None = None
    entry_limit_price_cents: int | None = None
    exit_limit_price_cents: int | None = None
    slippage_cents: int = 0
    tag: str | None = None
    note_prefix: str | None = None

    def __post_init__(self) -> None:
        if _signal_decimal(self.enter_edge_cents) < 0:
            raise ValueError("enter_edge_cents must be non-negative")
        if _signal_decimal(self.exit_edge_cents) < 0:
            raise ValueError("exit_edge_cents must be non-negative")
        if _signal_decimal(self.exit_edge_cents) > _signal_decimal(self.enter_edge_cents):
            raise ValueError("exit_edge_cents must be less than or equal to enter_edge_cents")

    def on_trade(self, context: BacktestContext, trade: dict[str, Any]) -> PositionTargetSignal | None:
        estimate_yes_cents = _probability_to_cents(self.probability_fn(context, trade))
        market_yes_cents, _ = _extract_trade_prices(trade)
        edge_cents = (estimate_yes_cents - Decimal(market_yes_cents)).quantize(Decimal("0.01"))
        note = _signal_note(self.note_prefix, edge_cents, label="edge_cents")

        if edge_cents >= _signal_decimal(self.enter_edge_cents):
            return target_yes(
                self.target_quantity,
                entry_limit_price_cents=self.entry_limit_price_cents,
                exit_limit_price_cents=self.exit_limit_price_cents,
                max_trade_quantity=self.max_trade_quantity,
                slippage_cents=self.slippage_cents,
                tag=self.tag,
                note=note,
            )
        if edge_cents <= -_signal_decimal(self.enter_edge_cents):
            return target_no(
                self.target_quantity,
                entry_limit_price_cents=self.entry_limit_price_cents,
                exit_limit_price_cents=self.exit_limit_price_cents,
                max_trade_quantity=self.max_trade_quantity,
                slippage_cents=self.slippage_cents,
                tag=self.tag,
                note=note,
            )
        if abs(edge_cents) <= _signal_decimal(self.exit_edge_cents):
            if context.yes_position == 0 and context.no_position == 0:
                return None
            return target_flat(
                exit_limit_price_cents=self.exit_limit_price_cents,
                max_trade_quantity=self.max_trade_quantity,
                slippage_cents=self.slippage_cents,
                tag=self.tag,
                note=note,
            )
        return None
