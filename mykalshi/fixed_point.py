from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any


CENT = Decimal("0.01")
TWO_DP = Decimal("0.01")
FOUR_DP = Decimal("0.0001")


def to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, str):
        return Decimal(value)
    raise TypeError(f"Unsupported numeric value: {value!r}")


def dollars_to_cents(value: Any) -> int:
    return int((to_decimal(value) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def quantize_count(value: Any) -> Decimal:
    return to_decimal(value).quantize(TWO_DP)


def format_decimal(value: Decimal, *, places: int = 2) -> str:
    quantum = FOUR_DP if places == 4 else TWO_DP
    return format(value.quantize(quantum), f".{places}f")
