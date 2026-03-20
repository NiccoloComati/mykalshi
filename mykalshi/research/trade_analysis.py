from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .. import routing
from ..exceptions import KalshiDependencyError


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("pandas is required for trade analysis") from exc
    return pd


def _normalize_trade_frame(frame: Any) -> Any:
    pd = _require_pandas()
    normalized = frame.copy()
    if normalized.empty:
        return normalized
    if "created_time" in normalized.columns:
        normalized["created_time"] = pd.to_datetime(normalized["created_time"], utc=True, errors="coerce")
        normalized = normalized.sort_values("created_time").reset_index(drop=True)
    if "count_fp" in normalized.columns:
        normalized["contracts"] = pd.to_numeric(normalized["count_fp"], errors="coerce").fillna(0.0)
    else:
        normalized["contracts"] = 0.0
    if "yes_price_dollars" in normalized.columns:
        normalized["yes_price_cents"] = pd.to_numeric(normalized["yes_price_dollars"], errors="coerce") * 100.0
    elif "yes_price" in normalized.columns:
        normalized["yes_price_cents"] = pd.to_numeric(normalized["yes_price"], errors="coerce")
    else:
        normalized["yes_price_cents"] = pd.NA
    normalized["notional_cents"] = normalized["contracts"] * pd.to_numeric(
        normalized["yes_price_cents"], errors="coerce"
    ).fillna(0.0)
    return normalized


@dataclass(frozen=True)
class TradeHistory:
    ticker: str
    trades: Any
    source_info: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        return summarize_trade_history(self)


def load_trade_history(
    ticker: str,
    *,
    start_ts: Any | None = None,
    end_ts: Any | None = None,
    historical_batch_size: int = 1000,
    live_batch_size: int = 100,
    calls_per_sec: int = 30,
) -> TradeHistory:
    result = routing.get_trades_auto(
        ticker,
        start_ts=start_ts,
        end_ts=end_ts,
        historical_batch_size=historical_batch_size,
        live_batch_size=live_batch_size,
        calls_per_sec=calls_per_sec,
    )
    pd = _require_pandas()
    frame = _normalize_trade_frame(pd.DataFrame(result.get("trades", [])))
    return TradeHistory(
        ticker=ticker,
        trades=frame,
        source_info={k: v for k, v in result.items() if k != "trades"},
    )


def summarize_trade_history(history_or_frame: TradeHistory | Any, *, ticker: str | None = None) -> dict[str, Any]:
    pd = _require_pandas()
    if isinstance(history_or_frame, TradeHistory):
        ticker = history_or_frame.ticker
        frame = history_or_frame.trades
    else:
        frame = _normalize_trade_frame(history_or_frame)

    if frame.empty:
        return {
            "ticker": ticker,
            "trade_count": 0,
            "total_contracts": 0.0,
            "vwap_yes_price_cents": None,
            "avg_trade_size": None,
            "first_trade_time": None,
            "last_trade_time": None,
            "yes_taker_contract_share": None,
        }

    contracts = pd.to_numeric(frame["contracts"], errors="coerce").fillna(0.0)
    prices = pd.to_numeric(frame["yes_price_cents"], errors="coerce")
    weighted_notional = pd.to_numeric(frame["notional_cents"], errors="coerce").fillna(0.0).sum()
    total_contracts = float(contracts.sum())
    yes_taker_contracts = float(contracts[frame.get("taker_side") == "yes"].sum()) if "taker_side" in frame.columns else 0.0
    vwap = None if total_contracts <= 0 else float(weighted_notional / total_contracts)
    return {
        "ticker": ticker,
        "trade_count": int(len(frame)),
        "total_contracts": total_contracts,
        "vwap_yes_price_cents": vwap,
        "avg_trade_size": float(total_contracts / len(frame)),
        "first_trade_time": None if "created_time" not in frame.columns else frame["created_time"].min().isoformat(),
        "last_trade_time": None if "created_time" not in frame.columns else frame["created_time"].max().isoformat(),
        "yes_taker_contract_share": None if total_contracts <= 0 else float(yes_taker_contracts / total_contracts),
        "min_yes_price_cents": None if prices.dropna().empty else float(prices.min()),
        "max_yes_price_cents": None if prices.dropna().empty else float(prices.max()),
    }


def resample_trade_history(history_or_frame: TradeHistory | Any, *, freq: str = "1D") -> Any:
    pd = _require_pandas()
    if isinstance(history_or_frame, TradeHistory):
        frame = history_or_frame.trades
    else:
        frame = _normalize_trade_frame(history_or_frame)

    if frame.empty:
        return pd.DataFrame(
            columns=[
                "trade_count",
                "contracts",
                "notional_cents",
                "vwap_yes_price_cents",
                "yes_taker_contract_share",
            ]
        )
    if "created_time" not in frame.columns:
        raise ValueError("Trade history must include created_time to resample")

    indexed = frame.set_index("created_time").sort_index()
    grouped = indexed.groupby(pd.Grouper(freq=freq))
    summary = grouped.agg(
        trade_count=("contracts", "size"),
        contracts=("contracts", "sum"),
        notional_cents=("notional_cents", "sum"),
    )
    summary["vwap_yes_price_cents"] = summary["notional_cents"] / summary["contracts"].replace({0.0: pd.NA})
    if "taker_side" in indexed.columns:
        yes_taker_contracts = grouped.apply(
            lambda group: float(group.loc[group["taker_side"] == "yes", "contracts"].sum())
        )
        summary["yes_taker_contract_share"] = yes_taker_contracts / summary["contracts"].replace({0.0: pd.NA})
    else:
        summary["yes_taker_contract_share"] = pd.NA
    return summary


def plot_trade_activity(
    history_or_frame: TradeHistory | Any,
    *,
    freq: str = "1D",
    title: str | None = None,
    axes: tuple[Any, Any] | None = None,
) -> tuple[Any, tuple[Any, Any]]:
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("matplotlib is required for trade activity plots") from exc

    summary = resample_trade_history(history_or_frame, freq=freq)
    if summary.empty:
        if axes is None:
            fig, axis_tuple = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        else:
            axis_tuple = axes
            fig = axis_tuple[0].figure
        for axis in axis_tuple:
            axis.text(0.5, 0.5, "No trade history", ha="center", va="center", transform=axis.transAxes)
        fig.tight_layout()
        return fig, axis_tuple

    if axes is None:
        fig, axis_tuple = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    else:
        axis_tuple = axes
        fig = axis_tuple[0].figure
    price_axis, volume_axis = axis_tuple

    pd = _require_pandas()
    price_values = pd.to_numeric(summary["vwap_yes_price_cents"], errors="coerce")
    contract_values = pd.to_numeric(summary["contracts"], errors="coerce").fillna(0.0)

    price_axis.plot(summary.index, price_values, label="VWAP yes price", color="tab:blue")
    price_axis.set_ylabel("Price (cents)")
    price_axis.set_title(title or "Trade activity")
    price_axis.grid(True)
    price_axis.legend(loc="best")

    volume_axis.bar(summary.index, contract_values, width=0.8, color="tab:orange", label="Contracts")
    volume_axis.set_ylabel("Contracts")
    volume_axis.set_xlabel("Timestamp")
    volume_axis.grid(True)
    volume_axis.legend(loc="best")

    fig.tight_layout()
    return fig, axis_tuple
