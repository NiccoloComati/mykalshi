from __future__ import annotations

from typing import Any, Iterable, Mapping

from ..exceptions import KalshiDependencyError
from .event_analysis import MarketHistory, build_market_comparison_panel, load_market_history


def _require_matplotlib():
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("matplotlib is required for charting helpers") from exc
    return plt


def _require_mplfinance():
    try:
        import mplfinance as mpf
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("mplfinance is required for candlestick chart helpers") from exc
    return mpf


def plot_market_candles(
    history_or_ticker: MarketHistory | str,
    *,
    series_ticker: str | None = None,
    period_interval: str = "d",
    start_ts: str | None = None,
    end_ts: str | None = None,
    volume: bool = True,
    mav: int | tuple[int, ...] | None = None,
    figratio: tuple[int, int] = (16, 9),
    figscale: float = 1.4,
    show_nontrading: bool = False,
    title: str | None = None,
    hlines: Iterable[float] | None = None,
    vlines: Iterable[Any] | None = None,
) -> tuple[Any, Any]:
    mpf = _require_mplfinance()

    history = history_or_ticker
    if isinstance(history_or_ticker, str):
        history = load_market_history(
            history_or_ticker,
            series_ticker=series_ticker,
            period_interval=period_interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    plot_kwargs = {
        "volume": volume,
        "figratio": figratio,
        "figscale": figscale,
        "show_nontrading": show_nontrading,
        "returnfig": True,
        "type": "candle",
        "title": title or history.label or history.ticker,
    }
    if mav is not None:
        plot_kwargs["mav"] = mav

    fig, axes = mpf.plot(history.candlestick_frame, **plot_kwargs)
    price_axis = axes[0]
    for y in hlines or ():
        price_axis.axhline(y=y, linestyle="--", linewidth=1)
    for x in vlines or ():
        price_axis.axvline(x=x, linestyle="--", linewidth=1)
    return fig, axes


def plot_market_comparison(
    histories: Mapping[str, MarketHistory] | Iterable[MarketHistory],
    *,
    metric: str = "mid",
    include_aggregate: bool = False,
    ax: Any | None = None,
    title: str | None = None,
) -> tuple[Any, Any]:
    plt = _require_matplotlib()
    panel = build_market_comparison_panel(histories)
    suffix = f"_{metric}"
    columns = [column for column in panel.columns if column.endswith(suffix)]
    if not columns:
        raise ValueError(f"No market comparison metric columns found for {metric!r}")

    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 6))
    else:
        fig = ax.figure

    for column in columns:
        label = column[: -len(suffix)]
        ax.plot(panel.index, panel[column], label=label)

    if include_aggregate and metric == "mid" and "aggregate_mid" in panel.columns:
        ax.plot(panel.index, panel["aggregate_mid"], label="aggregate_mid", linewidth=2.0, linestyle="--")

    ax.set_title(title or f"Market comparison: {metric}")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel(f"{metric.title()} (cents)")
    ax.legend(loc="best")
    ax.grid(True)
    fig.tight_layout()
    return fig, ax
