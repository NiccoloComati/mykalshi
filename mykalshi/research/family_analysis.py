from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .. import market
from ..exceptions import KalshiDependencyError
from .charts import plot_market_comparison
from .event_analysis import MarketHistory, build_market_comparison_panel, load_market_history


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("pandas is required for family analysis") from exc
    return pd


@dataclass
class MarketFamilyAnalysis:
    series_ticker: str
    markets: Any
    histories: dict[str, MarketHistory] = field(default_factory=dict)
    panel: Any = None

    def summary(self) -> dict[str, Any]:
        return {
            "series_ticker": self.series_ticker,
            "market_count": len(self.markets),
            "history_count": len(self.histories),
            "panel_rows": 0 if self.panel is None else len(self.panel),
        }


def _normalize_market_rows(markets: list[dict[str, Any]]) -> Any:
    pd = _require_pandas()
    frame = pd.json_normalize(markets)
    if frame.empty:
        return frame
    for target, source in {
        "volume": "volume_fp",
        "open_interest": "open_interest_fp",
        "liquidity": "liquidity_dollars",
    }.items():
        if target not in frame.columns and source in frame.columns:
            frame[target] = pd.to_numeric(frame[source], errors="coerce")
    for target, source in {
        "yes_bid": "yes_bid_dollars",
        "yes_ask": "yes_ask_dollars",
        "last_price": "last_price_dollars",
    }.items():
        if target not in frame.columns and source in frame.columns:
            frame[target] = pd.to_numeric(frame[source], errors="coerce") * 100
    return frame


def build_market_family_analysis(
    series_ticker: str,
    *,
    status: str | None = None,
    sort_by: str = "volume",
    top_n: int | None = None,
    period_interval: str = "h",
    start_ts: str | None = None,
    end_ts: str | None = None,
) -> MarketFamilyAnalysis:
    raw_markets = market.get_all_markets(series_ticker=series_ticker, status=status, batch_size=200)
    markets_frame = _normalize_market_rows(raw_markets)
    if not markets_frame.empty:
        ascending = sort_by == "close_time"
        if sort_by in markets_frame.columns:
            markets_frame = markets_frame.sort_values([sort_by, "ticker"], ascending=[ascending, True], na_position="last")
        if top_n is not None:
            markets_frame = markets_frame.head(top_n)
        markets_frame = markets_frame.reset_index(drop=True)

    histories: dict[str, MarketHistory] = {}
    for row in markets_frame.to_dict(orient="records"):
        ticker = str(row["ticker"])
        label = str(row.get("yes_sub_title") or row.get("subtitle") or row.get("title") or ticker)
        histories[ticker] = load_market_history(
            ticker,
            series_ticker=series_ticker,
            label=label,
            period_interval=period_interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    panel = build_market_comparison_panel(histories.values())
    return MarketFamilyAnalysis(series_ticker=series_ticker, markets=markets_frame, histories=histories, panel=panel)


def plot_market_family_comparison(
    family: MarketFamilyAnalysis,
    *,
    metric: str = "mid",
    include_aggregate: bool = False,
    ax: Any | None = None,
) -> tuple[Any, Any]:
    return plot_market_comparison(family.histories, metric=metric, include_aggregate=include_aggregate, ax=ax, title=f"Series {family.series_ticker}: {metric}")
