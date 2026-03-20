from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping

from .. import events, historical, market
from ..exceptions import KalshiDependencyError, KalshiHTTPError


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("pandas is required for research event analysis") from exc
    return pd


def _market_label(payload: Mapping[str, Any]) -> str:
    for key in ("yes_sub_title", "subtitle", "title", "ticker"):
        value = payload.get(key)
        if value:
            return str(value)
    return "market"


def _timestamp_value(value: str | int | float | None, fallback: str) -> int | str:
    if value is None:
        return fallback
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except ValueError:
        return str(value)


def _normalize_event_markets(markets: Iterable[Mapping[str, Any]]) -> Any:
    pd = _require_pandas()
    rows: list[dict[str, Any]] = []
    for item in markets:
        rows.append(
            {
                "market_ticker": item.get("ticker"),
                "market_title": item.get("title"),
                "market_subtitle": item.get("subtitle"),
                "yes_sub_title": item.get("yes_sub_title"),
                "no_sub_title": item.get("no_sub_title"),
                "status": item.get("status"),
                "open_time": item.get("open_time"),
                "close_time": item.get("close_time"),
                "volume": events._market_count(item, "volume"),
                "open_interest": events._market_count(item, "open_interest"),
                "liquidity": events._market_count(item, "liquidity"),
                "yes_bid": events._market_price_cents(item, "yes_bid", "yes_bid_dollars"),
                "yes_ask": events._market_price_cents(item, "yes_ask", "yes_ask_dollars"),
                "no_bid": events._market_price_cents(item, "no_bid", "no_bid_dollars"),
                "no_ask": events._market_price_cents(item, "no_ask", "no_ask_dollars"),
                "last_price": events._market_price_cents(item, "last_price", "last_price_dollars"),
                "rules_primary": item.get("rules_primary"),
                "raw": dict(item),
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty and "volume" in frame.columns:
        frame = frame.sort_values(["volume", "market_ticker"], ascending=[False, True], na_position="last").reset_index(
            drop=True
        )
    return frame


def _build_market_metric_frame(candlestick_response: dict[str, Any]) -> Any:
    pd = _require_pandas()
    frame = market.candlesticks_to_df(candlestick_response).copy()
    if frame.empty:
        return pd.DataFrame(columns=["bid", "ask", "mid", "volume", "open_interest"])
    frame["timestamp"] = pd.to_datetime(frame["end_period"], utc=True)
    frame = frame.set_index("timestamp").sort_index()
    frame["bid"] = pd.to_numeric(frame.get("yes_bid_close"), errors="coerce")
    frame["ask"] = pd.to_numeric(frame.get("yes_ask_close"), errors="coerce")
    frame["mid"] = (frame["bid"] + frame["ask"]) / 2.0
    frame["volume"] = pd.to_numeric(frame.get("volume"), errors="coerce")
    frame["open_interest"] = pd.to_numeric(frame.get("open_interest"), errors="coerce")
    return frame[["bid", "ask", "mid", "volume", "open_interest"]]


def _resolve_live_series_ticker(ticker: str) -> str:
    market_payload = market.get_market(ticker)
    market_item = market_payload.get("market", market_payload)
    event_ticker = market_item.get("event_ticker")
    if not event_ticker:
        raise LookupError(f"Unable to resolve series ticker for market {ticker}")
    event_payload = events.get_event(str(event_ticker))
    event_item = event_payload.get("event", event_payload)
    series_ticker = event_item.get("series_ticker")
    if not series_ticker:
        raise LookupError(f"Unable to resolve series ticker for market {ticker}")
    return str(series_ticker)


def load_event_market_payload(
    event_ticker: str,
    *,
    series_ticker: str | None = None,
    search_limit: int = 25,
) -> dict[str, Any]:
    """Load an event plus nested markets, with a series-scoped fallback.

    Kalshi's direct `/events/{event_ticker}` path occasionally returns the event
    payload with an empty `markets` list even when the series-scoped `/events`
    listing still includes nested markets. This helper centralizes the fallback
    so event-level research flows do not have to special-case that behavior.
    """

    response = events.get_event(event_ticker, with_nested_markets=True)
    event_item = dict(response.get("event", response))
    markets = list(response.get("markets") or event_item.get("markets") or [])
    resolved_series_ticker = series_ticker or event_item.get("series_ticker")
    if markets or not resolved_series_ticker:
        return {"event": event_item, "markets": markets}

    series_response = events.get_events(
        series_ticker=str(resolved_series_ticker),
        limit=search_limit,
        with_nested_markets=True,
    )
    for candidate in series_response.get("events", []):
        if candidate.get("event_ticker") != event_ticker:
            continue
        candidate_event = dict(candidate)
        candidate_markets = list(candidate_event.pop("markets", []) or [])
        merged_event = dict(event_item)
        merged_event.update(candidate_event)
        return {"event": merged_event, "markets": candidate_markets}

    return {"event": event_item, "markets": markets}


@dataclass(frozen=True)
class MarketHistory:
    ticker: str
    series_ticker: str | None
    source: str
    response: dict[str, Any]
    metrics: Any
    candlestick_frame: Any
    label: str | None = None

    def summary(self) -> dict[str, Any]:
        first_timestamp = None
        last_timestamp = None
        if not self.metrics.empty:
            first_timestamp = self.metrics.index.min().isoformat()
            last_timestamp = self.metrics.index.max().isoformat()
        return {
            "ticker": self.ticker,
            "series_ticker": self.series_ticker,
            "source": self.source,
            "label": self.label,
            "candlestick_count": len(self.metrics),
            "first_timestamp": first_timestamp,
            "last_timestamp": last_timestamp,
        }


@dataclass
class EventCloseup:
    event_ticker: str
    series_ticker: str | None
    event: dict[str, Any]
    markets: Any
    histories: dict[str, MarketHistory] = field(default_factory=dict)
    panel: Any = None

    def summary(self) -> dict[str, Any]:
        return {
            "event_ticker": self.event_ticker,
            "series_ticker": self.series_ticker,
            "market_count": len(self.markets),
            "history_count": len(self.histories),
            "panel_rows": 0 if self.panel is None else len(self.panel),
        }

    def metric_frame(self, metric: str = "mid") -> Any:
        pd = _require_pandas()
        if self.panel is None or self.panel.empty:
            return pd.DataFrame()
        suffix = f"_{metric}"
        columns = [column for column in self.panel.columns if column.endswith(suffix)]
        frame = self.panel[columns].copy()
        frame.columns = [column[: -len(suffix)] for column in columns]
        return frame


def load_market_history(
    ticker: str,
    *,
    series_ticker: str | None = None,
    label: str | None = None,
    period_interval: str = "h",
    start_ts: str | None = None,
    end_ts: str | None = None,
) -> MarketHistory:
    source = "live"
    resolved_series_ticker = series_ticker
    try:
        if resolved_series_ticker is None:
            resolved_series_ticker = _resolve_live_series_ticker(ticker)
        response = market.get_full_market(
            series_ticker=resolved_series_ticker,
            ticker=ticker,
            period_interval=period_interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )
    except KalshiHTTPError as exc:
        if exc.status_code != 404:
            raise
        source = "historical"
        historical_market = historical.get_historical_market(ticker).get("market", {})
        response = {
            "ticker": ticker,
            "candlesticks": historical.get_historical_market_candlesticks(
                ticker,
                start_ts=_timestamp_value(start_ts or historical_market.get("open_time"), "01/01/1970 00:00:00"),
                end_ts=_timestamp_value(end_ts or historical_market.get("close_time"), "12/31/2100 23:59:59"),
                period_interval={"m": 1, "h": 60, "d": 1440}.get(period_interval.lower(), period_interval),
            ).get("candlesticks", []),
        }
        resolved_series_ticker = None
    metrics = _build_market_metric_frame(response)
    candlestick_frame = market.build_candlestick(response)
    return MarketHistory(
        ticker=ticker,
        series_ticker=resolved_series_ticker,
        source=source,
        response=response,
        metrics=metrics,
        candlestick_frame=candlestick_frame,
        label=label,
    )


def build_market_comparison_panel(
    histories: Mapping[str, MarketHistory] | Iterable[MarketHistory],
    *,
    forward_fill_on_volume_threshold: float | None = None,
) -> Any:
    pd = _require_pandas()
    if isinstance(histories, Mapping):
        history_items = list(histories.items())
    else:
        history_items = []
        for history in histories:
            key = history.label or history.ticker
            history_items.append((key, history))

    if not history_items:
        return pd.DataFrame()

    panel = pd.DataFrame()
    all_indices = sorted(
        set().union(*(history.metrics.index for _, history in history_items if not history.metrics.empty))
    )
    if not all_indices:
        return pd.DataFrame()
    panel = pd.DataFrame(index=pd.DatetimeIndex(all_indices))
    panel.index.name = "timestamp"

    for label, history in history_items:
        metrics = history.metrics.groupby(level=0).mean().reindex(panel.index)
        renamed = metrics.rename(
            columns={
                "bid": f"{label}_bid",
                "ask": f"{label}_ask",
                "mid": f"{label}_mid",
                "volume": f"{label}_volume",
                "open_interest": f"{label}_open_interest",
            }
        )
        panel = pd.concat([panel, renamed], axis=1)

    if forward_fill_on_volume_threshold is not None:
        for column in list(panel.columns):
            if not column.endswith("_mid"):
                continue
            volume_column = column.removesuffix("_mid") + "_volume"
            if volume_column not in panel.columns:
                continue
            values = panel[column].copy()
            volumes = panel[volume_column]
            for index in range(1, len(values)):
                if pd.isna(values.iloc[index]) and pd.notna(volumes.iloc[index - 1]) and volumes.iloc[index - 1] > forward_fill_on_volume_threshold:
                    values.iloc[index] = values.iloc[index - 1]
            panel[column] = values

    panel["aggregate_volume"] = panel[[c for c in panel.columns if c.endswith("_volume")]].sum(axis=1)
    panel["aggregate_mid"] = panel[[c for c in panel.columns if c.endswith("_mid")]].sum(axis=1)
    return panel


def build_event_closeup(
    event_ticker: str,
    *,
    period_interval: str = "h",
    start_ts: str | None = None,
    end_ts: str | None = None,
    market_limit: int | None = None,
    forward_fill_on_volume_threshold: float | None = 200.0,
) -> EventCloseup:
    event_payload = load_event_market_payload(event_ticker)
    event_item = dict(event_payload.get("event", {}))
    raw_markets = list(event_payload.get("markets") or [])
    if not raw_markets:
        raw_markets = historical.get_all_historical_markets(event_ticker=event_ticker)
    markets_frame = _normalize_event_markets(raw_markets)
    if market_limit is not None and market_limit >= 0:
        markets_frame = markets_frame.head(market_limit).reset_index(drop=True)

    histories: dict[str, MarketHistory] = {}
    for row in markets_frame.to_dict(orient="records"):
        ticker = str(row["market_ticker"])
        label = _market_label(row)
        history = load_market_history(
            ticker,
            series_ticker=event_item.get("series_ticker"),
            label=label,
            period_interval=period_interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        histories[ticker] = history

    panel = build_market_comparison_panel(histories.values(), forward_fill_on_volume_threshold=forward_fill_on_volume_threshold)
    return EventCloseup(
        event_ticker=event_ticker,
        series_ticker=event_item.get("series_ticker"),
        event=event_item,
        markets=markets_frame,
        histories=histories,
        panel=panel,
    )


def plot_event_closeup(
    closeup: EventCloseup,
    *,
    metric: str = "mid",
    include_aggregate: bool = True,
    ax: Any | None = None,
    title: str | None = None,
) -> tuple[Any, Any]:
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("matplotlib is required to plot event closeups") from exc

    frame = closeup.metric_frame(metric=metric)
    if frame.empty:
        raise ValueError("Event closeup does not contain any market history to plot")

    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 6))
    else:
        fig = ax.figure

    for column in frame.columns:
        ax.plot(frame.index, frame[column], label=column)

    if include_aggregate and "aggregate_mid" in closeup.panel.columns and metric == "mid":
        ax.plot(closeup.panel.index, closeup.panel["aggregate_mid"], label="aggregate_mid", linewidth=2.0, linestyle="--")

    ax.set_title(title or closeup.event.get("title") or closeup.event_ticker)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel(f"{metric.title()} (cents)")
    ax.legend(loc="best")
    ax.grid(True)
    fig.tight_layout()
    return fig, ax
