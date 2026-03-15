from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .. import discovery
from ..exceptions import KalshiDependencyError
from .backtest import ReplayBacktester, TradeBacktester
from .datasets import (
    load_market_data_events,
    load_orderbook_events,
    load_replay_event_stream,
    market_data_events_to_dataframe,
    orderbook_events_to_dataframe,
)
from .engine import BacktestRunResult, KalshiStrategy


def _first_non_empty_market_ticker(*event_groups: Iterable[dict[str, Any]]) -> str | None:
    for events in event_groups:
        for event in events:
            market_ticker = event.get("market_ticker")
            if market_ticker:
                return str(market_ticker)
    return None


@dataclass(frozen=True)
class DiscoveredMarket:
    category: str | None
    series_ticker: str | None
    series_title: str | None
    event_ticker: str | None
    event_title: str | None
    event_subtitle: str | None
    market_ticker: str
    market_title: str | None
    market_subtitle: str | None
    status: str | None
    series: dict[str, Any] | None = None
    event: dict[str, Any] | None = None
    market: dict[str, Any] | None = None

    @classmethod
    def from_discovery_result(cls, payload: dict[str, Any]) -> "DiscoveredMarket":
        return cls(
            category=payload.get("category"),
            series_ticker=payload.get("series_ticker"),
            series_title=payload.get("series_title"),
            event_ticker=payload.get("event_ticker"),
            event_title=payload.get("event_title"),
            event_subtitle=payload.get("event_subtitle"),
            market_ticker=str(payload.get("market_ticker") or ""),
            market_title=payload.get("market_title"),
            market_subtitle=payload.get("market_subtitle"),
            status=payload.get("status"),
            series=payload.get("series"),
            event=payload.get("event"),
            market=payload.get("market"),
        )

    def summary(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "series_ticker": self.series_ticker,
            "series_title": self.series_title,
            "event_ticker": self.event_ticker,
            "event_title": self.event_title,
            "market_ticker": self.market_ticker,
            "market_title": self.market_title,
            "market_subtitle": self.market_subtitle,
            "status": self.status,
        }


@dataclass
class ReplayDataset:
    market_ticker: str | None
    market_data_events: list[dict[str, Any]] = field(default_factory=list)
    orderbook_events: list[dict[str, Any]] = field(default_factory=list)
    replay_events: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_sources(
        cls,
        *,
        market_data_source: str | Any | None = None,
        orderbook_source: str | Any | None = None,
        market_ticker: str | None = None,
        channel: str | None = None,
        include_replayed_orderbook_levels: bool = True,
        limit: int | None = None,
    ) -> "ReplayDataset":
        if market_data_source is None and orderbook_source is None:
            raise ValueError("At least one of market_data_source or orderbook_source must be provided")

        market_data_events: list[dict[str, Any]] = []
        if market_data_source is not None:
            market_data_events = load_market_data_events(
                market_data_source,
                market_ticker=market_ticker,
                channel=channel,
                limit=None,
            )

        orderbook_events: list[dict[str, Any]] = []
        if orderbook_source is not None:
            orderbook_events = load_orderbook_events(
                orderbook_source,
                market_ticker=market_ticker,
                limit=None,
            )

        replay_events = load_replay_event_stream(
            market_data_source=market_data_events,
            orderbook_source=orderbook_events,
            market_ticker=market_ticker,
            include_replayed_orderbook_levels=include_replayed_orderbook_levels,
            limit=limit,
        )
        resolved_market_ticker = market_ticker or _first_non_empty_market_ticker(
            replay_events,
            market_data_events,
            orderbook_events,
        )
        return cls(
            market_ticker=resolved_market_ticker,
            market_data_events=market_data_events,
            orderbook_events=orderbook_events,
            replay_events=replay_events,
        )

    @property
    def channel_counts(self) -> dict[str, int]:
        counts = Counter(str(event.get("channel") or event.get("event_type") or "unknown") for event in self.replay_events)
        return dict(sorted(counts.items()))

    def summary(self) -> dict[str, Any]:
        first_timestamp = self.replay_events[0].get("captured_at") if self.replay_events else None
        last_timestamp = self.replay_events[-1].get("captured_at") if self.replay_events else None
        return {
            "market_ticker": self.market_ticker,
            "market_data_event_count": len(self.market_data_events),
            "orderbook_event_count": len(self.orderbook_events),
            "replay_event_count": len(self.replay_events),
            "first_timestamp": first_timestamp,
            "last_timestamp": last_timestamp,
            "channel_counts": self.channel_counts,
        }

    def backtest(
        self,
        strategy: KalshiStrategy,
        *,
        backtester: ReplayBacktester | None = None,
        **kwargs: Any,
    ) -> BacktestRunResult:
        runner = backtester or ReplayBacktester()
        return runner.run_on_replay_event_stream(
            self.replay_events,
            strategy,
            market_ticker=self.market_ticker,
            **kwargs,
        )

    def to_dataframes(self) -> dict[str, Any]:
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            raise KalshiDependencyError("pandas is required to export replay datasets as DataFrames") from exc

        replay_frame = pd.DataFrame.from_records(self.replay_events)
        if not replay_frame.empty and "captured_at" in replay_frame.columns:
            replay_frame["captured_at"] = pd.to_datetime(replay_frame["captured_at"], utc=True)
            sort_columns = [column for column in ("captured_at", "market_ticker", "sequence") if column in replay_frame.columns]
            if sort_columns:
                replay_frame = replay_frame.sort_values(sort_columns, na_position="last")
        return {
            "market_data": market_data_events_to_dataframe(self.market_data_events),
            "orderbook": orderbook_events_to_dataframe(self.orderbook_events),
            "replay": replay_frame,
        }


class ResearchSession:
    """User-facing helper for common discovery, replay, and backtest workflows."""

    def __init__(
        self,
        *,
        replay_backtester_factory: Callable[[], ReplayBacktester] | None = None,
        trade_backtester_factory: Callable[[], TradeBacktester] | None = None,
    ) -> None:
        self._replay_backtester_factory = replay_backtester_factory or ReplayBacktester
        self._trade_backtester_factory = trade_backtester_factory or TradeBacktester

    def search_markets(self, **filters: Any) -> list[DiscoveredMarket]:
        return [DiscoveredMarket.from_discovery_result(item) for item in discovery.search_markets(**filters)]

    def resolve_market(self, **filters: Any) -> DiscoveredMarket:
        return DiscoveredMarket.from_discovery_result(discovery.resolve_market(**filters))

    def load_replay_dataset(
        self,
        *,
        market_data_source: str | Any | None = None,
        orderbook_source: str | Any | None = None,
        market_ticker: str | None = None,
        channel: str | None = None,
        include_replayed_orderbook_levels: bool = True,
        limit: int | None = None,
    ) -> ReplayDataset:
        return ReplayDataset.from_sources(
            market_data_source=market_data_source,
            orderbook_source=orderbook_source,
            market_ticker=market_ticker,
            channel=channel,
            include_replayed_orderbook_levels=include_replayed_orderbook_levels,
            limit=limit,
        )

    def run_replay_backtest(
        self,
        strategy: KalshiStrategy,
        *,
        market_data_source: str | Any | None = None,
        orderbook_source: str | Any | None = None,
        market_ticker: str | None = None,
        channel: str | None = None,
        include_replayed_orderbook_levels: bool = True,
        limit: int | None = None,
        **backtest_kwargs: Any,
    ) -> BacktestRunResult:
        dataset = self.load_replay_dataset(
            market_data_source=market_data_source,
            orderbook_source=orderbook_source,
            market_ticker=market_ticker,
            channel=channel,
            include_replayed_orderbook_levels=include_replayed_orderbook_levels,
            limit=limit,
        )
        return dataset.backtest(strategy, backtester=self._replay_backtester_factory(), **backtest_kwargs)

    def run_historical_backtest(
        self,
        ticker: str,
        strategy: Any,
        **backtest_kwargs: Any,
    ) -> Any:
        return self._trade_backtester_factory().run_on_historical_trades(ticker, strategy, **backtest_kwargs)
