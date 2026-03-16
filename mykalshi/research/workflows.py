from __future__ import annotations

import json
import shutil
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
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
from .storage import (
    MultiMarketDataSink,
    MultiOrderbookSink,
    ParquetMarketDataSink,
    ParquetOrderbookSink,
    SQLiteMarketDataSink,
    SQLiteOrderbookSink,
    SplitMarketCaptureSink,
)
from .websocket import KalshiWebsocketClient


SESSION_MANIFEST_NAME = "manifest.json"
DEFAULT_CAPTURE_CHANNELS = ("ticker", "trade", "orderbook_delta")


def _first_non_empty_market_ticker(*event_groups: Iterable[dict[str, Any]]) -> str | None:
    for events in event_groups:
        for event in events:
            market_ticker = event.get("market_ticker")
            if market_ticker:
                return str(market_ticker)
    return None


def _capture_event_summary(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    materialized = list(events)
    counts = Counter(str(event.get("channel") or event.get("event_type") or "unknown") for event in materialized)
    first_timestamp = materialized[0].get("captured_at") if materialized else None
    last_timestamp = materialized[-1].get("captured_at") if materialized else None
    return {
        "event_count": len(materialized),
        "channel_counts": dict(sorted(counts.items())),
        "first_timestamp": first_timestamp,
        "last_timestamp": last_timestamp,
    }


def _serialize_manifest_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, default=_serialize_manifest_value) + "\n",
        encoding="utf-8",
    )


def _normalize_capture_channels(channels: Iterable[str] | None) -> list[str]:
    normalized = []
    for channel in channels or DEFAULT_CAPTURE_CHANNELS:
        stripped = str(channel).strip()
        if stripped and stripped not in normalized:
            normalized.append(stripped)
    if not normalized:
        raise ValueError("At least one capture channel must be provided")
    return normalized


def _resolve_session_output(
    directory: Path,
    outputs: dict[str, Any],
    *,
    preferred_keys: tuple[str, ...],
    fallback_names: tuple[str, ...],
) -> Path | None:
    for key in preferred_keys:
        relative_path = outputs.get(key)
        if relative_path:
            resolved = directory / str(relative_path)
            if resolved.exists():
                return resolved
    for name in fallback_names:
        candidate = directory / name
        if candidate.exists():
            return candidate
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


@dataclass(frozen=True)
class DiscoveredSeries:
    ticker: str
    title: str | None
    category: str | None
    tags: tuple[str, ...] = ()
    raw: dict[str, Any] | None = None

    @classmethod
    def from_discovery_result(cls, payload: dict[str, Any]) -> "DiscoveredSeries":
        return cls(
            ticker=str(payload.get("ticker") or ""),
            title=payload.get("title"),
            category=payload.get("category"),
            tags=tuple(str(tag) for tag in (payload.get("tags") or []) if tag is not None),
            raw=payload,
        )

    def summary(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "title": self.title,
            "category": self.category,
            "tags": list(self.tags),
        }


@dataclass(frozen=True)
class DiscoveredEvent:
    event_ticker: str
    series_ticker: str | None
    title: str | None
    subtitle: str | None
    category: str | None
    status: str | None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_discovery_result(cls, payload: dict[str, Any]) -> "DiscoveredEvent":
        return cls(
            event_ticker=str(payload.get("event_ticker") or ""),
            series_ticker=payload.get("series_ticker"),
            title=payload.get("title"),
            subtitle=payload.get("sub_title"),
            category=payload.get("category"),
            status=payload.get("status"),
            raw=payload,
        )

    def summary(self) -> dict[str, Any]:
        return {
            "event_ticker": self.event_ticker,
            "series_ticker": self.series_ticker,
            "title": self.title,
            "subtitle": self.subtitle,
            "category": self.category,
            "status": self.status,
        }


@dataclass
class MarketUniverse:
    category: str | None
    series_ticker: str | None
    series_title: str | None
    event_ticker: str | None
    event_title: str | None
    event_subtitle: str | None
    markets: list[DiscoveredMarket] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "series_ticker": self.series_ticker,
            "series_title": self.series_title,
            "event_ticker": self.event_ticker,
            "event_title": self.event_title,
            "event_subtitle": self.event_subtitle,
            "market_count": len(self.markets),
            "market_tickers": [market.market_ticker for market in self.markets],
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


@dataclass(frozen=True)
class CaptureSession:
    directory: Path
    market_ticker: str | None
    manifest: dict[str, Any] = field(default_factory=dict)
    market_data_source: Path | None = None
    orderbook_source: Path | None = None

    @classmethod
    def from_directory(cls, directory: str | Path) -> "CaptureSession":
        session_directory = Path(directory)
        manifest_path = session_directory / SESSION_MANIFEST_NAME
        manifest: dict[str, Any] = {}
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        outputs = manifest.get("outputs", {})
        market_ticker = manifest.get("market_ticker")
        market_data_source = _resolve_session_output(
            session_directory,
            outputs,
            preferred_keys=("market_data_sqlite", "market_data_parquet_dir"),
            fallback_names=("market-data.sqlite", "market-data"),
        )
        orderbook_source = _resolve_session_output(
            session_directory,
            outputs,
            preferred_keys=("orderbook_sqlite", "orderbook_parquet_dir"),
            fallback_names=("orderbook.sqlite", "orderbook"),
        )
        return cls(
            directory=session_directory,
            market_ticker=market_ticker,
            manifest=manifest,
            market_data_source=market_data_source,
            orderbook_source=orderbook_source,
        )

    @property
    def manifest_path(self) -> Path:
        return self.directory / SESSION_MANIFEST_NAME

    def summary(self) -> dict[str, Any]:
        return {
            "directory": str(self.directory),
            "manifest_path": str(self.manifest_path),
            "market_ticker": self.market_ticker,
            "channels": list(self.manifest.get("channels", [])),
            "market_data_source": str(self.market_data_source) if self.market_data_source is not None else None,
            "orderbook_source": str(self.orderbook_source) if self.orderbook_source is not None else None,
            "capture_summary": self.manifest.get("capture_summary"),
        }

    def load_dataset(
        self,
        *,
        market_ticker: str | None = None,
        channel: str | None = None,
        include_replayed_orderbook_levels: bool = True,
        limit: int | None = None,
    ) -> ReplayDataset:
        return ReplayDataset.from_sources(
            market_data_source=self.market_data_source,
            orderbook_source=self.orderbook_source,
            market_ticker=market_ticker or self.market_ticker,
            channel=channel,
            include_replayed_orderbook_levels=include_replayed_orderbook_levels,
            limit=limit,
        )

    def backtest(
        self,
        strategy: KalshiStrategy,
        *,
        backtester: ReplayBacktester | None = None,
        market_ticker: str | None = None,
        include_replayed_orderbook_levels: bool = True,
        limit: int | None = None,
        **kwargs: Any,
    ) -> BacktestRunResult:
        dataset = self.load_dataset(
            market_ticker=market_ticker,
            include_replayed_orderbook_levels=include_replayed_orderbook_levels,
            limit=limit,
        )
        return dataset.backtest(
            strategy,
            backtester=backtester,
            **kwargs,
        )


class ResearchSession:
    """User-facing helper for common discovery, replay, and backtest workflows."""

    def __init__(
        self,
        *,
        replay_backtester_factory: Callable[[], ReplayBacktester] | None = None,
        trade_backtester_factory: Callable[[], TradeBacktester] | None = None,
        websocket_client_factory: Callable[[], KalshiWebsocketClient] | None = None,
    ) -> None:
        self._replay_backtester_factory = replay_backtester_factory or ReplayBacktester
        self._trade_backtester_factory = trade_backtester_factory or TradeBacktester
        self._websocket_client_factory = websocket_client_factory or KalshiWebsocketClient

    def search_series(self, **filters: Any) -> list[DiscoveredSeries]:
        return [DiscoveredSeries.from_discovery_result(item) for item in discovery.search_series(**filters)]

    def resolve_series(self, **filters: Any) -> DiscoveredSeries:
        return DiscoveredSeries.from_discovery_result(discovery.resolve_series(**filters))

    def search_events(self, **filters: Any) -> list[DiscoveredEvent]:
        return [DiscoveredEvent.from_discovery_result(item) for item in discovery.search_events(**filters)]

    def resolve_event(self, **filters: Any) -> DiscoveredEvent:
        return DiscoveredEvent.from_discovery_result(discovery.resolve_event(**filters))

    def search_markets(self, **filters: Any) -> list[DiscoveredMarket]:
        return [DiscoveredMarket.from_discovery_result(item) for item in discovery.search_markets(**filters)]

    def resolve_market(self, **filters: Any) -> DiscoveredMarket:
        return DiscoveredMarket.from_discovery_result(discovery.resolve_market(**filters))

    def search_market_universes(self, **filters: Any) -> list[MarketUniverse]:
        grouped: dict[tuple[str | None, str | None], MarketUniverse] = {}
        for match in self.search_markets(**filters):
            key = (match.event_ticker, match.series_ticker)
            universe = grouped.get(key)
            if universe is None:
                universe = MarketUniverse(
                    category=match.category,
                    series_ticker=match.series_ticker,
                    series_title=match.series_title,
                    event_ticker=match.event_ticker,
                    event_title=match.event_title,
                    event_subtitle=match.event_subtitle,
                    markets=[],
                )
                grouped[key] = universe
            universe.markets.append(match)
        return list(grouped.values())

    def open_capture_session(self, directory: str | Path) -> CaptureSession:
        return CaptureSession.from_directory(directory)

    def capture_market_session(
        self,
        directory: str | Path,
        *,
        market_ticker: str | None = None,
        query: str | None = None,
        category: str | None = None,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        status: str | None = None,
        series_title_contains: str | None = None,
        event_title_contains: str | None = None,
        market_title_contains: str | None = None,
        subtitle_contains: str | None = None,
        market_ticker_contains: str | None = None,
        channels: Iterable[str] | None = None,
        include_parquet: bool = False,
        max_events: int | None = None,
        duration_secs: float | None = None,
        receive_timeout: float = 30.0,
        send_initial_snapshot: bool | None = None,
        include_book_state: bool = False,
        authenticated: bool = True,
        overwrite: bool = False,
    ) -> CaptureSession:
        session_directory = Path(directory)
        if session_directory.exists() and any(session_directory.iterdir()):
            if not overwrite:
                raise FileExistsError(f"Capture session directory already exists and is not empty: {session_directory}")
            shutil.rmtree(session_directory)
        session_directory.mkdir(parents=True, exist_ok=True)

        resolved_market = None
        if market_ticker is None:
            if not any(
                value is not None
                for value in (
                    query,
                    category,
                    series_ticker,
                    event_ticker,
                    status,
                    series_title_contains,
                    event_title_contains,
                    market_title_contains,
                    subtitle_contains,
                    market_ticker_contains,
                )
            ):
                raise ValueError("Provide market_ticker or discovery filters to resolve one market for capture")
            resolved_market = self.resolve_market(
                query=query,
                category=category,
                series_ticker=series_ticker,
                event_ticker=event_ticker,
                status=status,
                series_title_contains=series_title_contains,
                event_title_contains=event_title_contains,
                market_title_contains=market_title_contains,
                subtitle_contains=subtitle_contains,
                market_ticker_contains=market_ticker_contains,
            )
            market_ticker = resolved_market.market_ticker

        normalized_channels = _normalize_capture_channels(channels)
        if "orderbook_delta" in normalized_channels and send_initial_snapshot is None:
            send_initial_snapshot = True

        market_data_sinks: list[Any] = []
        orderbook_sinks: list[Any] = []
        outputs: dict[str, str] = {}
        if any(channel != "orderbook_delta" for channel in normalized_channels):
            outputs["market_data_sqlite"] = "market-data.sqlite"
            market_data_sinks.append(SQLiteMarketDataSink(session_directory / outputs["market_data_sqlite"]))
            if include_parquet:
                outputs["market_data_parquet_dir"] = "market-data"
                market_data_sinks.append(ParquetMarketDataSink(session_directory / outputs["market_data_parquet_dir"]))
        if "orderbook_delta" in normalized_channels:
            outputs["orderbook_sqlite"] = "orderbook.sqlite"
            orderbook_sinks.append(SQLiteOrderbookSink(session_directory / outputs["orderbook_sqlite"]))
            if include_parquet:
                outputs["orderbook_parquet_dir"] = "orderbook"
                orderbook_sinks.append(ParquetOrderbookSink(session_directory / outputs["orderbook_parquet_dir"]))

        market_data_sink = None
        if len(market_data_sinks) == 1:
            market_data_sink = market_data_sinks[0]
        elif market_data_sinks:
            market_data_sink = MultiMarketDataSink(*market_data_sinks)

        orderbook_sink = None
        if len(orderbook_sinks) == 1:
            orderbook_sink = orderbook_sinks[0]
        elif orderbook_sinks:
            orderbook_sink = MultiOrderbookSink(*orderbook_sinks)

        sink = SplitMarketCaptureSink(
            market_data_sink=market_data_sink,
            orderbook_sink=orderbook_sink,
        )
        client = self._websocket_client_factory()
        try:
            events = client.capture_market_data_sync(
                channels=normalized_channels,
                market_ticker=market_ticker,
                sink=sink,
                max_events=max_events,
                duration_secs=duration_secs,
                receive_timeout=receive_timeout,
                send_initial_snapshot=send_initial_snapshot,
                include_book_state=include_book_state,
                authenticated=authenticated,
            )
        finally:
            sink.close()

        manifest = {
            "schema_version": 1,
            "capture_type": "market_session",
            "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "market_ticker": market_ticker,
            "resolved_market": resolved_market.summary() if resolved_market is not None else None,
            "channels": normalized_channels,
            "options": {
                "include_parquet": include_parquet,
                "max_events": max_events,
                "duration_secs": duration_secs,
                "receive_timeout": receive_timeout,
                "send_initial_snapshot": send_initial_snapshot,
                "include_book_state": include_book_state,
                "authenticated": authenticated,
            },
            "outputs": outputs,
            "capture_summary": _capture_event_summary(events),
        }
        _write_manifest(session_directory / SESSION_MANIFEST_NAME, manifest)
        return CaptureSession.from_directory(session_directory)

    def load_replay_dataset(
        self,
        *,
        session_dir: str | Path | None = None,
        market_data_source: str | Any | None = None,
        orderbook_source: str | Any | None = None,
        market_ticker: str | None = None,
        channel: str | None = None,
        include_replayed_orderbook_levels: bool = True,
        limit: int | None = None,
    ) -> ReplayDataset:
        if session_dir is not None:
            if market_data_source is not None or orderbook_source is not None:
                raise ValueError("Provide either session_dir or explicit market/orderbook sources, not both")
            session = self.open_capture_session(session_dir)
            return session.load_dataset(
                market_ticker=market_ticker,
                channel=channel,
                include_replayed_orderbook_levels=include_replayed_orderbook_levels,
                limit=limit,
            )
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
        session_dir: str | Path | None = None,
        market_data_source: str | Any | None = None,
        orderbook_source: str | Any | None = None,
        market_ticker: str | None = None,
        channel: str | None = None,
        include_replayed_orderbook_levels: bool = True,
        limit: int | None = None,
        **backtest_kwargs: Any,
    ) -> BacktestRunResult:
        dataset = self.load_replay_dataset(
            session_dir=session_dir,
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
