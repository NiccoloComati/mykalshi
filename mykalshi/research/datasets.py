from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

from ..orderbook import OrderbookState
from .storage import (
    ParquetMarketDataSink,
    ParquetOrderbookSink,
    SQLiteMarketDataSink,
    SQLiteOrderbookSink,
)


SQLITE_SUFFIXES = {".db", ".sqlite", ".sqlite3"}


def _apply_filters(
    events: Iterable[dict[str, Any]],
    *,
    market_ticker: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    filtered = []
    for event in events:
        if market_ticker is not None and event.get("market_ticker") != market_ticker:
            continue
        filtered.append(event)
        if limit is not None and len(filtered) >= limit:
            break
    return filtered


def _sort_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        events,
        key=lambda event: (
            str(event.get("captured_at") or ""),
            str(event.get("market_ticker") or ""),
            int(event.get("sequence") or 0),
        ),
    )


def _sqlite_has_table(path: Path, table_name: str) -> bool:
    connection = sqlite3.connect(path)
    try:
        cursor = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        )
        return cursor.fetchone() is not None
    finally:
        connection.close()


def load_orderbook_events(
    source: str | Path | Iterable[dict[str, Any]] | Any,
    *,
    market_ticker: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    if isinstance(source, (list, tuple)):
        return _apply_filters(source, market_ticker=market_ticker, limit=limit)

    if hasattr(source, "load_events"):
        try:
            return source.load_events(market_ticker=market_ticker, limit=limit)
        except TypeError:
            return _apply_filters(
                source.load_events(),
                market_ticker=market_ticker,
                limit=limit,
            )

    path = Path(source)
    if path.is_dir():
        with ParquetOrderbookSink(path) as sink:
            return sink.load_events(market_ticker=market_ticker, limit=limit)

    if path.suffix.lower() in SQLITE_SUFFIXES:
        if not _sqlite_has_table(path, "orderbook_events"):
            return []
        with SQLiteOrderbookSink(path) as sink:
            return sink.load_events(market_ticker=market_ticker, limit=limit)

    raise ValueError(
        "Unsupported orderbook dataset source. Expected a SQLite file, a Parquet directory, "
        "a sink object, or an iterable of events."
    )


def load_market_data_events(
    source: str | Path | Iterable[dict[str, Any]] | Any,
    *,
    market_ticker: str | None = None,
    channel: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    if isinstance(source, (list, tuple)):
        events = _apply_filters(source, market_ticker=market_ticker, limit=None)
        if channel is not None:
            events = [event for event in events if event.get("channel") == channel]
        if limit is not None:
            events = events[:limit]
        return events

    if hasattr(source, "load_events"):
        try:
            return source.load_events(market_ticker=market_ticker, channel=channel, limit=limit)
        except TypeError:
            events = _apply_filters(source.load_events(), market_ticker=market_ticker, limit=None)
            if channel is not None:
                events = [event for event in events if event.get("channel") == channel]
            if limit is not None:
                events = events[:limit]
            return events

    path = Path(source)
    if path.is_dir():
        with ParquetMarketDataSink(path) as sink:
            return sink.load_events(market_ticker=market_ticker, channel=channel, limit=limit)

    if path.suffix.lower() in SQLITE_SUFFIXES:
        if not _sqlite_has_table(path, "market_data_events"):
            return []
        with SQLiteMarketDataSink(path) as sink:
            return sink.load_events(market_ticker=market_ticker, channel=channel, limit=limit)

    raise ValueError(
        "Unsupported market-data dataset source. Expected a SQLite file, a Parquet directory, "
        "a sink object, or an iterable of events."
    )


def replay_orderbook_events(
    events: Iterable[dict[str, Any]],
    *,
    include_levels: bool = True,
) -> list[dict[str, Any]]:
    states: dict[str, OrderbookState] = {}
    replayed: list[dict[str, Any]] = []

    for event in _sort_events(events):
        market_ticker = str(event.get("market_ticker") or "")
        state = states.setdefault(market_ticker, OrderbookState())
        raw_message = event.get("raw_message") or {}
        event_type = event.get("event_type")

        if event_type == "orderbook_snapshot":
            state.apply_snapshot(raw_message or event)
        elif event_type == "orderbook_delta":
            state.apply_delta(raw_message or event)
        else:
            continue

        replayed.append(
            {
                **event,
                "best_yes_bid_cents": state.best_yes_bid_cents,
                "best_yes_ask_cents": state.best_yes_ask_cents,
                "best_no_bid_cents": state.best_no_bid_cents,
                "best_no_ask_cents": state.best_no_ask_cents,
                "yes_levels": state.serialized_yes_levels() if include_levels else event.get("yes_levels"),
                "no_levels": state.serialized_no_levels() if include_levels else event.get("no_levels"),
            }
        )

    return replayed


def orderbook_events_to_dataframe(events: Iterable[dict[str, Any]]):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for orderbook_events_to_dataframe") from exc

    rows = []
    for event in events:
        rows.append(
            {
                "captured_at": event.get("captured_at"),
                "market_ticker": event.get("market_ticker"),
                "event_type": event.get("event_type"),
                "sequence": event.get("sequence"),
                "price_cents": event.get("price_cents"),
                "delta_fp": event.get("delta_fp"),
                "best_yes_bid_cents": event.get("best_yes_bid_cents"),
                "best_yes_ask_cents": event.get("best_yes_ask_cents"),
                "best_no_bid_cents": event.get("best_no_bid_cents"),
                "best_no_ask_cents": event.get("best_no_ask_cents"),
            }
        )

    dataframe = pd.DataFrame(rows)
    if not dataframe.empty and "captured_at" in dataframe.columns:
        dataframe["captured_at"] = pd.to_datetime(dataframe["captured_at"], utc=True)
        dataframe = dataframe.sort_values(["captured_at", "sequence"], na_position="last")
    return dataframe


def market_data_events_to_dataframe(events: Iterable[dict[str, Any]]):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for market_data_events_to_dataframe") from exc

    rows = []
    for event in events:
        rows.append(
            {
                "captured_at": event.get("captured_at"),
                "channel": event.get("channel"),
                "event_type": event.get("event_type"),
                "market_ticker": event.get("market_ticker"),
                "sequence": event.get("sequence"),
                "price_cents": event.get("price_cents"),
                "yes_price_cents": event.get("yes_price_cents"),
                "no_price_cents": event.get("no_price_cents"),
                "yes_bid_cents": event.get("yes_bid_cents"),
                "yes_ask_cents": event.get("yes_ask_cents"),
                "count_fp": event.get("count_fp"),
                "volume_fp": event.get("volume_fp"),
                "open_interest_fp": event.get("open_interest_fp"),
                "trade_id": event.get("trade_id"),
                "taker_side": event.get("taker_side"),
            }
        )

    dataframe = pd.DataFrame(rows)
    if not dataframe.empty and "captured_at" in dataframe.columns:
        dataframe["captured_at"] = pd.to_datetime(dataframe["captured_at"], utc=True)
        dataframe = dataframe.sort_values(["captured_at", "sequence"], na_position="last")
    return dataframe
