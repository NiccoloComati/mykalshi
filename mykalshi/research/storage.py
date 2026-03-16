from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from ..exceptions import KalshiDependencyError


JSON_COLUMNS = ("yes_levels", "no_levels", "raw_message")
ORDERBOOK_COLUMNS = (
    "captured_at",
    "event_type",
    "channel",
    "subscription_id",
    "sequence",
    "market_ticker",
    "market_id",
    "event_ts",
    "side",
    "price_cents",
    "delta_fp",
    "best_yes_bid_cents",
    "best_yes_ask_cents",
    "best_no_bid_cents",
    "best_no_ask_cents",
    "yes_levels",
    "no_levels",
    "raw_message",
)
MARKET_DATA_COLUMNS = (
    "captured_at",
    "event_type",
    "channel",
    "subscription_id",
    "sequence",
    "market_ticker",
    "market_id",
    "event_ts",
    "side",
    "price_cents",
    "yes_price_cents",
    "no_price_cents",
    "yes_bid_cents",
    "yes_ask_cents",
    "yes_bid_size_fp",
    "yes_ask_size_fp",
    "last_trade_size_fp",
    "delta_fp",
    "count_fp",
    "volume_fp",
    "open_interest_fp",
    "dollar_volume",
    "dollar_open_interest",
    "trade_id",
    "taker_side",
    "best_yes_bid_cents",
    "best_yes_ask_cents",
    "best_no_bid_cents",
    "best_no_ask_cents",
    "yes_levels",
    "no_levels",
    "raw_message",
)


def _row_for_columns(serialized: dict[str, Any], columns: tuple[str, ...]) -> dict[str, Any]:
    return {column: serialized.get(column) for column in columns}


def _serialize_event(event: dict[str, Any]) -> dict[str, Any]:
    serialized = dict(event)
    for column in JSON_COLUMNS:
        serialized[column] = json.dumps(event.get(column), separators=(",", ":"), sort_keys=True)
    return serialized


def _deserialize_event(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for column in JSON_COLUMNS:
        value = payload.get(column)
        payload[column] = json.loads(value) if value else None
    return payload


class SQLiteOrderbookSink:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS orderbook_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                channel TEXT NOT NULL,
                subscription_id INTEGER,
                sequence INTEGER,
                market_ticker TEXT NOT NULL,
                market_id TEXT,
                event_ts TEXT,
                side TEXT,
                price_cents INTEGER,
                delta_fp TEXT,
                best_yes_bid_cents INTEGER,
                best_yes_ask_cents INTEGER,
                best_no_bid_cents INTEGER,
                best_no_ask_cents INTEGER,
                yes_levels TEXT,
                no_levels TEXT,
                raw_message TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orderbook_events_market_seq
            ON orderbook_events (market_ticker, sequence)
            """
        )
        self._connection.commit()

    def write_orderbook_event(self, event: dict[str, Any]) -> None:
        row = _serialize_event(event)
        placeholders = ", ".join("?" for _ in ORDERBOOK_COLUMNS)
        columns = ", ".join(ORDERBOOK_COLUMNS)
        self._connection.execute(
            f"INSERT INTO orderbook_events ({columns}) VALUES ({placeholders})",
            tuple(row[column] for column in ORDERBOOK_COLUMNS),
        )

    def flush(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.commit()
        self._connection.close()

    def load_events(
        self,
        *,
        market_ticker: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM orderbook_events"
        params: list[Any] = []
        if market_ticker is not None:
            query += " WHERE market_ticker = ?"
            params.append(market_ticker)
        query += " ORDER BY id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        cursor = self._connection.execute(query, params)
        return [_deserialize_event(row) for row in cursor.fetchall()]

    def __enter__(self) -> "SQLiteOrderbookSink":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class ParquetOrderbookSink:
    def __init__(
        self,
        directory: str | Path,
        *,
        batch_size: int = 500,
        file_prefix: str = "orderbook-events",
    ) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.file_prefix = file_prefix
        self._buffer: list[dict[str, Any]] = []
        self._part_index = len(list(self.directory.glob("*.parquet")))

    def _get_parquet_modules(self) -> tuple[Any, Any]:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise KalshiDependencyError(
                "pyarrow is required to write Parquet research datasets"
            ) from exc
        return pa, pq

    @staticmethod
    def _schema(pa: Any) -> Any:
        return pa.schema(
            [
                ("captured_at", pa.string()),
                ("event_type", pa.string()),
                ("channel", pa.string()),
                ("subscription_id", pa.int64()),
                ("sequence", pa.int64()),
                ("market_ticker", pa.string()),
                ("market_id", pa.string()),
                ("event_ts", pa.string()),
                ("side", pa.string()),
                ("price_cents", pa.int64()),
                ("delta_fp", pa.string()),
                ("best_yes_bid_cents", pa.int64()),
                ("best_yes_ask_cents", pa.int64()),
                ("best_no_bid_cents", pa.int64()),
                ("best_no_ask_cents", pa.int64()),
                ("yes_levels", pa.string()),
                ("no_levels", pa.string()),
                ("raw_message", pa.string()),
            ]
        )

    def write_orderbook_event(self, event: dict[str, Any]) -> None:
        self._buffer.append(_serialize_event(event))
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        pa, pq = self._get_parquet_modules()
        output_path = self.directory / f"{self.file_prefix}-{self._part_index:05d}.parquet"
        table = pa.Table.from_pylist(
            [_row_for_columns(row, ORDERBOOK_COLUMNS) for row in self._buffer],
            schema=self._schema(pa),
        )
        pq.write_table(table, output_path)
        self._part_index += 1
        self._buffer.clear()

    def close(self) -> None:
        self.flush()

    def load_events(
        self,
        *,
        market_ticker: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        try:
            import pyarrow.dataset as ds
        except ImportError as exc:
            raise KalshiDependencyError(
                "pyarrow is required to read Parquet research datasets"
            ) from exc

        dataset = ds.dataset(self.directory, format="parquet")
        rows = dataset.to_table().to_pylist()
        events = [_deserialize_event(row) for row in rows]
        if market_ticker is not None:
            events = [event for event in events if event.get("market_ticker") == market_ticker]
        if limit is not None:
            events = events[:limit]
        return events

    def __enter__(self) -> "ParquetOrderbookSink":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class MultiOrderbookSink:
    def __init__(self, *sinks: Any) -> None:
        self.sinks = list(sinks)

    def write_orderbook_event(self, event: dict[str, Any]) -> None:
        for sink in self.sinks:
            sink.write_orderbook_event(event)

    def flush(self) -> None:
        for sink in self.sinks:
            if hasattr(sink, "flush"):
                sink.flush()

    def close(self) -> None:
        for sink in self.sinks:
            if hasattr(sink, "close"):
                sink.close()


class SQLiteMarketDataSink:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                channel TEXT NOT NULL,
                subscription_id INTEGER,
                sequence INTEGER,
                market_ticker TEXT,
                market_id TEXT,
                event_ts TEXT,
                side TEXT,
                price_cents INTEGER,
                yes_price_cents INTEGER,
                no_price_cents INTEGER,
                yes_bid_cents INTEGER,
                yes_ask_cents INTEGER,
                yes_bid_size_fp TEXT,
                yes_ask_size_fp TEXT,
                last_trade_size_fp TEXT,
                delta_fp TEXT,
                count_fp TEXT,
                volume_fp TEXT,
                open_interest_fp TEXT,
                dollar_volume INTEGER,
                dollar_open_interest INTEGER,
                trade_id TEXT,
                taker_side TEXT,
                best_yes_bid_cents INTEGER,
                best_yes_ask_cents INTEGER,
                best_no_bid_cents INTEGER,
                best_no_ask_cents INTEGER,
                yes_levels TEXT,
                no_levels TEXT,
                raw_message TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_data_events_lookup
            ON market_data_events (channel, market_ticker, sequence)
            """
        )
        self._connection.commit()

    def write_market_data_event(self, event: dict[str, Any]) -> None:
        row = _serialize_event(event)
        placeholders = ", ".join("?" for _ in MARKET_DATA_COLUMNS)
        columns = ", ".join(MARKET_DATA_COLUMNS)
        self._connection.execute(
            f"INSERT INTO market_data_events ({columns}) VALUES ({placeholders})",
            tuple(row.get(column) for column in MARKET_DATA_COLUMNS),
        )

    def flush(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.commit()
        self._connection.close()

    def load_events(
        self,
        *,
        market_ticker: str | None = None,
        channel: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM market_data_events"
        params: list[Any] = []
        clauses = []
        if market_ticker is not None:
            clauses.append("market_ticker = ?")
            params.append(market_ticker)
        if channel is not None:
            clauses.append("channel = ?")
            params.append(channel)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        cursor = self._connection.execute(query, params)
        return [_deserialize_event(row) for row in cursor.fetchall()]

    def __enter__(self) -> "SQLiteMarketDataSink":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class ParquetMarketDataSink:
    def __init__(
        self,
        directory: str | Path,
        *,
        batch_size: int = 500,
        file_prefix: str = "market-data-events",
    ) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.file_prefix = file_prefix
        self._buffer: list[dict[str, Any]] = []
        self._part_index = len(list(self.directory.glob("*.parquet")))

    def _get_parquet_modules(self) -> tuple[Any, Any]:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise KalshiDependencyError(
                "pyarrow is required to write Parquet research datasets"
            ) from exc
        return pa, pq

    @staticmethod
    def _schema(pa: Any) -> Any:
        return pa.schema(
            [
                ("captured_at", pa.string()),
                ("event_type", pa.string()),
                ("channel", pa.string()),
                ("subscription_id", pa.int64()),
                ("sequence", pa.int64()),
                ("market_ticker", pa.string()),
                ("market_id", pa.string()),
                ("event_ts", pa.string()),
                ("side", pa.string()),
                ("price_cents", pa.int64()),
                ("yes_price_cents", pa.int64()),
                ("no_price_cents", pa.int64()),
                ("yes_bid_cents", pa.int64()),
                ("yes_ask_cents", pa.int64()),
                ("yes_bid_size_fp", pa.string()),
                ("yes_ask_size_fp", pa.string()),
                ("last_trade_size_fp", pa.string()),
                ("delta_fp", pa.string()),
                ("count_fp", pa.string()),
                ("volume_fp", pa.string()),
                ("open_interest_fp", pa.string()),
                ("dollar_volume", pa.int64()),
                ("dollar_open_interest", pa.int64()),
                ("trade_id", pa.string()),
                ("taker_side", pa.string()),
                ("best_yes_bid_cents", pa.int64()),
                ("best_yes_ask_cents", pa.int64()),
                ("best_no_bid_cents", pa.int64()),
                ("best_no_ask_cents", pa.int64()),
                ("yes_levels", pa.string()),
                ("no_levels", pa.string()),
                ("raw_message", pa.string()),
            ]
        )

    def write_market_data_event(self, event: dict[str, Any]) -> None:
        self._buffer.append(_serialize_event(event))
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        pa, pq = self._get_parquet_modules()
        output_path = self.directory / f"{self.file_prefix}-{self._part_index:05d}.parquet"
        table = pa.Table.from_pylist(
            [_row_for_columns(row, MARKET_DATA_COLUMNS) for row in self._buffer],
            schema=self._schema(pa),
        )
        pq.write_table(table, output_path)
        self._part_index += 1
        self._buffer.clear()

    def close(self) -> None:
        self.flush()

    def load_events(
        self,
        *,
        market_ticker: str | None = None,
        channel: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        try:
            import pyarrow.dataset as ds
        except ImportError as exc:
            raise KalshiDependencyError(
                "pyarrow is required to read Parquet research datasets"
            ) from exc

        dataset = ds.dataset(self.directory, format="parquet")
        rows = dataset.to_table().to_pylist()
        events = [_deserialize_event(row) for row in rows]
        if market_ticker is not None:
            events = [event for event in events if event.get("market_ticker") == market_ticker]
        if channel is not None:
            events = [event for event in events if event.get("channel") == channel]
        if limit is not None:
            events = events[:limit]
        return events

    def __enter__(self) -> "ParquetMarketDataSink":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class MultiMarketDataSink:
    def __init__(self, *sinks: Any) -> None:
        self.sinks = list(sinks)

    def write_market_data_event(self, event: dict[str, Any]) -> None:
        for sink in self.sinks:
            sink.write_market_data_event(event)

    def flush(self) -> None:
        for sink in self.sinks:
            if hasattr(sink, "flush"):
                sink.flush()

    def close(self) -> None:
        for sink in self.sinks:
            if hasattr(sink, "close"):
                sink.close()


class SplitMarketCaptureSink:
    """Route mixed websocket capture events into market-data and orderbook sinks."""

    def __init__(
        self,
        *,
        market_data_sink: Any | None = None,
        orderbook_sink: Any | None = None,
    ) -> None:
        if market_data_sink is None and orderbook_sink is None:
            raise ValueError("At least one underlying sink must be provided")
        self.market_data_sink = market_data_sink
        self.orderbook_sink = orderbook_sink

    def write_market_data_event(self, event: dict[str, Any]) -> None:
        if event.get("channel") == "orderbook_delta":
            if self.orderbook_sink is not None:
                self.orderbook_sink.write_orderbook_event(event)
                return
            if self.market_data_sink is not None:
                self.market_data_sink.write_market_data_event(event)
                return
        if self.market_data_sink is not None:
            self.market_data_sink.write_market_data_event(event)
            return
        if self.orderbook_sink is not None:
            self.orderbook_sink.write_orderbook_event(event)

    def write_orderbook_event(self, event: dict[str, Any]) -> None:
        if self.orderbook_sink is not None:
            self.orderbook_sink.write_orderbook_event(event)
            return
        if self.market_data_sink is not None:
            self.market_data_sink.write_market_data_event(event)

    def flush(self) -> None:
        for sink in (self.market_data_sink, self.orderbook_sink):
            if sink is not None and hasattr(sink, "flush"):
                sink.flush()

    def close(self) -> None:
        for sink in (self.market_data_sink, self.orderbook_sink):
            if sink is not None and hasattr(sink, "close"):
                sink.close()
