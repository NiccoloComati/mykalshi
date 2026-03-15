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
        self._connection = sqlite3.connect(self.path)
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

    def write_orderbook_event(self, event: dict[str, Any]) -> None:
        self._buffer.append(_serialize_event(event))
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        pa, pq = self._get_parquet_modules()
        output_path = self.directory / f"{self.file_prefix}-{self._part_index:05d}.parquet"
        table = pa.Table.from_pylist(self._buffer)
        pq.write_table(table, output_path)
        self._part_index += 1
        self._buffer.clear()

    def close(self) -> None:
        self.flush()

    def load_events(self) -> list[dict[str, Any]]:
        try:
            import pyarrow.dataset as ds
        except ImportError as exc:
            raise KalshiDependencyError(
                "pyarrow is required to read Parquet research datasets"
            ) from exc

        dataset = ds.dataset(self.directory, format="parquet")
        rows = dataset.to_table().to_pylist()
        return [_deserialize_event(row) for row in rows]

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
