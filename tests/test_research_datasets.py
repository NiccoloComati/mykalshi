from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mykalshi.research.datasets import (
    load_orderbook_events,
    load_replay_event_stream,
    merge_replay_event_streams,
    replay_orderbook_events,
)
from mykalshi.research.storage import ParquetOrderbookSink, SQLiteOrderbookSink


def snapshot_event() -> dict:
    return {
        "captured_at": "2026-03-15T12:00:00.000+00:00",
        "event_type": "orderbook_snapshot",
        "channel": "orderbook_delta",
        "subscription_id": 1,
        "sequence": 1,
        "market_ticker": "FED-23DEC-T3.00",
        "market_id": "market-1",
        "event_ts": None,
        "side": None,
        "price_cents": None,
        "delta_fp": None,
        "best_yes_bid_cents": 22,
        "best_yes_ask_cents": 46,
        "best_no_bid_cents": 54,
        "best_no_ask_cents": 78,
        "yes_levels": [{"price_cents": 22, "count_fp": "300.00"}],
        "no_levels": [{"price_cents": 54, "count_fp": "20.00"}],
        "raw_message": {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "market_id": "market-1",
                "yes_dollars_fp": [["0.2200", "300.00"]],
                "no_dollars_fp": [["0.5400", "20.00"]],
            },
        },
    }


def delta_event() -> dict:
    return {
        "captured_at": "2026-03-15T12:00:01.000+00:00",
        "event_type": "orderbook_delta",
        "channel": "orderbook_delta",
        "subscription_id": 1,
        "sequence": 2,
        "market_ticker": "FED-23DEC-T3.00",
        "market_id": "market-1",
        "event_ts": "2026-03-15T12:00:01Z",
        "side": "yes",
        "price_cents": 22,
        "delta_fp": "-50.00",
        "best_yes_bid_cents": 22,
        "best_yes_ask_cents": 46,
        "best_no_bid_cents": 54,
        "best_no_ask_cents": 78,
        "yes_levels": None,
        "no_levels": None,
        "raw_message": {
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 2,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "market_id": "market-1",
                "price_dollars": "0.2200",
                "delta_fp": "-50.00",
                "side": "yes",
                "ts": "2026-03-15T12:00:01Z",
            },
        },
    }


def trade_market_data_event() -> dict:
    return {
        "captured_at": "2026-03-15T12:00:00.500+00:00",
        "event_type": "trade",
        "channel": "trade",
        "market_ticker": "FED-23DEC-T3.00",
        "yes_price_cents": 23,
        "no_price_cents": 77,
        "count_fp": "1.00",
        "sequence": 1,
    }


def ticker_market_data_event() -> dict:
    return {
        "captured_at": "2026-03-15T12:00:00.250+00:00",
        "event_type": "ticker",
        "channel": "ticker",
        "market_ticker": "FED-23DEC-T3.00",
        "yes_bid_cents": 22,
        "yes_ask_cents": 46,
        "yes_bid_size_fp": "180.00",
        "yes_ask_size_fp": "67.00",
        "sequence": 1,
    }


class DatasetTests(unittest.TestCase):
    def test_load_orderbook_events_from_sqlite_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "orderbook.sqlite"
            with SQLiteOrderbookSink(path) as sink:
                sink.write_orderbook_event(snapshot_event())
                sink.flush()

            loaded = load_orderbook_events(path)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["market_ticker"], "FED-23DEC-T3.00")

    def test_load_orderbook_events_from_parquet_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with ParquetOrderbookSink(tmpdir, batch_size=1) as sink:
                sink.write_orderbook_event(snapshot_event())

            loaded = load_orderbook_events(tmpdir)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["market_ticker"], "FED-23DEC-T3.00")

    def test_replay_orderbook_events_reconstructs_levels(self):
        replayed = replay_orderbook_events([snapshot_event(), delta_event()])

        self.assertEqual(len(replayed), 2)
        self.assertEqual(replayed[-1]["best_yes_bid_cents"], 22)
        self.assertEqual(replayed[-1]["yes_levels"][0]["count_fp"], "250.00")

    def test_merge_replay_event_streams_sorts_market_data_and_orderbook(self):
        merged = merge_replay_event_streams(
            market_data_events=[trade_market_data_event(), ticker_market_data_event()],
            orderbook_events=[snapshot_event()],
        )

        self.assertEqual(len(merged), 3)
        self.assertEqual([event.get("event_type") for event in merged], ["orderbook_snapshot", "ticker", "trade"])
        self.assertIsNotNone(merged[0].get("yes_levels"))

    def test_load_replay_event_stream_combines_sources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "orderbook.sqlite"
            with SQLiteOrderbookSink(sqlite_path) as sink:
                sink.write_orderbook_event(snapshot_event())
                sink.write_orderbook_event(delta_event())
                sink.flush()

            merged = load_replay_event_stream(
                market_data_source=[trade_market_data_event(), ticker_market_data_event()],
                orderbook_source=sqlite_path,
                market_ticker="FED-23DEC-T3.00",
            )

        self.assertEqual([event.get("event_type") for event in merged], ["orderbook_snapshot", "ticker", "trade", "orderbook_delta"])
        self.assertEqual(merged[-1]["yes_levels"][0]["count_fp"], "250.00")



if __name__ == "__main__":
    unittest.main()
