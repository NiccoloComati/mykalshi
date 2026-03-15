from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mykalshi.research.datasets import load_orderbook_events, replay_orderbook_events
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


if __name__ == "__main__":
    unittest.main()
