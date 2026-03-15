from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mykalshi.research.storage import ParquetOrderbookSink, SQLiteOrderbookSink


def sample_event() -> dict:
    return {
        "captured_at": "2026-03-15T12:00:00.000+00:00",
        "event_type": "orderbook_snapshot",
        "channel": "orderbook_delta",
        "subscription_id": 1,
        "sequence": 2,
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
        "raw_message": {"type": "orderbook_snapshot"},
    }


class SQLiteOrderbookSinkTests(unittest.TestCase):
    def test_sqlite_sink_round_trips_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = SQLiteOrderbookSink(Path(tmpdir) / "orderbook.sqlite")
            sink.write_orderbook_event(sample_event())
            sink.flush()

            loaded = sink.load_events()
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["market_ticker"], "FED-23DEC-T3.00")
            self.assertEqual(loaded[0]["yes_levels"][0]["price_cents"], 22)
            sink.close()


class ParquetOrderbookSinkTests(unittest.TestCase):
    def test_parquet_sink_round_trips_events(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")

        with tempfile.TemporaryDirectory() as tmpdir:
            sink = ParquetOrderbookSink(tmpdir, batch_size=1)
            sink.write_orderbook_event(sample_event())
            sink.close()

            files = list(Path(tmpdir).glob("*.parquet"))
            self.assertEqual(len(files), 1)
            loaded = sink.load_events()
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["best_yes_bid_cents"], 22)


if __name__ == "__main__":
    unittest.main()
