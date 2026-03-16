from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from mykalshi.research.storage import (
    ParquetOrderbookSink,
    SplitMarketCaptureSink,
    SQLiteMarketDataSink,
    SQLiteOrderbookSink,
)


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


def sample_ticker_event() -> dict:
    return {
        "captured_at": "2026-03-15T12:01:00.000+00:00",
        "event_type": "ticker",
        "channel": "ticker",
        "subscription_id": 1,
        "sequence": 3,
        "market_ticker": "FED-23DEC-T3.00",
        "market_id": "market-1",
        "event_ts": None,
        "side": None,
        "price_cents": None,
        "yes_price_cents": None,
        "no_price_cents": None,
        "yes_bid_cents": 44,
        "yes_ask_cents": 46,
        "yes_bid_size_fp": "12.00",
        "yes_ask_size_fp": "8.00",
        "last_trade_size_fp": None,
        "delta_fp": None,
        "count_fp": None,
        "volume_fp": "100.00",
        "open_interest_fp": "50.00",
        "dollar_volume": 4200,
        "dollar_open_interest": 2100,
        "trade_id": None,
        "taker_side": None,
        "best_yes_bid_cents": None,
        "best_yes_ask_cents": None,
        "best_no_bid_cents": None,
        "best_no_ask_cents": None,
        "yes_levels": None,
        "no_levels": None,
        "raw_message": {"type": "ticker"},
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

    def test_sqlite_sink_supports_cross_thread_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = SQLiteOrderbookSink(Path(tmpdir) / "orderbook.sqlite")

            def writer():
                sink.write_orderbook_event(sample_event())
                sink.flush()

            thread = threading.Thread(target=writer)
            thread.start()
            thread.join()

            loaded = sink.load_events()
            self.assertEqual(len(loaded), 1)
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


class SplitMarketCaptureSinkTests(unittest.TestCase):
    def test_split_sink_routes_orderbook_and_market_data_to_separate_sinks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            market_data_sink = SQLiteMarketDataSink(Path(tmpdir) / "market-data.sqlite")
            orderbook_sink = SQLiteOrderbookSink(Path(tmpdir) / "orderbook.sqlite")
            sink = SplitMarketCaptureSink(
                market_data_sink=market_data_sink,
                orderbook_sink=orderbook_sink,
            )

            sink.write_market_data_event(sample_ticker_event())
            sink.write_market_data_event(sample_event())
            sink.flush()

            market_data_events = market_data_sink.load_events()
            orderbook_events = orderbook_sink.load_events()
            self.assertEqual(len(market_data_events), 1)
            self.assertEqual(market_data_events[0]["channel"], "ticker")
            self.assertEqual(len(orderbook_events), 1)
            self.assertEqual(orderbook_events[0]["channel"], "orderbook_delta")
            sink.close()


if __name__ == "__main__":
    unittest.main()
