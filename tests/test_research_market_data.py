from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mykalshi.research import (
    ParquetMarketDataSink,
    SQLiteMarketDataSink,
    build_ticker_event,
    build_trade_event,
    load_market_data_events,
    normalize_market_data_message,
)


def ticker_message() -> dict:
    return {
        "type": "ticker",
        "sid": 1,
        "msg": {
            "market_id": "market-1",
            "market_ticker": "KXELONMARS-99",
            "price_dollars": "0.1000",
            "yes_bid_dollars": "0.0900",
            "yes_ask_dollars": "0.1000",
            "volume_fp": "110642.00",
            "open_interest_fp": "39652.00",
            "dollar_volume": 55321,
            "dollar_open_interest": 19826,
            "yes_bid_size_fp": "180.00",
            "yes_ask_size_fp": "67.00",
            "last_trade_size_fp": "1.00",
            "ts": 1773600630,
            "time": "2026-03-15T18:50:30.40732Z",
        },
    }


def trade_message() -> dict:
    return {
        "type": "trade",
        "sid": 1,
        "seq": 1,
        "msg": {
            "trade_id": "trade-1",
            "market_ticker": "KXPGATOUR-THPC26-MFIT",
            "yes_price_dollars": "0.2200",
            "no_price_dollars": "0.7800",
            "count_fp": "107.00",
            "taker_side": "yes",
            "ts": 1773600879,
        },
    }


class MarketDataNormalizationTests(unittest.TestCase):
    def test_build_ticker_event_normalizes_prices(self):
        event = build_ticker_event(ticker_message(), captured_at="2026-03-15T18:50:30.000+00:00")

        self.assertEqual(event["channel"], "ticker")
        self.assertEqual(event["price_cents"], 10)
        self.assertEqual(event["yes_bid_cents"], 9)
        self.assertEqual(event["yes_ask_cents"], 10)

    def test_build_trade_event_normalizes_prices(self):
        event = build_trade_event(trade_message(), captured_at="2026-03-15T18:50:30.000+00:00")

        self.assertEqual(event["channel"], "trade")
        self.assertEqual(event["yes_price_cents"], 22)
        self.assertEqual(event["no_price_cents"], 78)
        self.assertEqual(event["trade_id"], "trade-1")

    def test_normalize_market_data_message_dispatches(self):
        event = normalize_market_data_message(trade_message(), orderbook_states={})
        self.assertEqual(event["channel"], "trade")


class MarketDataStorageTests(unittest.TestCase):
    def test_sqlite_market_data_sink_round_trips(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "market-data.sqlite"
            with SQLiteMarketDataSink(path) as sink:
                sink.write_market_data_event(build_ticker_event(ticker_message()))
                sink.flush()

            loaded = load_market_data_events(path, channel="ticker")
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["market_ticker"], "KXELONMARS-99")

    def test_parquet_market_data_sink_round_trips(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with ParquetMarketDataSink(tmpdir, batch_size=1) as sink:
                sink.write_market_data_event(build_ticker_event(ticker_message()))
                sink.write_market_data_event(build_trade_event(trade_message()))

            loaded = load_market_data_events(tmpdir)
            self.assertEqual(len(loaded), 2)
            trades = [event for event in loaded if event["channel"] == "trade"]
            tickers = [event for event in loaded if event["channel"] == "ticker"]
            self.assertEqual(trades[0]["trade_id"], "trade-1")
            self.assertEqual(tickers[0]["market_ticker"], "KXELONMARS-99")


if __name__ == "__main__":
    unittest.main()
