from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mykalshi.research import KalshiStrategy, ReplayBacktester, SQLiteOrderbookSink, enrich_replay_events_with_market_lifecycle


def orderbook_snapshot_event(*, market_ticker: str = "FED-23DEC-T3.00") -> dict:
    return {
        "captured_at": "2026-03-15T12:00:00.000+00:00",
        "event_type": "orderbook_snapshot",
        "channel": "orderbook_delta",
        "subscription_id": 1,
        "sequence": 1,
        "market_ticker": market_ticker,
        "market_id": "market-1",
        "event_ts": None,
        "side": None,
        "price_cents": None,
        "delta_fp": None,
        "best_yes_bid_cents": 40,
        "best_yes_ask_cents": 45,
        "best_no_bid_cents": 55,
        "best_no_ask_cents": 60,
        "yes_levels": [{"price_cents": 40, "count_fp": "10.00"}],
        "no_levels": [{"price_cents": 55, "count_fp": "10.00"}],
        "raw_message": {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": market_ticker,
                "market_id": "market-1",
                "yes_dollars_fp": [["0.4000", "10.00"]],
                "no_dollars_fp": [["0.5500", "10.00"]],
            },
        },
    }


def ticker_event(*, market_ticker: str = "FED-23DEC-T3.00", captured_at: str = "2026-03-15T12:01:00.000+00:00") -> dict:
    return {
        "captured_at": captured_at,
        "event_type": "ticker",
        "channel": "ticker",
        "market_ticker": market_ticker,
        "yes_bid_cents": 60,
        "yes_ask_cents": 62,
        "yes_bid_size_fp": "25.00",
        "yes_ask_size_fp": "10.00",
        "sequence": 2,
    }


class BuyOnFirstOrderbookStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.submitted = False

    def on_orderbook(self, context, event):
        if self.submitted:
            return
        context.buy_yes(event.market_ticker, quantity=1)
        self.submitted = True


class NoOpStrategy(KalshiStrategy):
    pass


class PassiveBidStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.submitted = False

    def on_orderbook(self, context, event):
        if self.submitted:
            return
        context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=40)
        self.submitted = True


class ReplayBacktesterTests(unittest.TestCase):
    def test_run_on_replay_event_stream_uses_merged_market_data_timeline(self):
        events = [
            ticker_event(),
            orderbook_snapshot_event(),
        ]

        result = ReplayBacktester().run_on_replay_event_stream(
            events,
            BuyOnFirstOrderbookStrategy(),
            initial_cash_cents=100,
            enrich_market_lifecycle=False,
        )

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].price_cents, 45)
        self.assertEqual(str(result.final_cash_cents), "55.00")
        self.assertEqual(str(result.final_equity_cents), "116.00")

    def test_run_on_replay_event_stream_supports_single_market_initial_positions(self):
        result = ReplayBacktester().run_on_replay_event_stream(
            [ticker_event()],
            NoOpStrategy(),
            initial_cash_cents=0,
            market_ticker="FED-23DEC-T3.00",
            enrich_market_lifecycle=False,
            initial_yes_position=2,
        )

        self.assertEqual(str(result.final_cash_cents), "0.00")
        self.assertEqual(str(result.final_equity_cents), "122.00")

    def test_run_on_captured_dataset_loads_and_filters_sources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "orderbook.sqlite"
            with SQLiteOrderbookSink(sqlite_path) as sink:
                sink.write_orderbook_event(orderbook_snapshot_event())
                sink.flush()

            result = ReplayBacktester().run_on_captured_dataset(
                BuyOnFirstOrderbookStrategy(),
                market_data_source=[
                    ticker_event(),
                    ticker_event(market_ticker="OTHER-26"),
                ],
                orderbook_source=sqlite_path,
                market_ticker="FED-23DEC-T3.00",
                enrich_market_lifecycle=False,
                initial_cash_cents=100,
            )

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].market_ticker, "FED-23DEC-T3.00")
        self.assertEqual(str(result.final_equity_cents), "116.00")

    def test_run_on_captured_dataset_requires_at_least_one_source(self):
        with self.assertRaisesRegex(ValueError, "At least one"):
            ReplayBacktester().run_on_captured_dataset(NoOpStrategy())

    def test_enrichment_adds_pending_and_actual_settlement_events_from_metadata(self):
        enriched = enrich_replay_events_with_market_lifecycle(
            [orderbook_snapshot_event(), ticker_event()],
            market_metadata={
                "FED-23DEC-T3.00": {
                    "ticker": "FED-23DEC-T3.00",
                    "status": "finalized",
                    "expiration_time": "2026-03-15T12:02:00Z",
                    "settlement_ts": "2026-03-15T12:03:00Z",
                    "result": "yes",
                    "settlement_value_dollars": "1.0000",
                }
            },
            reference_time="2026-03-15T12:05:00Z",
        )

        settlement_events = [event for event in enriched if event.get("event_type") == "settlement"]
        self.assertEqual(len(settlement_events), 2)
        self.assertEqual(settlement_events[0]["reason"], "synthetic_expiration")
        self.assertEqual(settlement_events[1]["yes_payout_cents"], 100)
        self.assertEqual(settlement_events[1]["no_payout_cents"], 0)

    def test_run_on_replay_event_stream_enriches_settlement_and_realizes_payout(self):
        result = ReplayBacktester().run_on_replay_event_stream(
            [orderbook_snapshot_event(), ticker_event()],
            BuyOnFirstOrderbookStrategy(),
            initial_cash_cents=100,
            market_metadata={
                "FED-23DEC-T3.00": {
                    "ticker": "FED-23DEC-T3.00",
                    "status": "finalized",
                    "expiration_time": "2026-03-15T12:02:00Z",
                    "settlement_ts": "2026-03-15T12:03:00Z",
                    "result": "yes",
                    "settlement_value_dollars": "1.0000",
                }
            },
            reference_time="2026-03-15T12:05:00Z",
        )

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(str(result.final_cash_cents), "155.00")
        self.assertEqual(str(result.final_equity_cents), "155.00")

    def test_run_on_replay_event_stream_enriches_expiration_and_cancels_resting_orders(self):
        result = ReplayBacktester().run_on_replay_event_stream(
            [orderbook_snapshot_event()],
            PassiveBidStrategy(),
            initial_cash_cents=100,
            market_metadata={
                "FED-23DEC-T3.00": {
                    "ticker": "FED-23DEC-T3.00",
                    "status": "finalized",
                    "expiration_time": "2026-03-15T12:02:00Z",
                    "result": "",
                }
            },
            reference_time="2026-03-15T12:05:00Z",
        )

        self.assertEqual(len(result.fills), 0)
        self.assertEqual(result.final_orders[0].status, "canceled")
        self.assertTrue(any("Settlement data missing" in log["message"] for log in result.logs))
        self.assertEqual(str(result.final_cash_cents), "100.00")


if __name__ == "__main__":
    unittest.main()
