from __future__ import annotations

import unittest
from unittest.mock import patch

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from mykalshi.research import ResearchSession
from mykalshi.research.event_analysis import load_event_market_payload
from mykalshi.research.trade_analysis import (
    TradeHistory,
    load_trade_history,
    plot_trade_activity,
    resample_trade_history,
    summarize_trade_history,
)


class ResearchTradeAnalysisTests(unittest.TestCase):
    def test_load_event_market_payload_falls_back_to_series_events(self):
        event_response = {
            "event": {
                "event_ticker": "KXOSCARPIC-26",
                "series_ticker": "KXOSCARPIC",
                "title": "Oscar for Best Picture?",
            },
            "markets": [],
        }
        series_response = {
            "events": [
                {
                    "event_ticker": "KXOSCARPIC-26",
                    "title": "Oscar for Best Picture?",
                    "series_ticker": "KXOSCARPIC",
                    "markets": [
                        {"ticker": "KXOSCARPIC-26-ONE", "yes_sub_title": "One Battle After Another", "status": "finalized"}
                    ],
                }
            ]
        }

        with patch("mykalshi.research.event_analysis.events.get_event", return_value=event_response), patch(
            "mykalshi.research.event_analysis.events.get_events",
            return_value=series_response,
        ):
            payload = load_event_market_payload("KXOSCARPIC-26")

        self.assertEqual(payload["event"]["event_ticker"], "KXOSCARPIC-26")
        self.assertEqual(len(payload["markets"]), 1)
        self.assertEqual(payload["markets"][0]["ticker"], "KXOSCARPIC-26-ONE")

    def test_trade_history_helpers_normalize_and_plot(self):
        trades = [
            {
                "ticker": "TEST",
                "trade_id": "1",
                "created_time": "2026-03-15T12:00:00Z",
                "count_fp": "10.00",
                "yes_price_dollars": "0.4000",
                "taker_side": "yes",
            },
            {
                "ticker": "TEST",
                "trade_id": "2",
                "created_time": "2026-03-15T12:30:00Z",
                "count_fp": "20.00",
                "yes_price_dollars": "0.6000",
                "taker_side": "no",
            },
        ]

        with patch("mykalshi.research.trade_analysis.routing.get_trades_auto", return_value={"ticker": "TEST", "trades": trades}):
            history = load_trade_history("TEST")

        self.assertIsInstance(history, TradeHistory)
        self.assertEqual(history.ticker, "TEST")
        self.assertEqual(len(history.trades), 2)
        self.assertEqual(float(history.trades.loc[0, "contracts"]), 10.0)
        self.assertEqual(float(history.trades.loc[1, "yes_price_cents"]), 60.0)

        summary = summarize_trade_history(history)
        self.assertEqual(summary["trade_count"], 2)
        self.assertEqual(summary["total_contracts"], 30.0)
        self.assertAlmostEqual(summary["vwap_yes_price_cents"], (10 * 40 + 20 * 60) / 30)
        self.assertAlmostEqual(summary["yes_taker_contract_share"], 10 / 30)

        resampled = resample_trade_history(history, freq="1D")
        self.assertEqual(int(resampled.iloc[0]["trade_count"]), 2)
        self.assertEqual(float(resampled.iloc[0]["contracts"]), 30.0)

        fig, axes = plot_trade_activity(history, freq="1D", title="Trade activity")
        self.assertIsNotNone(fig)
        self.assertEqual(len(axes), 2)

    def test_research_session_wraps_event_payload_and_trade_history(self):
        session = ResearchSession()
        with patch("mykalshi.research.workflows.load_event_market_payload", return_value={"event": {}, "markets": []}), patch(
            "mykalshi.research.workflows.load_trade_history",
            return_value="history",
        ):
            self.assertEqual(session.load_event_market_payload("TEST-EVENT"), {"event": {}, "markets": []})
            self.assertEqual(session.load_trade_history("TEST"), "history")


if __name__ == "__main__":
    unittest.main()
