from __future__ import annotations

import tempfile
import unittest
from unittest.mock import patch

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from mykalshi.exceptions import KalshiHTTPError
from mykalshi.research import ResearchSession
from mykalshi.research.charts import plot_market_candles, plot_market_comparison
from mykalshi.research.event_analysis import MarketHistory, build_event_closeup, load_market_history, plot_event_closeup
from mykalshi.research.family_analysis import build_market_family_analysis
from mykalshi.research.orderbook_analysis import (
    get_orderbook_snapshot,
    orderbook_snapshots_to_matrices,
    plot_orderbook_depth,
    plot_orderbook_matrix_snapshot,
    render_orderbook_text,
)
from mykalshi.research.universe import UniverseSpec, open_market_universe, sync_market_universe


def sample_candlesticks(*, bid_close: int, ask_close: int, volume: str = "10.00") -> dict:
    return {
        "candlesticks": [
            {
                "end_period_ts": 1710000000,
                "volume_fp": volume,
                "open_interest_fp": "100.00",
                "price": {
                    "open_dollars": "0.5000",
                    "high_dollars": "0.5200",
                    "low_dollars": "0.4900",
                    "close_dollars": "0.5100",
                },
                "yes_bid": {"close_dollars": f"{bid_close / 100:.4f}"},
                "yes_ask": {"close_dollars": f"{ask_close / 100:.4f}"},
            },
            {
                "end_period_ts": 1710003600,
                "volume_fp": volume,
                "open_interest_fp": "101.00",
                "price": {
                    "open_dollars": "0.5100",
                    "high_dollars": "0.5300",
                    "low_dollars": "0.5000",
                    "close_dollars": "0.5200",
                },
                "yes_bid": {"close_dollars": f"{(bid_close + 1) / 100:.4f}"},
                "yes_ask": {"close_dollars": f"{(ask_close + 1) / 100:.4f}"},
            },
        ]
    }


def sample_history(ticker: str, label: str) -> MarketHistory:
    timestamps = pd.to_datetime(["2026-03-20T12:00:00Z", "2026-03-20T13:00:00Z"], utc=True)
    metrics = pd.DataFrame(
        {
            "bid": [40.0, 41.0],
            "ask": [42.0, 43.0],
            "mid": [41.0, 42.0],
            "volume": [10.0, 12.0],
            "open_interest": [100.0, 102.0],
        },
        index=timestamps,
    )
    candlestick_frame = pd.DataFrame(
        {
            "Open": [40.0, 41.0],
            "High": [42.0, 43.0],
            "Low": [39.0, 40.0],
            "Close": [41.0, 42.0],
            "Volume": [10.0, 12.0],
        },
        index=pd.to_datetime(["2026-03-20 12:00:00", "2026-03-20 13:00:00"]),
    )
    return MarketHistory(
        ticker=ticker,
        series_ticker="HIGHMIA",
        source="live",
        response={"ticker": ticker},
        metrics=metrics,
        candlestick_frame=candlestick_frame,
        label=label,
    )


class ResearchAnalysisTests(unittest.TestCase):
    def test_build_event_closeup_builds_panel_and_plot(self):
        event_payload = {
            "event": {
                "event_ticker": "PRES-2024",
                "series_ticker": "PRES",
                "title": "Presidential election",
            },
            "markets": [
                {"ticker": "PRES-2024-DJT", "yes_sub_title": "Trump", "volume_fp": "1000.00", "status": "settled"},
                {"ticker": "PRES-2024-KH", "yes_sub_title": "Harris", "volume_fp": "900.00", "status": "settled"},
            ],
        }

        def fake_get_full_market(*, ticker, **kwargs):
            if ticker == "PRES-2024-DJT":
                return {"ticker": ticker, **sample_candlesticks(bid_close=49, ask_close=51)}
            return {"ticker": ticker, **sample_candlesticks(bid_close=47, ask_close=49)}

        with patch("mykalshi.research.event_analysis.events.get_event", return_value=event_payload), patch(
            "mykalshi.research.event_analysis.market.get_full_market",
            side_effect=fake_get_full_market,
        ):
            closeup = build_event_closeup("PRES-2024", period_interval="h")

        self.assertEqual(closeup.series_ticker, "PRES")
        self.assertEqual(closeup.markets["market_ticker"].tolist(), ["PRES-2024-DJT", "PRES-2024-KH"])
        self.assertIn("Trump_mid", closeup.panel.columns)
        self.assertIn("Harris_mid", closeup.panel.columns)
        self.assertIn("aggregate_mid", closeup.panel.columns)

        fig, ax = plot_event_closeup(closeup)
        self.assertEqual(ax.get_ylabel(), "Mid (cents)")
        self.assertIsNotNone(fig)

    def test_load_market_history_falls_back_to_historical(self):
        with patch(
            "mykalshi.research.event_analysis.market.get_full_market",
            side_effect=KalshiHTTPError(404, "GET", "https://api.elections.kalshi.com/trade-api/v2/markets/ARCHIVE"),
        ), patch(
            "mykalshi.research.event_analysis.historical.get_historical_market",
            return_value={"market": {"open_time": "2024-01-01T00:00:00Z", "close_time": "2024-01-31T00:00:00Z"}},
        ), patch(
            "mykalshi.research.event_analysis.historical.get_historical_market_candlesticks",
            return_value=sample_candlesticks(bid_close=30, ask_close=32),
        ):
            history = load_market_history("ARCHIVE-YES", period_interval="d")

        self.assertEqual(history.source, "historical")
        self.assertFalse(history.metrics.empty)
        self.assertEqual(history.ticker, "ARCHIVE-YES")

    def test_orderbook_snapshot_render_and_matrix_helpers(self):
        response = {
            "market_ticker": "LOB-YES",
            "orderbook": {
                "yes": [[40, 5.0], [41, 3.0]],
                "no": [[55, 2.0], [57, 4.0]],
            },
        }
        snapshot = get_orderbook_snapshot(response=response)
        text = render_orderbook_text(snapshot)

        self.assertEqual(snapshot.best_bid_cents, 41)
        self.assertEqual(snapshot.best_ask_cents, 43)
        self.assertEqual(snapshot.spread_cents, 2)
        self.assertIn("YES @ 41c x 3.00 contracts", text)
        self.assertIn("YES @ 43c x 4.00 contracts", text)

        fig, ax = plot_orderbook_depth(snapshot)
        self.assertEqual(ax.get_title(), "YES Order Book Depth")
        self.assertIsNotNone(fig)

        bids_frame, asks_frame = orderbook_snapshots_to_matrices(
            [
                {
                    "captured_at": "2026-03-20T12:00:00+00:00",
                    "yes_levels": [{"price_cents": 40, "count_fp": "5.00"}],
                    "no_levels": [{"price_cents": 55, "count_fp": "2.00"}],
                },
                {
                    "captured_at": "2026-03-20T12:01:00+00:00",
                    "bids": {41: 7.0},
                    "asks": {44: 3.0},
                },
            ]
        )
        self.assertEqual(float(bids_frame.loc[bids_frame.index[0], 40]), 5.0)
        self.assertEqual(float(asks_frame.loc[asks_frame.index[0], 45]), 2.0)
        self.assertEqual(float(asks_frame.loc[asks_frame.index[1], 44]), 3.0)

        fig2, ax2 = plot_orderbook_matrix_snapshot(0, bids_frame, asks_frame)
        self.assertIsNotNone(fig2)
        self.assertEqual(ax2.get_xlabel(), "Price (c)")

    def test_build_market_family_analysis_uses_top_n(self):
        raw_markets = [
            {"ticker": "HIGHMIA-1", "yes_sub_title": "Above 85", "volume": 1000},
            {"ticker": "HIGHMIA-2", "yes_sub_title": "Above 90", "volume": 500},
        ]

        def fake_load_market_history(ticker, **kwargs):
            return sample_history(ticker, "Above 85" if ticker == "HIGHMIA-1" else "Above 90")

        with patch("mykalshi.research.family_analysis.market.get_all_markets", return_value=raw_markets), patch(
            "mykalshi.research.family_analysis.load_market_history",
            side_effect=fake_load_market_history,
        ):
            family = build_market_family_analysis("HIGHMIA", top_n=1)

        self.assertEqual(family.series_ticker, "HIGHMIA")
        self.assertEqual(family.markets["ticker"].tolist(), ["HIGHMIA-1"])
        self.assertEqual(list(family.histories.keys()), ["HIGHMIA-1"])
        self.assertIn("Above 85_mid", family.panel.columns)

    def test_sync_market_universe_writes_filtered_snapshot(self):
        live_market = {
            "market_ticker": "PRES-2024-DJT",
            "event_ticker": "PRES-2024",
            "market_title": "Trump",
            "status": "open",
        }
        historical_market = {
            "ticker": "PRES-2020-DJT",
            "event_ticker": "PRES-2020",
            "title": "Trump 2020",
            "status": "settled",
        }

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "mykalshi.research.universe.discovery.search_markets",
            return_value=[live_market],
        ), patch(
            "mykalshi.research.universe.historical.get_all_historical_markets",
            return_value=[historical_market],
        ):
            snapshot = sync_market_universe(tmpdir, UniverseSpec(event_ticker="PRES-2024", include_historical=True))
            reopened = open_market_universe(tmpdir)
            frame = reopened.to_dataframe()

        self.assertEqual(snapshot.summary()["market_count"], 2)
        self.assertEqual(reopened.manifest["market_count"], 2)
        self.assertEqual(len(frame), 2)

    def test_research_session_wraps_new_analysis_helpers(self):
        with patch("mykalshi.research.workflows.build_event_closeup", return_value="closeup"), patch(
            "mykalshi.research.workflows.build_market_family_analysis",
            return_value="family",
        ), patch(
            "mykalshi.research.workflows.get_orderbook_snapshot",
            return_value="lob",
        ), patch(
            "mykalshi.research.workflows.sync_market_universe",
            return_value="snapshot",
        ):
            session = ResearchSession()
            self.assertEqual(session.build_event_closeup("PRES-2024"), "closeup")
            self.assertEqual(session.build_market_family_analysis("HIGHMIA"), "family")
            self.assertEqual(session.get_orderbook_snapshot("TEST"), "lob")
            self.assertEqual(session.sync_market_universe("cache", UniverseSpec(series_ticker="HIGHMIA")), "snapshot")

    def test_chart_helpers_render_market_candles_and_comparison(self):
        history = sample_history("HIGHMIA-1", "Above 85")

        fig, axes = plot_market_candles(history, volume=True)
        self.assertIsNotNone(fig)
        self.assertGreaterEqual(len(axes), 1)

        fig2, ax2 = plot_market_comparison({"HIGHMIA-1": history}, metric="mid")
        self.assertIsNotNone(fig2)
        self.assertEqual(ax2.get_ylabel(), "Mid (cents)")


if __name__ == "__main__":
    unittest.main()
