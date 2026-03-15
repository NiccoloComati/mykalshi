from __future__ import annotations

import unittest
from unittest.mock import patch

from mykalshi import routing
from mykalshi.exceptions import KalshiHTTPError


class RoutingTests(unittest.TestCase):
    def test_resolve_trade_source_prefers_live_for_active_market_without_time_bounds(self):
        with patch(
            "mykalshi.routing.historical.get_historical_cutoff",
            return_value={
                "market_settled_ts": "2025-03-15T00:00:00Z",
                "orders_updated_ts": "2025-03-15T00:00:00Z",
                "trades_created_ts": "2025-03-15T00:00:00Z",
            },
        ), patch(
            "mykalshi.routing.market.get_market",
            return_value={"market": {"ticker": "LIVE-1", "close_time": "2026-03-15T00:00:00Z", "status": "open"}},
        ):
            route = routing.resolve_trade_source("LIVE-1")

        self.assertTrue(route["use_live"])
        self.assertFalse(route["use_historical"])

    def test_resolve_trade_source_falls_back_to_historical_on_live_404(self):
        with patch(
            "mykalshi.routing.historical.get_historical_cutoff",
            return_value={
                "market_settled_ts": "2025-03-15T00:00:00Z",
                "orders_updated_ts": "2025-03-15T00:00:00Z",
                "trades_created_ts": "2025-03-15T00:00:00Z",
            },
        ), patch(
            "mykalshi.routing.market.get_market",
            side_effect=KalshiHTTPError(404, "GET", "https://example.com", "not found"),
        ):
            route = routing.resolve_trade_source("HIST-1")

        self.assertFalse(route["use_live"])
        self.assertTrue(route["use_historical"])

    def test_resolve_trade_source_uses_historical_for_pre_cutoff_finalized_market(self):
        with patch(
            "mykalshi.routing.historical.get_historical_cutoff",
            return_value={
                "market_settled_ts": "2025-03-15T00:00:00Z",
                "orders_updated_ts": "2025-03-15T00:00:00Z",
                "trades_created_ts": "2025-03-15T00:00:00Z",
            },
        ), patch(
            "mykalshi.routing.market.get_market",
            return_value={"market": {"ticker": "OLD-1", "close_time": "2025-03-14T00:00:00Z", "status": "finalized"}},
        ):
            route = routing.resolve_trade_source("OLD-1")

        self.assertFalse(route["use_live"])
        self.assertTrue(route["use_historical"])

    def test_resolve_trade_source_uses_historical_when_finalized_market_has_archived_trades(self):
        with patch(
            "mykalshi.routing.historical.get_historical_cutoff",
            return_value={
                "market_settled_ts": "2025-03-15T00:00:00Z",
                "orders_updated_ts": "2025-03-15T00:00:00Z",
                "trades_created_ts": "2025-03-15T00:00:00Z",
            },
        ), patch(
            "mykalshi.routing.market.get_market",
            return_value={"market": {"ticker": "OLD-2", "close_time": "2025-03-15T05:27:37Z", "status": "finalized"}},
        ), patch(
            "mykalshi.routing.historical.get_historical_trades",
            return_value={"trades": [{"trade_id": "a"}]},
        ):
            route = routing.resolve_trade_source("OLD-2")

        self.assertFalse(route["use_live"])
        self.assertTrue(route["use_historical"])

    def test_get_trades_auto_merges_historical_and_live_ranges(self):
        with patch(
            "mykalshi.routing.resolve_trade_source",
            return_value={
                "ticker": "TEST-1",
                "cutoff_ts": 100,
                "start_ts": 50,
                "end_ts": 150,
                "use_historical": True,
                "use_live": True,
                "historical_range": {"min_ts": 50, "max_ts": 99},
                "live_range": {"min_ts": 100, "max_ts": 150},
            },
        ), patch(
            "mykalshi.routing.historical.get_all_historical_trades",
            return_value={"trades": [{"trade_id": "a", "created_time": "2025-03-14T00:00:00Z"}]},
        ) as mocked_hist, patch(
            "mykalshi.routing.market.get_all_trades",
            return_value={"trades": [{"trade_id": "b", "created_time": "2025-03-16T00:00:00Z"}]},
        ) as mocked_live:
            result = routing.get_trades_auto("TEST-1")

        mocked_hist.assert_called_once()
        mocked_live.assert_called_once()
        self.assertEqual(result["sources_used"], ["historical", "live"])
        self.assertEqual([trade["trade_id"] for trade in result["trades"]], ["a", "b"])


if __name__ == "__main__":
    unittest.main()
