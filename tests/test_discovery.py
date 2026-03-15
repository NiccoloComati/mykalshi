from __future__ import annotations

import unittest
from unittest.mock import patch

from mykalshi import discovery


class DiscoveryTests(unittest.TestCase):
    def test_search_series_filters_case_insensitively(self):
        with patch(
            "mykalshi.discovery.events.get_series_list",
            side_effect=[
                {
                    "series": [
                        {"ticker": "HIGHMIA", "title": "Highest temperature in Miami", "tags": ["Daily temperature"]},
                        {"ticker": "RAINNYC", "title": "Rain in New York", "tags": ["Precipitation"]},
                    ]
                }
            ],
        ):
            result = discovery.search_series(category="Climate and Weather", title_contains="miami")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["ticker"], "HIGHMIA")

    def test_search_events_uses_series_scope_filters(self):
        with patch(
            "mykalshi.discovery.events.get_series_list",
            return_value={"series": [{"ticker": "HIGHMIA", "title": "Highest temperature in Miami", "tags": []}]},
        ), patch(
            "mykalshi.discovery.events.get_events",
            return_value={
                "events": [
                    {
                        "event_ticker": "HIGHMIA-20260315",
                        "series_ticker": "HIGHMIA",
                        "title": "Highest temperature in Miami on March 15",
                        "sub_title": "Daily",
                        "category": "Climate and Weather",
                    }
                ]
            },
        ):
            result = discovery.search_events(
                series_title_contains="miami",
                event_title_contains="march 15",
                status="open",
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["event_ticker"], "HIGHMIA-20260315")

    def test_search_markets_enriches_event_context(self):
        with patch(
            "mykalshi.discovery.events.get_series_list",
            return_value={"series": [{"ticker": "HIGHMIA", "title": "Highest temperature in Miami", "tags": []}]},
        ), patch(
            "mykalshi.discovery.events.get_events",
            return_value={
                "events": [
                    {
                        "event_ticker": "HIGHMIA-20260315",
                        "series_ticker": "HIGHMIA",
                        "title": "Highest temperature in Miami on March 15",
                        "sub_title": "Daily",
                        "category": "Climate and Weather",
                    }
                ]
            },
        ), patch(
            "mykalshi.discovery.market.get_markets",
            return_value={
                "markets": [
                    {
                        "ticker": "HIGHMIA-20260315-B85",
                        "title": "Will the high in Miami exceed 85F?",
                        "subtitle": "Above 85F",
                        "status": "open",
                    }
                ]
            },
        ):
            result = discovery.search_markets(
                series_ticker="HIGHMIA",
                market_title_contains="exceed 85f",
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["market_ticker"], "HIGHMIA-20260315-B85")
        self.assertEqual(result[0]["event_ticker"], "HIGHMIA-20260315")
        self.assertEqual(result[0]["series_title"], "Highest temperature in Miami")

    def test_search_events_uses_direct_event_lookup_for_exact_event_ticker(self):
        with patch(
            "mykalshi.discovery.events.get_event",
            return_value={
                "event": {
                    "event_ticker": "KXELONMARS-99",
                    "series_ticker": "KXELONMARS",
                    "title": "Will Elon Musk visit Mars in his lifetime?",
                    "sub_title": "Before 2099",
                }
            },
        ) as mocked_get_event, patch(
            "mykalshi.discovery.events.get_all_events"
        ) as mocked_get_all_events:
            result = discovery.search_events(event_ticker="KXELONMARS-99")

        mocked_get_event.assert_called_once_with("KXELONMARS-99", with_nested_markets=False)
        mocked_get_all_events.assert_not_called()
        self.assertEqual(result[0]["event_ticker"], "KXELONMARS-99")

    def test_resolve_market_raises_on_ambiguous_result(self):
        with patch(
            "mykalshi.discovery.search_markets",
            return_value=[{"market_ticker": "A"}, {"market_ticker": "B"}],
        ):
            with self.assertRaises(LookupError):
                discovery.resolve_market(series_ticker="HIGHMIA")

    def test_search_markets_stops_paging_once_limit_is_satisfied(self):
        calls = []

        def fake_get_markets(*, status=None, limit=None, cursor=None, **kwargs):
            calls.append({"status": status, "limit": limit, "cursor": cursor})
            if cursor is None:
                return {
                    "markets": [
                        {
                            "ticker": "HIGHMIA-20260315-B85",
                            "title": "Will the high in Miami exceed 85F?",
                            "subtitle": "Above 85F",
                            "status": "open",
                            "event_ticker": "HIGHMIA-20260315",
                        }
                    ],
                    "cursor": "unused-next-page",
                }
            return {
                "markets": [
                    {
                        "ticker": "SHOULD-NOT-BE-FETCHED",
                        "title": "Unexpected extra page",
                        "subtitle": "",
                        "status": "open",
                        "event_ticker": "OTHER",
                    }
                ]
            }

        with patch("mykalshi.discovery.market.get_markets", side_effect=fake_get_markets):
            result = discovery.search_markets(status="open", limit=1)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["market_ticker"], "HIGHMIA-20260315-B85")
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
