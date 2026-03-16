from __future__ import annotations

import unittest
from unittest.mock import patch

from mykalshi import events, market


class MarketHelperTests(unittest.TestCase):
    def test_get_market_orderbook_adds_legacy_compatibility_shape(self):
        payload = {
            "orderbook_fp": {
                "yes_dollars": [["0.4500", "10.00"]],
                "no_dollars": [["0.6000", "5.00"]],
            }
        }

        with patch("mykalshi.market.kalshi_get", return_value=payload):
            response = market.get_market_orderbook("TEST-YES")

        self.assertEqual(response["orderbook"]["yes"], [[45, 10.0]])
        self.assertEqual(response["orderbook"]["no"], [[60, 5.0]])

    def test_candlesticks_to_df_normalizes_fixed_point_candles(self):
        response = {
            "candlesticks": [
                {
                    "end_period_ts": 1773460800,
                    "volume_fp": "25.00",
                    "open_interest_fp": "19890.00",
                    "price": {
                        "open_dollars": "0.1200",
                        "high_dollars": "0.1300",
                        "low_dollars": "0.1000",
                        "close_dollars": "0.1100",
                    },
                    "yes_bid": {"close_dollars": "0.1000"},
                    "yes_ask": {"close_dollars": "0.1200"},
                }
            ]
        }

        dataframe = market.candlesticks_to_df(response)

        self.assertEqual(float(dataframe.loc[0, "volume"]), 25.0)
        self.assertEqual(float(dataframe.loc[0, "open_interest"]), 19890.0)
        self.assertEqual(dataframe.loc[0, "price_open"], 12)
        self.assertEqual(dataframe.loc[0, "price_close"], 11)
        self.assertEqual(dataframe.loc[0, "yes_bid_close"], 10)
        self.assertEqual(dataframe.loc[0, "yes_ask_close"], 12)

    def test_build_candlestick_uses_cents_from_dollar_fields(self):
        response = {
            "candlesticks": [
                {
                    "end_period_ts": 1773460800,
                    "volume_fp": "25.00",
                    "price": {
                        "open_dollars": "0.1200",
                        "high_dollars": "0.1300",
                        "low_dollars": "0.1000",
                        "close_dollars": "0.1100",
                    },
                    "yes_bid": {"close_dollars": "0.1000"},
                }
            ]
        }

        dataframe = market.build_candlestick(response)

        self.assertEqual(float(dataframe.iloc[0]["Open"]), 12.0)
        self.assertEqual(float(dataframe.iloc[0]["High"]), 13.0)
        self.assertEqual(float(dataframe.iloc[0]["Low"]), 10.0)
        self.assertEqual(float(dataframe.iloc[0]["Close"]), 11.0)
        self.assertEqual(float(dataframe.iloc[0]["Volume"]), 25.0)

    def test_event_info_normalizes_current_market_shape(self):
        payload = {
            "event": {
                "event_ticker": "PRES-2024",
                "series_ticker": "PRES",
                "title": "Will Donald Trump win the 2024 presidential election?",
                "sub_title": "",
                "category": "Politics",
            },
            "markets": [
                {
                    "ticker": "PRES-2024-DJT",
                    "yes_sub_title": "Donald Trump",
                    "subtitle": "Donald Trump",
                    "strike_type": "candidate",
                    "last_price_dollars": "0.5100",
                    "yes_bid_dollars": "0.5000",
                    "yes_ask_dollars": "0.5200",
                    "no_bid_dollars": "0.4800",
                    "no_ask_dollars": "0.5000",
                    "volume_fp": "1000.00",
                    "open_time": "2024-01-01T00:00:00Z",
                    "close_time": "2024-11-05T23:59:00Z",
                    "status": "finalized",
                    "rules_primary": "rules",
                }
            ],
        }

        with patch("mykalshi.events.get_event", return_value=payload):
            result = events.event_info("PRES-2024")

        self.assertEqual(result["markets"].loc[0, "last_price"], 51)
        self.assertEqual(result["markets"].loc[0, "yes_bid"], 50)
        self.assertEqual(result["markets"].loc[0, "yes_ask"], 52)
        self.assertEqual(float(result["markets"].loc[0, "volume"]), 1000.0)


if __name__ == "__main__":
    unittest.main()
