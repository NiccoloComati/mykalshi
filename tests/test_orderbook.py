from __future__ import annotations

import unittest

from mykalshi.orderbook import OrderbookState, extract_orderbook_levels
from mykalshi.research.websocket import build_orderbook_event


class OrderbookTests(unittest.TestCase):
    def test_extract_orderbook_levels_supports_rest_fixed_point_shape(self):
        yes_levels, no_levels = extract_orderbook_levels(
            {
                "orderbook_fp": {
                    "yes_dollars": [["0.1500", "100.00"]],
                    "no_dollars": [["0.6300", "25.00"]],
                }
            }
        )

        self.assertEqual(set(yes_levels), {15})
        self.assertEqual(set(no_levels), {63})

    def test_orderbook_state_applies_snapshot_and_delta(self):
        state = OrderbookState()
        snapshot = {
            "type": "orderbook_snapshot",
            "sid": 2,
            "seq": 2,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "market_id": "market-1",
                "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]],
                "no_dollars_fp": [["0.5400", "20.00"]],
            },
        }
        delta = {
            "type": "orderbook_delta",
            "sid": 2,
            "seq": 3,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "market_id": "market-1",
                "price_dollars": "0.2200",
                "delta_fp": "-33.00",
                "side": "yes",
                "ts": "2022-11-22T20:44:01Z",
            },
        }

        state.apply_snapshot(snapshot)
        state.apply_delta(delta)

        self.assertEqual(state.best_yes_bid_cents, 22)
        self.assertEqual(state.best_yes_ask_cents, 46)
        self.assertEqual(state.serialized_yes_levels()[1]["count_fp"], "300.00")

        event = build_orderbook_event(delta, state)
        self.assertEqual(event["price_cents"], 22)
        self.assertEqual(event["best_yes_bid_cents"], 22)
        self.assertIsNone(event["yes_levels"])


if __name__ == "__main__":
    unittest.main()
