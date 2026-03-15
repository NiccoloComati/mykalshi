from __future__ import annotations

import unittest

from mykalshi.research import ProbabilityEdgeStrategy, ThresholdSignalStrategy, TradeBacktester


class StrategyHelpersTests(unittest.TestCase):
    def test_threshold_signal_strategy_targets_yes_flat_then_no(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
                "score": "0.80",
            },
            {
                "created_time": "2026-03-15T12:01:00Z",
                "yes_price_dollars": "0.4500",
                "no_price_dollars": "0.5500",
                "score": "0.10",
            },
            {
                "created_time": "2026-03-15T12:02:00Z",
                "yes_price_dollars": "0.3500",
                "no_price_dollars": "0.6500",
                "score": "-0.90",
            },
        ]

        strategy = ThresholdSignalStrategy(
            signal_fn=lambda context, trade: trade["score"],
            yes_threshold="0.50",
            no_threshold="-0.50",
            target_quantity=1,
        )

        result = TradeBacktester().run(
            trades,
            strategy,
            ticker="FED-23DEC-T3.00",
            initial_cash_cents=200,
        )

        self.assertEqual([fill.action for fill in result.fills], ["buy_yes", "sell_yes", "buy_no"])
        self.assertEqual(str(result.yes_position), "0.00")
        self.assertEqual(str(result.no_position), "1.00")

    def test_probability_edge_strategy_uses_hysteresis_to_hold_then_exit(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            },
            {
                "created_time": "2026-03-15T12:01:00Z",
                "yes_price_dollars": "0.4500",
                "no_price_dollars": "0.5500",
            },
            {
                "created_time": "2026-03-15T12:02:00Z",
                "yes_price_dollars": "0.4700",
                "no_price_dollars": "0.5300",
            },
        ]
        estimates = iter(["0.60", "0.50", "0.48"])
        strategy = ProbabilityEdgeStrategy(
            probability_fn=lambda context, trade: next(estimates),
            enter_edge_cents=10,
            exit_edge_cents=3,
            target_quantity=1,
        )

        result = TradeBacktester().run(
            trades,
            strategy,
            ticker="FED-23DEC-T3.00",
            initial_cash_cents=200,
        )

        self.assertEqual([fill.action for fill in result.fills], ["buy_yes", "sell_yes"])
        self.assertEqual(len(result.orders), 2)
        self.assertEqual(str(result.yes_position), "0.00")
        self.assertEqual(str(result.no_position), "0.00")


if __name__ == "__main__":
    unittest.main()
