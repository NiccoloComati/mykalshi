from __future__ import annotations

import unittest
from unittest.mock import patch

from mykalshi.research.backtest import KalshiTakerFeeModel, PositionTargetSignal, TradeBacktester, TradeSignal


class BuyThenSellStrategy:
    def __init__(self) -> None:
        self.calls = 0

    def on_trade(self, context, trade):
        self.calls += 1
        if self.calls == 1:
            return TradeSignal("buy_yes", quantity=1)
        if self.calls == 2:
            return TradeSignal("sell_yes", quantity=1)
        return None


class HookedStrategy:
    def __init__(self) -> None:
        self.started = False
        self.finished = False

    def on_start(self, context):
        self.started = True

    def on_trade(self, context, trade):
        return TradeSignal("buy_yes", quantity=1, limit_price_cents=30)

    def on_finish(self, context):
        self.finished = True


class TradeBacktesterTests(unittest.TestCase):
    def test_run_executes_yes_round_trip(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            },
            {
                "created_time": "2026-03-15T12:01:00Z",
                "yes_price_dollars": "0.5500",
                "no_price_dollars": "0.4500",
            },
        ]

        result = TradeBacktester().run(
            trades,
            BuyThenSellStrategy(),
            ticker="FED-23DEC-T3.00",
            initial_cash_cents=100,
        )

        self.assertEqual(str(result.final_cash_cents), "115.00")
        self.assertEqual(str(result.final_equity_cents), "115.00")
        self.assertEqual(str(result.yes_position), "0.00")
        self.assertEqual(len(result.fills), 2)
        self.assertEqual(len(result.orders), 2)

    def test_run_on_historical_trades_uses_historical_module(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            }
        ]

        with patch(
            "mykalshi.research.backtest.historical.get_all_historical_trades",
            return_value={"trades": trades},
        ) as mocked_get:
            result = TradeBacktester().run_on_historical_trades(
                "FED-23DEC-T3.00",
                lambda context, trade: None,
                initial_cash_cents=100,
            )

        mocked_get.assert_called_once()
        self.assertEqual(result.ticker, "FED-23DEC-T3.00")
        self.assertEqual(str(result.final_cash_cents), "100.00")
        self.assertEqual(len(result.marks), 1)

    def test_limit_order_can_be_rejected_without_fill(self):
        strategy = HookedStrategy()
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            }
        ]

        result = TradeBacktester().run(
            trades,
            strategy,
            ticker="FED-23DEC-T3.00",
            initial_cash_cents=100,
        )

        self.assertTrue(strategy.started)
        self.assertTrue(strategy.finished)
        self.assertEqual(len(result.fills), 0)
        self.assertEqual(result.orders[0].status, "rejected")
        self.assertEqual(str(result.final_cash_cents), "100.00")

    def test_kalshi_taker_fee_model_applies_rounded_fee(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.5000",
                "no_price_dollars": "0.5000",
            }
        ]

        result = TradeBacktester(fee_model=KalshiTakerFeeModel()).run(
            trades,
            lambda context, trade: TradeSignal("buy_yes", quantity=1),
            ticker="FED-23DEC-T3.00",
            initial_cash_cents=100,
        )

        self.assertEqual(str(result.total_fees_cents), "2.00")
        self.assertEqual(str(result.final_cash_cents), "48.00")

    def test_position_target_signal_flips_positions_in_sequence(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            }
        ]

        result = TradeBacktester().run(
            trades,
            lambda context, trade: PositionTargetSignal("yes", target_quantity=1),
            ticker="FED-23DEC-T3.00",
            initial_cash_cents=0,
            initial_no_position=2,
        )

        self.assertEqual([order.action for order in result.orders], ["sell_no", "buy_yes"])
        self.assertEqual(str(result.final_cash_cents), "80.00")
        self.assertEqual(str(result.yes_position), "1.00")
        self.assertEqual(str(result.no_position), "0.00")

    def test_position_target_signal_can_stage_transition_with_trade_budget(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            },
            {
                "created_time": "2026-03-15T12:01:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            },
        ]

        result = TradeBacktester().run(
            trades,
            lambda context, trade: PositionTargetSignal("yes", target_quantity=1, max_trade_quantity=2),
            ticker="FED-23DEC-T3.00",
            initial_cash_cents=0,
            initial_no_position=2,
        )

        self.assertEqual([order.action for order in result.orders], ["sell_no", "buy_yes"])
        self.assertEqual(result.orders[0].timestamp, "2026-03-15T12:00:00Z")
        self.assertEqual(result.orders[1].timestamp, "2026-03-15T12:01:00Z")
        self.assertEqual(str(result.yes_position), "1.00")
        self.assertEqual(str(result.no_position), "0.00")

    def test_insufficient_cash_rejects_order_instead_of_raising(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            }
        ]

        result = TradeBacktester().run(
            trades,
            lambda context, trade: TradeSignal("buy_yes", quantity=1),
            ticker="FED-23DEC-T3.00",
            initial_cash_cents=20,
        )

        self.assertEqual(result.orders[0].status, "rejected")
        self.assertIn("Insufficient cash", result.orders[0].reason)
        self.assertEqual(len(result.fills), 0)
        self.assertEqual(str(result.final_cash_cents), "20.00")
        self.assertEqual(result.summary()["rejected_order_count"], 1)

    def test_invalid_strategy_output_raises_clear_type_error(self):
        trades = [
            {
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
            }
        ]

        with self.assertRaisesRegex(TypeError, "TradeSignal, PositionTargetSignal"):
            TradeBacktester().run(
                trades,
                lambda context, trade: ["not-a-signal"],
                ticker="FED-23DEC-T3.00",
                initial_cash_cents=100,
            )


if __name__ == "__main__":
    unittest.main()
