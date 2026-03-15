from __future__ import annotations

import unittest
from unittest.mock import patch

from mykalshi.research.backtest import TradeBacktester, TradeSignal


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


if __name__ == "__main__":
    unittest.main()
