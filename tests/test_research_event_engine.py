from __future__ import annotations

import unittest

from mykalshi.research import EventDrivenBacktestEngine, HistoricalTradeReplay, KalshiStrategy, MarketDataReplay


class BuyLowSellHighStrategy(KalshiStrategy):
    def on_trade(self, context, event):
        open_orders = context.open_orders(event.market_ticker)
        position = context.position(event.market_ticker)
        if position.yes_quantity == 0 and not open_orders and event.yes_price_cents <= 40:
            context.buy_yes(event.market_ticker, quantity=1)
        elif position.yes_quantity > 0 and not open_orders and event.yes_price_cents >= 55:
            context.sell_yes(event.market_ticker, quantity=1)


class PartialFillStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.submitted = False

    def on_trade(self, context, event):
        if not self.submitted:
            context.buy_yes(event.market_ticker, quantity=2, limit_price_cents=50)
            self.submitted = True


class CancelRestingOrderStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.canceled = False

    def on_orderbook(self, context, event):
        open_orders = context.open_orders(event.market_ticker)
        if not open_orders:
            context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=40)
            return
        if not self.canceled:
            context.cancel(open_orders[0].order_id)
            self.canceled = True


class EventDrivenBacktestEngineTests(unittest.TestCase):
    def test_historical_trade_replay_runs_through_event_engine(self):
        trades = [
            {
                "ticker": "FED-23DEC-T3.00",
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4000",
                "no_price_dollars": "0.6000",
                "count_fp": "1.00",
            },
            {
                "ticker": "FED-23DEC-T3.00",
                "created_time": "2026-03-15T12:01:00Z",
                "yes_price_dollars": "0.5500",
                "no_price_dollars": "0.4500",
                "count_fp": "1.00",
            },
        ]

        result = EventDrivenBacktestEngine(initial_cash_cents=100).run(
            HistoricalTradeReplay.from_trade_dicts(trades),
            BuyLowSellHighStrategy(),
        )

        self.assertEqual(str(result.final_cash_cents), "115.00")
        self.assertEqual(str(result.final_equity_cents), "115.00")
        self.assertEqual(len(result.fills), 2)
        self.assertEqual([event.status for event in result.order_events], ["accepted", "filled", "accepted", "filled"])

    def test_partial_fills_are_tracked_across_trade_events(self):
        trades = [
            {
                "ticker": "FED-23DEC-T3.00",
                "created_time": "2026-03-15T12:00:00Z",
                "yes_price_dollars": "0.4500",
                "no_price_dollars": "0.5500",
                "count_fp": "1.00",
            },
            {
                "ticker": "FED-23DEC-T3.00",
                "created_time": "2026-03-15T12:01:00Z",
                "yes_price_dollars": "0.4500",
                "no_price_dollars": "0.5500",
                "count_fp": "1.00",
            },
        ]

        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            HistoricalTradeReplay.from_trade_dicts(trades),
            PartialFillStrategy(),
        )

        self.assertEqual([fill.quantity for fill in result.fills], [result.fills[0].quantity, result.fills[1].quantity])
        self.assertEqual(str(result.fills[0].quantity), "1.00")
        self.assertEqual(str(result.fills[1].quantity), "1.00")
        self.assertEqual([event.status for event in result.order_events], ["accepted", "partially_filled", "filled"])
        self.assertEqual(str(result.final_equity_cents), "200.00")

    def test_market_data_replay_supports_resting_order_cancellation(self):
        market_data_events = [
            {
                "captured_at": "2026-03-15T12:00:00Z",
                "event_type": "orderbook_snapshot",
                "channel": "orderbook_delta",
                "market_ticker": "FED-23DEC-T3.00",
                "best_yes_bid_cents": 35,
                "best_yes_ask_cents": 50,
                "best_no_bid_cents": 50,
                "best_no_ask_cents": 65,
                "yes_levels": [{"price_cents": 35, "count_fp": "2.00"}],
                "no_levels": [{"price_cents": 50, "count_fp": "2.00"}],
            },
            {
                "captured_at": "2026-03-15T12:01:00Z",
                "event_type": "orderbook_snapshot",
                "channel": "orderbook_delta",
                "market_ticker": "FED-23DEC-T3.00",
                "best_yes_bid_cents": 36,
                "best_yes_ask_cents": 51,
                "best_no_bid_cents": 49,
                "best_no_ask_cents": 64,
                "yes_levels": [{"price_cents": 36, "count_fp": "2.00"}],
                "no_levels": [{"price_cents": 49, "count_fp": "2.00"}],
            },
        ]

        result = EventDrivenBacktestEngine(initial_cash_cents=100).run(
            MarketDataReplay.from_market_data_events(market_data_events),
            CancelRestingOrderStrategy(),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "canceled"])
        self.assertEqual(len(result.fills), 0)
        self.assertEqual(str(result.final_cash_cents), "100.00")


if __name__ == "__main__":
    unittest.main()
