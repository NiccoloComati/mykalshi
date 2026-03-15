from __future__ import annotations

import unittest

from mykalshi.research import EventDrivenBacktestEngine, KalshiStrategy
from mykalshi.research.engine import SettlementEvent, TradeMarketEvent


def trade_event(
    timestamp: str,
    *,
    market_ticker: str,
    yes_price_cents: int,
    trade_quantity: str = "1.00",
) -> TradeMarketEvent:
    return TradeMarketEvent(
        timestamp=timestamp,
        event_type="trade",
        market_ticker=market_ticker,
        yes_price_cents=yes_price_cents,
        no_price_cents=100 - yes_price_cents,
        trade_quantity=trade_quantity,
        raw_data={
            "created_time": timestamp,
            "ticker": market_ticker,
            "yes_price_dollars": f"{yes_price_cents / 100:.4f}",
            "no_price_dollars": f"{(100 - yes_price_cents) / 100:.4f}",
            "count_fp": trade_quantity,
        },
    )


class BuyAndHoldYesStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.entered = False

    def on_trade(self, context, event):
        if not self.entered:
            context.buy_yes(event.market_ticker, quantity=1)
            self.entered = True


class TwoMarketSettlementStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.entered = set()

    def on_trade(self, context, event):
        if event.market_ticker in self.entered:
            return
        if event.market_ticker.endswith("-YES"):
            context.buy_yes(event.market_ticker, quantity=1)
        else:
            context.buy_no(event.market_ticker, quantity=1)
        self.entered.add(event.market_ticker)


class ReportingTests(unittest.TestCase):
    def test_position_snapshot_tracks_unrealized_pnl_from_latest_mark(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=100).run(
            [
                trade_event("2026-03-15T12:00:00Z", market_ticker="FED-OPEN", yes_price_cents=40),
                trade_event("2026-03-15T12:01:00Z", market_ticker="FED-OPEN", yes_price_cents=55),
            ],
            BuyAndHoldYesStrategy(),
        )

        position = result.position("FED-OPEN")
        self.assertIsNotNone(position)
        self.assertEqual(str(position.yes_quantity), "1.00")
        self.assertEqual(str(position.realized_pnl_cents), "0.00")
        self.assertEqual(str(position.unrealized_pnl_cents), "15.00")
        self.assertEqual(str(position.market_value_cents), "55.00")

        summary = result.summary()
        self.assertEqual(summary["net_profit_cents"], "15.00")
        self.assertEqual(summary["unrealized_pnl_cents"], "15.00")
        self.assertEqual(summary["peak_exposure_cents"], "55.00")

        market_summary = result.market_summary("FED-OPEN")
        self.assertIsNotNone(market_summary)
        self.assertEqual(str(market_summary.total_pnl_cents), "15.00")
        self.assertEqual(str(market_summary.turnover_cents), "40.00")

    def test_market_summaries_and_summary_metrics_cover_multi_market_results(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                trade_event("2026-03-15T12:00:00Z", market_ticker="FAMILY-YES", yes_price_cents=40),
                trade_event("2026-03-15T12:00:01Z", market_ticker="FAMILY-NO", yes_price_cents=55),
                SettlementEvent(
                    timestamp="2026-03-15T12:01:00Z",
                    event_type="settlement",
                    market_ticker="FAMILY-YES",
                    yes_payout_cents=100,
                    no_payout_cents=0,
                ),
                SettlementEvent(
                    timestamp="2026-03-15T12:01:01Z",
                    event_type="settlement",
                    market_ticker="FAMILY-NO",
                    yes_payout_cents=0,
                    no_payout_cents=100,
                ),
            ],
            TwoMarketSettlementStrategy(),
        )

        summary = result.summary()
        self.assertEqual(summary["net_profit_cents"], "115.00")
        self.assertEqual(summary["turnover_cents"], "85.00")
        self.assertEqual(summary["peak_exposure_cents"], "85.00")
        self.assertEqual(summary["return_pct"], "57.50")
        self.assertEqual(summary["market_count"], 2)
        self.assertEqual(summary["orders_with_fill_count"], 2)
        self.assertEqual(summary["taker_fill_count"], 2)

        market_summaries = {entry.market_ticker: entry for entry in result.market_summaries()}
        yes_market = market_summaries["FAMILY-YES"]
        no_market = market_summaries["FAMILY-NO"]
        self.assertEqual(str(yes_market.realized_pnl_cents), "60.00")
        self.assertEqual(str(yes_market.turnover_cents), "40.00")
        self.assertEqual(yes_market.fill_count, 1)
        self.assertEqual(str(no_market.realized_pnl_cents), "55.00")
        self.assertEqual(str(no_market.turnover_cents), "45.00")
        self.assertEqual(no_market.fill_count, 1)

    def test_record_and_dataframe_exports_cover_engine_outputs(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=100).run(
            [
                trade_event("2026-03-15T12:00:00Z", market_ticker="FED-OPEN", yes_price_cents=40),
                trade_event("2026-03-15T12:01:00Z", market_ticker="FED-OPEN", yes_price_cents=55),
            ],
            BuyAndHoldYesStrategy(),
        )

        self.assertIn("market_ticker", result.position_records()[0])
        self.assertIn("action", result.fill_records()[0])
        self.assertIn("market_equity_cents", result.mark_records()[0])
        self.assertIn("event_type", result.event_records()[0])
        self.assertIn("market_ticker", result.market_summary_records()[0])

        dataframes = result.to_dataframes()
        self.assertEqual(
            sorted(dataframes.keys()),
            ["events", "fills", "logs", "markets", "marks", "order_events", "orders", "positions"],
        )
        self.assertEqual(len(dataframes["fills"]), 1)
        self.assertEqual(len(dataframes["positions"]), 1)
        self.assertEqual(len(dataframes["markets"]), 1)


if __name__ == "__main__":
    unittest.main()
