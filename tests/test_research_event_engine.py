from __future__ import annotations

import unittest

from mykalshi.research import EventDrivenBacktestEngine, HistoricalTradeReplay, KalshiStrategy, MarketDataReplay
from mykalshi.research.engine import OrderbookMarketEvent, SettlementEvent, TradeMarketEvent


def trade_event(
    timestamp: str,
    *,
    market_ticker: str = "FED-23DEC-T3.00",
    yes_price_cents: int,
    no_price_cents: int | None = None,
    trade_quantity: str = "1.00",
):
    return TradeMarketEvent(
        timestamp=timestamp,
        event_type="trade",
        market_ticker=market_ticker,
        yes_price_cents=yes_price_cents,
        no_price_cents=100 - yes_price_cents if no_price_cents is None else no_price_cents,
        trade_quantity=trade_quantity,
        raw_data={
            "created_time": timestamp,
            "ticker": market_ticker,
            "yes_price_dollars": f"{yes_price_cents / 100:.4f}",
            "no_price_dollars": f"{(100 - yes_price_cents) / 100:.4f}",
            "count_fp": trade_quantity,
        },
    )


def book_event(
    timestamp: str,
    *,
    market_ticker: str = "FED-23DEC-T3.00",
    yes_bid: int | None = None,
    yes_ask: int | None = None,
    no_bid: int | None = None,
    no_ask: int | None = None,
    yes_size: str = "2.00",
    no_size: str = "2.00",
):
    yes_levels = [(yes_bid, yes_size)] if yes_bid is not None else ()
    no_levels = [(no_bid, no_size)] if no_bid is not None else ()
    return OrderbookMarketEvent(
        timestamp=timestamp,
        event_type="orderbook_snapshot",
        market_ticker=market_ticker,
        best_yes_bid_cents=yes_bid,
        best_yes_ask_cents=yes_ask,
        best_no_bid_cents=no_bid,
        best_no_ask_cents=no_ask,
        yes_levels=yes_levels,
        no_levels=no_levels,
    )


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


class ReservationConflictStrategy(KalshiStrategy):
    def on_orderbook(self, context, event):
        if context.open_orders(event.market_ticker):
            return
        context.buy_yes(event.market_ticker, quantity=2, limit_price_cents=40, tag="first")
        context.buy_yes(event.market_ticker, quantity=2, limit_price_cents=40, tag="second")


class InventoryConflictStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.stage = 0

    def on_trade(self, context, event):
        if self.stage == 0:
            context.buy_yes(event.market_ticker, quantity=2)
            self.stage = 1

    def on_orderbook(self, context, event):
        if self.stage != 1:
            return
        if context.open_orders(event.market_ticker):
            return
        context.sell_yes(event.market_ticker, quantity=2, limit_price_cents=60, tag="first")
        context.sell_yes(event.market_ticker, quantity=2, limit_price_cents=60, tag="second")
        self.stage = 2


class CancelReplaceStrategy(KalshiStrategy):
    def __init__(self, *, submit_before_cancel: bool = False) -> None:
        self.submit_before_cancel = submit_before_cancel
        self.submitted = False

    def on_orderbook(self, context, event):
        open_orders = context.open_orders(event.market_ticker)
        if not self.submitted:
            context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=40, tag="original")
            self.submitted = True
            return
        if not open_orders:
            return
        original_order_id = open_orders[0].order_id
        if self.submit_before_cancel:
            context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=39, tag="replacement")
            context.cancel(original_order_id)
        else:
            context.cancel(original_order_id)
            context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=39, tag="replacement")


class PartialFillThenCancelStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.submitted = False
        self.canceled = False

    def on_trade(self, context, event):
        if not self.submitted:
            context.buy_yes(event.market_ticker, quantity=2, limit_price_cents=50)
            self.submitted = True
            return
        if self.submitted and not self.canceled:
            open_orders = context.open_orders(event.market_ticker)
            if open_orders:
                context.cancel(open_orders[0].order_id)
                self.canceled = True


class CancelAfterFillStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.filled_order_id = None

    def on_trade(self, context, event):
        if self.filled_order_id is None and not context.open_orders(event.market_ticker):
            context.buy_yes(event.market_ticker, quantity=1)

    def on_fill(self, context, event):
        self.filled_order_id = event.order_id
        context.cancel(event.order_id)


class LaterCashInsufficientStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.stage = 0

    def on_trade(self, context, event):
        if self.stage == 0:
            context.buy_yes(event.market_ticker, quantity=1)
            self.stage = 1
            return
        if self.stage == 1:
            context.buy_no(event.market_ticker, quantity=1, limit_price_cents=50)
            self.stage = 2


class YesNoAccountingStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.stage = 0

    def on_trade(self, context, event):
        if self.stage == 0:
            context.buy_yes(event.market_ticker, quantity=1)
            self.stage = 1
        elif self.stage == 1:
            context.buy_no(event.market_ticker, quantity=1)
            self.stage = 2




class PassiveRestingStrategy(KalshiStrategy):
    def __init__(self, *, limit_price_cents: int, quantity: int | str = 1) -> None:
        self.limit_price_cents = limit_price_cents
        self.quantity = quantity
        self.submitted = False

    def on_orderbook(self, context, event):
        if not self.submitted:
            context.buy_yes(event.market_ticker, quantity=self.quantity, limit_price_cents=self.limit_price_cents)
            self.submitted = True




class PassiveOnTickerStrategy(KalshiStrategy):
    def __init__(self, *, limit_price_cents: int) -> None:
        self.limit_price_cents = limit_price_cents
        self.submitted = False

    def on_ticker(self, context, event):
        if not self.submitted:
            context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=self.limit_price_cents)
            self.submitted = True

class AggressiveVsPassiveStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.submitted = False

    def on_orderbook(self, context, event):
        if self.submitted:
            return
        context.buy_yes(event.market_ticker, quantity=1)
        context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=40, tag="passive")
        self.submitted = True
class SettlementStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.stage = 0

    def on_trade(self, context, event):
        if self.stage == 0:
            context.buy_yes(event.market_ticker, quantity=2)
            self.stage = 1

    def on_orderbook(self, context, event):
        if self.stage != 1 or context.open_orders(event.market_ticker):
            return
        context.sell_yes(event.market_ticker, quantity=1, limit_price_cents=70)
        self.stage = 2


class MultipleMarketFamilyStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.done = set()

    def on_trade(self, context, event):
        if event.market_ticker in self.done:
            return
        if event.market_ticker.endswith("YES"):
            context.buy_yes(event.market_ticker, quantity=1)
        else:
            context.buy_no(event.market_ticker, quantity=1)
        self.done.add(event.market_ticker)




class LatencyAggressiveStrategy(KalshiStrategy):
    def __init__(self, *, latency_events: int) -> None:
        self.latency_events = latency_events
        self.submitted = False

    def on_orderbook(self, context, event):
        if self.submitted:
            return
        context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=100, latency_events=self.latency_events)
        self.submitted = True


class InvalidLatencyStrategy(KalshiStrategy):
    def on_orderbook(self, context, event):
        context.buy_yes(event.market_ticker, quantity=1, latency_events=-1)
class LiquidityRoleFeeModel:
    def __call__(self, order, market_event, execution_price_cents, quantity, liquidity_role=None):
        if liquidity_role == "passive":
            return 1
        if liquidity_role == "aggressive":
            return 4
        return 0


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
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                trade_event("2026-03-15T12:00:00Z", yes_price_cents=45, trade_quantity="1.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=45, trade_quantity="1.00"),
            ],
            PartialFillStrategy(),
        )

        self.assertEqual(str(result.fills[0].quantity), "1.00")
        self.assertEqual(str(result.fills[1].quantity), "1.00")
        self.assertEqual([event.status for event in result.order_events], ["accepted", "partially_filled", "filled"])
        self.assertEqual(str(result.final_orders[0].reserved_cash_cents), "0.00")
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

    def test_reservation_rejects_overlapping_resting_buy_orders(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=100).run(
            [book_event("2026-03-15T12:00:00Z", yes_bid=35, yes_ask=50, no_bid=50, no_ask=65)],
            ReservationConflictStrategy(),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "rejected"])
        self.assertIn("Insufficient available cash", result.order_events[1].reason)
        self.assertEqual(str(result.final_orders[0].reserved_cash_cents), "80.00")

    def test_reservation_rejects_overlapping_inventory_orders(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                trade_event("2026-03-15T12:00:00Z", yes_price_cents=40, trade_quantity="2.00"),
                book_event("2026-03-15T12:01:00Z", yes_bid=35, yes_ask=50, no_bid=50, no_ask=65),
            ],
            InventoryConflictStrategy(),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "filled", "accepted", "rejected"])
        self.assertIn("Insufficient available yes inventory", result.order_events[-1].reason)

    def test_cancel_releases_reservation_before_replace_when_queued_first(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=100).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=35, yes_ask=50, no_bid=50, no_ask=65),
                book_event("2026-03-15T12:01:00Z", yes_bid=35, yes_ask=50, no_bid=50, no_ask=65),
            ],
            CancelReplaceStrategy(submit_before_cancel=False),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "canceled", "accepted"])
        self.assertEqual(result.final_orders[-1].tag, "replacement")
        self.assertEqual(result.final_orders[-1].status, "accepted")

    def test_submit_before_cancel_is_rejected_when_old_order_still_reserves_cash(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=40).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=35, yes_ask=50, no_bid=50, no_ask=65),
                book_event("2026-03-15T12:01:00Z", yes_bid=35, yes_ask=50, no_bid=50, no_ask=65),
            ],
            CancelReplaceStrategy(submit_before_cancel=True),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "rejected", "canceled"])
        self.assertEqual(result.final_orders[0].status, "canceled")
        self.assertEqual(result.final_orders[1].status, "rejected")

    def test_partial_fill_then_cancel_releases_remaining_reservation(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                trade_event("2026-03-15T12:00:00Z", yes_price_cents=45, trade_quantity="1.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=60, trade_quantity="1.00"),
            ],
            PartialFillThenCancelStrategy(),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "partially_filled", "canceled"])
        self.assertEqual(str(result.final_orders[0].reserved_cash_cents), "0.00")
        self.assertEqual(str(result.fills[0].cash_after_cents), "155.00")

    def test_attempted_cancel_after_full_fill_does_not_overwrite_order_state(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=100).run(
            [trade_event("2026-03-15T12:00:00Z", yes_price_cents=40, trade_quantity="1.00")],
            CancelAfterFillStrategy(),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "filled", "filled"])
        self.assertEqual(result.final_orders[0].status, "filled")
        self.assertEqual(result.order_events[-1].reason, "Order is not cancelable in its current state")

    def test_insufficient_cash_after_first_fill_rejects_later_order(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=100).run(
            [
                trade_event("2026-03-15T12:00:00Z", yes_price_cents=60, trade_quantity="1.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=45, trade_quantity="1.00"),
            ],
            LaterCashInsufficientStrategy(),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "filled", "rejected"])
        self.assertIn("Insufficient available cash", result.order_events[-1].reason)

    def test_yes_and_no_side_accounting_is_tracked_cleanly(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                trade_event("2026-03-15T12:00:00Z", yes_price_cents=40, trade_quantity="1.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=60, trade_quantity="1.00"),
            ],
            YesNoAccountingStrategy(),
        )

        self.assertEqual([fill.action for fill in result.fills], ["buy_yes", "buy_no"])
        self.assertEqual(str(result.fills[-1].yes_position), "1.00")
        self.assertEqual(str(result.fills[-1].no_position), "1.00")
        self.assertEqual(str(result.final_equity_cents), "220.00")

    def test_passive_limit_rests_without_immediate_fill(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=20, yes_ask=60, no_bid=40, no_ask=80, no_size="3.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=45, trade_quantity="1.00"),
            ],
            PassiveRestingStrategy(limit_price_cents=40),
        )

        self.assertEqual(len(result.fills), 0)
        self.assertEqual(result.final_orders[0].status, "accepted")
        self.assertEqual(str(result.final_orders[0].queue_ahead_quantity), "3.00")
        self.assertEqual(result.final_orders[0].liquidity_intent, "passive")

    def test_passive_order_partially_fills_across_events(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=20, yes_ask=60, no_bid=40, no_ask=80, no_size="0.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=40, trade_quantity="0.60"),
                trade_event("2026-03-15T12:02:00Z", yes_price_cents=40, trade_quantity="0.40"),
            ],
            PassiveRestingStrategy(limit_price_cents=40),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "partially_filled", "filled"])
        self.assertEqual([str(fill.quantity) for fill in result.fills], ["0.60", "0.40"])
        self.assertTrue(all(fill.liquidity_role == "passive" for fill in result.fills))

    def test_aggressive_and_passive_orders_execute_differently_on_same_replay(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=300).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=35, yes_ask=55, no_bid=45, no_ask=65, no_size="1.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=40, trade_quantity="1.00"),
            ],
            AggressiveVsPassiveStrategy(),
        )

        self.assertEqual([fill.liquidity_role for fill in result.fills], ["aggressive", "passive"])
        self.assertEqual([fill.price_cents for fill in result.fills], [55, 40])
        self.assertEqual([event.status for event in result.order_events], ["accepted", "accepted", "filled", "filled"])

    def test_orderbook_size_drop_consumes_queue_ahead(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=20, yes_ask=60, no_bid=40, no_ask=80, no_size="3.00"),
                book_event("2026-03-15T12:01:00Z", yes_bid=20, yes_ask=60, no_bid=40, no_ask=80, no_size="1.00"),
                trade_event("2026-03-15T12:02:00Z", yes_price_cents=40, trade_quantity="2.00"),
            ],
            PassiveRestingStrategy(limit_price_cents=40),
        )

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].timestamp, "2026-03-15T12:02:00Z")

    def test_ticker_size_drop_can_consume_top_of_book_queue(self):
        ticker_events = [
            {
                "captured_at": "2026-03-15T12:00:00Z",
                "event_type": "ticker",
                "channel": "ticker",
                "market_ticker": "FED-23DEC-T3.00",
                "yes_bid_cents": 40,
                "yes_ask_cents": 60,
                "yes_bid_size_fp": "3.00",
                "yes_ask_size_fp": "2.00",
            },
            {
                "captured_at": "2026-03-15T12:01:00Z",
                "event_type": "ticker",
                "channel": "ticker",
                "market_ticker": "FED-23DEC-T3.00",
                "yes_bid_cents": 40,
                "yes_ask_cents": 60,
                "yes_bid_size_fp": "1.00",
                "yes_ask_size_fp": "2.00",
            },
            {
                "captured_at": "2026-03-15T12:02:00Z",
                "event_type": "trade",
                "channel": "trade",
                "market_ticker": "FED-23DEC-T3.00",
                "yes_price_cents": 40,
                "no_price_cents": 60,
                "count_fp": "1.00",
            },
            {
                "captured_at": "2026-03-15T12:03:00Z",
                "event_type": "trade",
                "channel": "trade",
                "market_ticker": "FED-23DEC-T3.00",
                "yes_price_cents": 40,
                "no_price_cents": 60,
                "count_fp": "1.00",
            },
        ]

        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            MarketDataReplay.from_market_data_events(ticker_events),
            PassiveOnTickerStrategy(limit_price_cents=40),
        )

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].timestamp, "2026-03-15T12:03:00Z")

    def test_latency_events_delay_aggressive_fill(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=35, yes_ask=55, no_bid=45, no_ask=65, no_size="2.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=55, trade_quantity="1.00"),
                trade_event("2026-03-15T12:02:00Z", yes_price_cents=55, trade_quantity="1.00"),
            ],
            LatencyAggressiveStrategy(latency_events=1),
        )

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].timestamp, "2026-03-15T12:01:00Z")

    def test_negative_latency_is_rejected(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [book_event("2026-03-15T12:00:00Z", yes_bid=35, yes_ask=55, no_bid=45, no_ask=65, no_size="2.00")],
            InvalidLatencyStrategy(),
        )

        self.assertEqual(result.order_events[0].status, "rejected")
        self.assertIn("latency_events", str(result.order_events[0].reason))

    def test_fee_model_receives_liquidity_role_for_maker_taker_pricing(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=400, fee_model=LiquidityRoleFeeModel()).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=35, yes_ask=55, no_bid=45, no_ask=65, no_size="1.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=40, trade_quantity="1.00"),
                trade_event("2026-03-15T12:02:00Z", yes_price_cents=40, trade_quantity="1.00"),
            ],
            AggressiveVsPassiveStrategy(),
        )

        self.assertEqual([fill.liquidity_role for fill in result.fills], ["aggressive", "passive"])
        self.assertEqual([str(fill.fee_cents) for fill in result.fills], ["4.00", "1.00"])

    def test_queue_ahead_delays_passive_fill_until_consumed(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                book_event("2026-03-15T12:00:00Z", yes_bid=20, yes_ask=60, no_bid=40, no_ask=80, no_size="2.00"),
                trade_event("2026-03-15T12:01:00Z", yes_price_cents=40, trade_quantity="1.00"),
                trade_event("2026-03-15T12:02:00Z", yes_price_cents=40, trade_quantity="1.00"),
                trade_event("2026-03-15T12:03:00Z", yes_price_cents=40, trade_quantity="1.00"),
            ],
            PassiveRestingStrategy(limit_price_cents=40),
        )

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].timestamp, "2026-03-15T12:03:00Z")
        self.assertEqual(result.fills[0].liquidity_role, "passive")

    def test_settlement_cancels_open_orders_and_realizes_payout(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                trade_event("2026-03-15T12:00:00Z", yes_price_cents=40, trade_quantity="2.00"),
                book_event("2026-03-15T12:01:00Z", yes_bid=35, yes_ask=50, no_bid=50, no_ask=65),
                SettlementEvent(
                    timestamp="2026-03-15T12:02:00Z",
                    event_type="settlement",
                    market_ticker="FED-23DEC-T3.00",
                    yes_payout_cents=100,
                    no_payout_cents=0,
                ),
            ],
            SettlementStrategy(),
        )

        self.assertEqual([event.status for event in result.order_events], ["accepted", "filled", "accepted", "canceled"])
        self.assertEqual(str(result.final_cash_cents), "320.00")
        self.assertEqual(str(result.final_equity_cents), "320.00")
        self.assertEqual(result.final_orders[-1].status, "canceled")

    def test_missing_settlement_data_cancels_orders_but_waits_for_payout(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                trade_event("2026-03-15T12:00:00Z", yes_price_cents=40, trade_quantity="1.00"),
                book_event("2026-03-15T12:01:00Z", yes_bid=35, yes_ask=50, no_bid=50, no_ask=65),
                SettlementEvent(
                    timestamp="2026-03-15T12:02:00Z",
                    event_type="settlement",
                    market_ticker="FED-23DEC-T3.00",
                ),
                SettlementEvent(
                    timestamp="2026-03-15T12:03:00Z",
                    event_type="settlement",
                    market_ticker="FED-23DEC-T3.00",
                    yes_payout_cents=100,
                    no_payout_cents=0,
                ),
            ],
            SettlementStrategy(),
        )

        self.assertEqual(result.final_orders[-1].status, "canceled")
        self.assertEqual(str(result.final_cash_cents), "260.00")
        self.assertTrue(any("Settlement data missing" in log["message"] for log in result.logs))

    def test_multiple_markets_in_same_family_are_accounted_independently(self):
        result = EventDrivenBacktestEngine(initial_cash_cents=200).run(
            [
                trade_event("2026-03-15T12:00:00Z", market_ticker="FAMILY-YES", yes_price_cents=40, trade_quantity="1.00"),
                trade_event("2026-03-15T12:00:01Z", market_ticker="FAMILY-NO", yes_price_cents=55, trade_quantity="1.00"),
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
            MultipleMarketFamilyStrategy(),
        )

        self.assertEqual(str(result.final_cash_cents), "315.00")
        self.assertEqual(str(result.final_equity_cents), "315.00")
        self.assertEqual(len(result.final_orders), 2)


if __name__ == "__main__":
    unittest.main()
