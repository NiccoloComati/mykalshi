from __future__ import annotations

import unittest
from unittest.mock import patch

from mykalshi.research import KalshiStrategy, ResearchSession, ReplayDataset


def orderbook_snapshot_event(*, market_ticker: str = "FED-23DEC-T3.00") -> dict:
    return {
        "captured_at": "2026-03-15T12:00:00.000+00:00",
        "event_type": "orderbook_snapshot",
        "channel": "orderbook_delta",
        "sequence": 1,
        "market_ticker": market_ticker,
        "best_yes_bid_cents": 40,
        "best_yes_ask_cents": 45,
        "best_no_bid_cents": 55,
        "best_no_ask_cents": 60,
        "yes_levels": [{"price_cents": 40, "count_fp": "10.00"}],
        "no_levels": [{"price_cents": 55, "count_fp": "10.00"}],
        "raw_message": {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": market_ticker,
                "market_id": "market-1",
                "yes_dollars_fp": [["0.4000", "10.00"]],
                "no_dollars_fp": [["0.5500", "10.00"]],
            },
        },
    }


def ticker_event(*, market_ticker: str = "FED-23DEC-T3.00", captured_at: str = "2026-03-15T12:01:00.000+00:00") -> dict:
    return {
        "captured_at": captured_at,
        "event_type": "ticker",
        "channel": "ticker",
        "market_ticker": market_ticker,
        "yes_bid_cents": 60,
        "yes_ask_cents": 62,
        "yes_bid_size_fp": "25.00",
        "yes_ask_size_fp": "10.00",
        "sequence": 2,
    }


class BuyOnFirstOrderbookStrategy(KalshiStrategy):
    def __init__(self) -> None:
        self.submitted = False

    def on_orderbook(self, context, event):
        if self.submitted:
            return
        context.buy_yes(event.market_ticker, quantity=1)
        self.submitted = True


class ResearchWorkflowTests(unittest.TestCase):
    def test_research_session_wraps_series_and_event_results(self):
        series_payload = {
            "ticker": "KXELONMARS",
            "title": "Elon Musk on Mars",
            "category": "Technology",
            "tags": ["Space", "Mars"],
        }
        event_payload = {
            "event_ticker": "KXELONMARS-99",
            "series_ticker": "KXELONMARS",
            "title": "Will Elon Musk visit Mars in his lifetime?",
            "sub_title": "Before 2099",
            "category": "Technology",
            "status": "open",
        }
        with patch("mykalshi.research.workflows.discovery.search_series", return_value=[series_payload]), patch(
            "mykalshi.research.workflows.discovery.resolve_series",
            return_value=series_payload,
        ), patch("mykalshi.research.workflows.discovery.search_events", return_value=[event_payload]), patch(
            "mykalshi.research.workflows.discovery.resolve_event",
            return_value=event_payload,
        ):
            session = ResearchSession()
            series_result = session.search_series(query="mars")
            resolved_series = session.resolve_series(query="mars")
            event_result = session.search_events(query="mars")
            resolved_event = session.resolve_event(query="mars")

        self.assertEqual(series_result[0].ticker, "KXELONMARS")
        self.assertEqual(resolved_series.summary()["category"], "Technology")
        self.assertEqual(event_result[0].event_ticker, "KXELONMARS-99")
        self.assertEqual(resolved_event.summary()["series_ticker"], "KXELONMARS")

    def test_load_replay_dataset_summarizes_sources(self):
        dataset = ResearchSession().load_replay_dataset(
            market_data_source=[ticker_event(), ticker_event(market_ticker="OTHER-26")],
            orderbook_source=[orderbook_snapshot_event()],
            market_ticker="FED-23DEC-T3.00",
        )

        self.assertEqual(dataset.market_ticker, "FED-23DEC-T3.00")
        self.assertEqual(len(dataset.market_data_events), 1)
        self.assertEqual(len(dataset.orderbook_events), 1)
        self.assertEqual(len(dataset.replay_events), 2)
        self.assertEqual(
            dataset.summary(),
            {
                "market_ticker": "FED-23DEC-T3.00",
                "market_data_event_count": 1,
                "orderbook_event_count": 1,
                "replay_event_count": 2,
                "first_timestamp": "2026-03-15T12:00:00.000+00:00",
                "last_timestamp": "2026-03-15T12:01:00.000+00:00",
                "channel_counts": {"orderbook_delta": 1, "ticker": 1},
            },
        )

    def test_replay_dataset_backtest_uses_replay_backtester(self):
        dataset = ReplayDataset(
            market_ticker="FED-23DEC-T3.00",
            replay_events=[orderbook_snapshot_event()],
        )

        with patch("mykalshi.research.workflows.ReplayBacktester") as mocked_backtester_cls:
            runner = mocked_backtester_cls.return_value
            runner.run_on_replay_event_stream.return_value = "result"

            result = dataset.backtest(BuyOnFirstOrderbookStrategy())

        self.assertEqual(result, "result")
        runner.run_on_replay_event_stream.assert_called_once()
        args, kwargs = runner.run_on_replay_event_stream.call_args
        self.assertEqual(args[0], dataset.replay_events)
        self.assertEqual(kwargs["market_ticker"], "FED-23DEC-T3.00")

    def test_research_session_wraps_discovery_results(self):
        payload = {
            "category": "Climate and Weather",
            "series_ticker": "HIGHMIA",
            "series_title": "Highest temperature in Miami",
            "event_ticker": "HIGHMIA-20260315",
            "event_title": "Highest temperature in Miami on March 15",
            "event_subtitle": "Daily",
            "market_ticker": "HIGHMIA-20260315-B85",
            "market_title": "Will the high in Miami exceed 85F?",
            "market_subtitle": "Above 85F",
            "status": "open",
        }

        with patch("mykalshi.research.workflows.discovery.search_markets", return_value=[payload]), patch(
            "mykalshi.research.workflows.discovery.resolve_market",
            return_value=payload,
        ):
            session = ResearchSession()
            search_results = session.search_markets(series_ticker="HIGHMIA")
            resolved = session.resolve_market(series_ticker="HIGHMIA", market_ticker_contains="B85")

        self.assertEqual(search_results[0].market_ticker, "HIGHMIA-20260315-B85")
        self.assertEqual(resolved.summary()["event_ticker"], "HIGHMIA-20260315")

    def test_research_session_groups_market_universes_by_event(self):
        payloads = [
            {
                "category": "Technology",
                "series_ticker": "KXELONMARS",
                "series_title": "Elon Musk on Mars",
                "event_ticker": "KXELONMARS-99",
                "event_title": "Will Elon Musk visit Mars in his lifetime?",
                "event_subtitle": "Before 2099",
                "market_ticker": "KXELONMARS-99-YES",
                "market_title": "Yes before 2099",
                "market_subtitle": "Before 2099",
                "status": "open",
            },
            {
                "category": "Technology",
                "series_ticker": "KXELONMARS",
                "series_title": "Elon Musk on Mars",
                "event_ticker": "KXELONMARS-99",
                "event_title": "Will Elon Musk visit Mars in his lifetime?",
                "event_subtitle": "Before 2099",
                "market_ticker": "KXELONMARS-99-NO",
                "market_title": "No before 2099",
                "market_subtitle": "Before 2099",
                "status": "open",
            },
        ]

        with patch("mykalshi.research.workflows.discovery.search_markets", return_value=payloads):
            universes = ResearchSession().search_market_universes(query="elon mars")

        self.assertEqual(len(universes), 1)
        self.assertEqual(universes[0].event_ticker, "KXELONMARS-99")
        self.assertEqual([market.market_ticker for market in universes[0].markets], ["KXELONMARS-99-YES", "KXELONMARS-99-NO"])

    def test_research_session_runs_replay_backtest_end_to_end(self):
        result = ResearchSession().run_replay_backtest(
            BuyOnFirstOrderbookStrategy(),
            market_data_source=[ticker_event()],
            orderbook_source=[orderbook_snapshot_event()],
            market_ticker="FED-23DEC-T3.00",
            enrich_market_lifecycle=False,
            initial_cash_cents=100,
        )

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].price_cents, 45)
        self.assertEqual(str(result.final_equity_cents), "116.00")

    def test_research_session_runs_historical_backtest_via_wrapper(self):
        session = ResearchSession()
        strategy = object()
        expected_result = object()
        with patch.object(
            session,
            "_trade_backtester_factory",
            return_value=type(
                "StubBacktester",
                (),
                {"run_on_historical_trades": lambda self, ticker, strategy, **kwargs: (ticker, strategy, kwargs)},
            )(),
        ):
            result = session.run_historical_backtest("ARCHIVE-YES", strategy, initial_cash_cents=100)

        self.assertEqual(result[0], "ARCHIVE-YES")
        self.assertIs(result[1], strategy)
        self.assertEqual(result[2]["initial_cash_cents"], 100)

    def test_replay_dataset_exports_dataframes(self):
        dataset = ResearchSession().load_replay_dataset(
            market_data_source=[ticker_event()],
            orderbook_source=[orderbook_snapshot_event()],
            market_ticker="FED-23DEC-T3.00",
        )

        frames = dataset.to_dataframes()
        self.assertEqual(sorted(frames.keys()), ["market_data", "orderbook", "replay"])
        self.assertEqual(len(frames["market_data"]), 1)
        self.assertEqual(len(frames["orderbook"]), 1)
        self.assertEqual(len(frames["replay"]), 2)


if __name__ == "__main__":
    unittest.main()
