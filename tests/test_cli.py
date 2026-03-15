from __future__ import annotations

import io
import json
import unittest
from unittest.mock import patch

from mykalshi.cli import run
from mykalshi.research.workflows import DiscoveredMarket


class _SummaryResult:
    def __init__(self, payload):
        self._payload = payload

    def summary(self):
        return self._payload


class CliTests(unittest.TestCase):
    def _run(self, argv):
        stdout = io.StringIO()
        stderr = io.StringIO()
        code = run(argv, stdout=stdout, stderr=stderr)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_discover_markets_command_emits_json(self):
        market = DiscoveredMarket(
            category="tech",
            series_ticker="SERIES",
            series_title="Series",
            event_ticker="EVENT",
            event_title="Event",
            event_subtitle=None,
            market_ticker="MARKET",
            market_title="Market",
            market_subtitle=None,
            status="open",
        )
        with patch("mykalshi.cli.ResearchSession.search_markets", return_value=[market]):
            code, stdout, stderr = self._run(["discover", "markets", "--query", "mars"])

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload[0]["market_ticker"], "MARKET")

    def test_replay_summary_command_uses_session_dataset_loader(self):
        with patch(
            "mykalshi.cli.ResearchSession.load_replay_dataset",
            return_value=_SummaryResult({"market_ticker": "MARKET", "replay_event_count": 3}),
        ), patch(
            "mykalshi.cli.ResearchSession.open_capture_session",
            return_value=_SummaryResult({"directory": "capture", "market_ticker": "MARKET"}),
        ):
            code, stdout, stderr = self._run(
                [
                    "replay",
                    "summary",
                    "--session-dir",
                    "capture",
                ]
            )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["replay_event_count"], 3)
        self.assertEqual(payload["session"]["market_ticker"], "MARKET")

    def test_backtest_replay_command_loads_strategy_and_runs(self):
        with patch(
            "mykalshi.cli.ResearchSession.run_replay_backtest",
            return_value=_SummaryResult({"fill_count": 1, "final_equity_cents": "100.00"}),
        ) as mocked_run:
            code, stdout, stderr = self._run(
                [
                    "backtest",
                    "replay",
                    "--session-dir",
                    "capture",
                    "--strategy",
                    "tests.cli_fixtures:DemoReplayStrategy",
                ]
            )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["fill_count"], 1)
        strategy = mocked_run.call_args.args[0]
        self.assertEqual(type(strategy).__name__, "DemoReplayStrategy")

    def test_capture_session_command_uses_research_session(self):
        with patch(
            "mykalshi.cli.ResearchSession.capture_market_session",
            return_value=_SummaryResult({"directory": "capture", "market_ticker": "MARKET"}),
        ) as mocked_capture:
            code, stdout, stderr = self._run(
                [
                    "capture",
                    "session",
                    "capture",
                    "--market-ticker",
                    "MARKET",
                    "--channels",
                    "ticker",
                    "orderbook_delta",
                ]
            )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["directory"], "capture")
        self.assertEqual(mocked_capture.call_args.kwargs["market_ticker"], "MARKET")
        self.assertEqual(mocked_capture.call_args.kwargs["channels"], ["ticker", "orderbook_delta"])

    def test_capture_session_command_can_use_discovery_filters(self):
        with patch(
            "mykalshi.cli.ResearchSession.capture_market_session",
            return_value=_SummaryResult({"directory": "capture", "market_ticker": "MARKET"}),
        ) as mocked_capture:
            code, stdout, stderr = self._run(
                [
                    "capture",
                    "session",
                    "capture",
                    "--query",
                    "miami",
                    "--status",
                    "open",
                ]
            )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(json.loads(stdout)["market_ticker"], "MARKET")
        self.assertEqual(mocked_capture.call_args.kwargs["query"], "miami")
        self.assertEqual(mocked_capture.call_args.kwargs["status"], "open")

    def test_trading_plan_order_defaults_to_dry_run(self):
        with patch(
            "mykalshi.cli.TradingSession.submit_order",
            return_value=_SummaryResult({"dry_run": True, "operation": "submit_order"}),
        ) as mocked_submit:
            code, stdout, stderr = self._run(
                [
                    "trading",
                    "plan-order",
                    "MARKET",
                    "--action",
                    "buy",
                    "--side",
                    "yes",
                    "--quantity",
                    "1",
                    "--limit-price-cents",
                    "40",
                ]
            )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertTrue(payload["dry_run"])
        intent = mocked_submit.call_args.args[0]
        self.assertEqual(intent.to_payload()["yes_price"], 40)

    def test_capture_market_data_command_summarizes_events(self):
        with patch(
            "mykalshi.cli.KalshiWebsocketClient.capture_market_data_sync",
            return_value=[
                {"channel": "ticker", "market_ticker": "MARKET"},
                {"channel": "trade", "market_ticker": "MARKET"},
            ],
        ):
            code, stdout, stderr = self._run(
                [
                    "capture",
                    "market-data",
                    "--channels",
                    "ticker",
                    "trade",
                    "--market-ticker",
                    "MARKET",
                    "--max-events",
                    "2",
                ]
            )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["event_count"], 2)
        self.assertEqual(payload["channels"], {"ticker": 1, "trade": 1})

    def test_invalid_strategy_import_returns_error(self):
        code, stdout, stderr = self._run(
            [
                "backtest",
                "historical",
                "MARKET",
                "--strategy",
                "missing.module:Strategy",
            ]
        )

        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("ModuleNotFoundError", stderr)


if __name__ == "__main__":
    unittest.main()
