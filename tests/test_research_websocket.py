from __future__ import annotations

import asyncio
import unittest

from mykalshi.research.websocket import KalshiWebsocketClient


class WebsocketSyncWrapperTests(unittest.TestCase):
    def test_capture_market_data_sync_works_inside_running_loop(self):
        client = KalshiWebsocketClient()

        async def fake_capture_market_data(**kwargs):
            return [{"channel": "ticker", "market_ticker": "TEST-YES"}]

        client.capture_market_data = fake_capture_market_data  # type: ignore[assignment]

        async def run_inside_loop():
            return client.capture_market_data_sync(channels=["ticker"], authenticated=False)

        result = asyncio.run(run_inside_loop())
        self.assertEqual(result[0]["channel"], "ticker")
        self.assertEqual(result[0]["market_ticker"], "TEST-YES")


if __name__ == "__main__":
    unittest.main()
