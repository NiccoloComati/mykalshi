from __future__ import annotations

import unittest
from unittest.mock import patch

from mykalshi import historical, market, trading


class WrapperTests(unittest.TestCase):
    def test_batch_cancel_orders_passes_request_body(self):
        with patch("mykalshi.trading.kalshi_delete") as mocked_delete:
            trading.batch_cancel_orders(["order-1", "order-2"])
            mocked_delete.assert_called_once_with(
                "/portfolio/orders/batched",
                body={"ids": ["order-1", "order-2"]},
                authenticated=True,
            )

    def test_market_orderbook_is_authenticated(self):
        with patch("mykalshi.market.kalshi_get") as mocked_get:
            market.get_market_orderbook("TEST-TICKER")
            mocked_get.assert_called_once_with(
                "/markets/TEST-TICKER/orderbook",
                None,
                authenticated=True,
            )

    def test_historical_cutoff_uses_public_transport(self):
        with patch("mykalshi.historical.kalshi_get") as mocked_get:
            historical.get_historical_cutoff()
            mocked_get.assert_called_once_with("/historical/cutoff")


if __name__ == "__main__":
    unittest.main()
