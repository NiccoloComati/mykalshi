from __future__ import annotations

import unittest

from mykalshi.auth import normalize_signing_path


class NormalizeSigningPathTests(unittest.TestCase):
    def test_strips_query_from_relative_path(self):
        self.assertEqual(
            normalize_signing_path(
                "/portfolio/balance?limit=1",
                base_url="https://demo-api.kalshi.co/trade-api/v2",
            ),
            "/trade-api/v2/portfolio/balance",
        )

    def test_accepts_full_urls(self):
        self.assertEqual(
            normalize_signing_path(
                "https://api.elections.kalshi.com/trade-api/v2/markets?limit=5"
            ),
            "/trade-api/v2/markets",
        )


if __name__ == "__main__":
    unittest.main()
