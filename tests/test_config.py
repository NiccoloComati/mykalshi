from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from mykalshi.config import (
    DEMO_REST_BASE_URL,
    PRODUCTION_REST_BASE_URL,
    KalshiConfig,
    KalshiEnvironment,
)


class KalshiConfigTests(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_defaults_to_production(self):
        config = KalshiConfig.from_env()
        self.assertEqual(config.environment, KalshiEnvironment.PRODUCTION)
        self.assertEqual(config.resolved_rest_base_url, PRODUCTION_REST_BASE_URL)

    @patch.dict(
        os.environ,
        {
            "ENV": "DEMO",
            "DEMO_KEYID": "demo-key",
            "DEMO_KEYFILE": "demo.pem",
        },
        clear=True,
    )
    def test_legacy_demo_env_variables_are_supported(self):
        config = KalshiConfig.from_env()
        self.assertEqual(config.environment, KalshiEnvironment.DEMO)
        self.assertEqual(config.api_key_id, "demo-key")
        self.assertEqual(config.private_key_path, "demo.pem")
        self.assertEqual(config.resolved_rest_base_url, DEMO_REST_BASE_URL)

    @patch.dict(
        os.environ,
        {
            "KALSHI_ENABLE_RATE_LIMITING": "false",
            "KALSHI_AUTO_DETECT_ACCOUNT_LIMITS": "no",
            "KALSHI_READ_LIMIT_PER_SECOND": "12.5",
            "KALSHI_WRITE_LIMIT_PER_SECOND": "7",
            "KALSHI_ACCOUNT_LIMITS_CACHE_SECONDS": "45",
            "KALSHI_MAX_RATE_LIMIT_RETRIES": "4",
        },
        clear=True,
    )
    def test_rate_limit_env_variables_are_supported(self):
        config = KalshiConfig.from_env()
        self.assertFalse(config.enable_rate_limiting)
        self.assertFalse(config.auto_detect_account_limits)
        self.assertEqual(config.read_limit_per_second, 12.5)
        self.assertEqual(config.write_limit_per_second, 7.0)
        self.assertEqual(config.account_limits_cache_seconds, 45.0)
        self.assertEqual(config.max_rate_limit_retries, 4)


if __name__ == "__main__":
    unittest.main()
