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


if __name__ == "__main__":
    unittest.main()
