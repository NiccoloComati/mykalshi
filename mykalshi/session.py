from __future__ import annotations

from .auth import KalshiAuthSigner
from .config import KalshiConfig
from .exceptions import KalshiAuthenticationError


_config = KalshiConfig.from_env()

ENV = _config.environment.value.upper()
KEY_ID = _config.api_key_id
KEY_FILE = _config.private_key_path
BASE_URL = _config.resolved_rest_base_url


def sign_request(method, full_path):
    if not KEY_ID or not KEY_FILE:
        raise KalshiAuthenticationError(
            "Kalshi credentials are not configured. Set KALSHI_API_KEY_ID and "
            "KALSHI_PRIVATE_KEY_PATH, or the legacy ENV/DEMO_KEY*/PROD_KEY* variables."
        )

    signer = KalshiAuthSigner(api_key_id=KEY_ID, private_key_path=KEY_FILE)
    headers = signer.sign_path(method, full_path)
    headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    )
    return headers
