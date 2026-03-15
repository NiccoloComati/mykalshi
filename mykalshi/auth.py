from __future__ import annotations

import base64
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from .exceptions import KalshiAuthenticationError, KalshiConfigurationError, KalshiDependencyError


def normalize_signing_path(path: str, base_url: str | None = None) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        sign_path = urlsplit(path).path
    elif base_url:
        sign_path = urlsplit(f"{base_url.rstrip('/')}/{path.lstrip('/')}").path
    else:
        sign_path = urlsplit(path).path

    if not sign_path.startswith("/"):
        sign_path = f"/{sign_path}"

    return sign_path


@dataclass
class KalshiAuthSigner:
    api_key_id: str
    private_key_path: str
    _private_key: Any = field(default=None, init=False, repr=False)

    def _load_private_key(self) -> Any:
        if self._private_key is not None:
            return self._private_key

        try:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
        except ImportError as exc:
            raise KalshiDependencyError(
                "cryptography is required for authenticated Kalshi requests"
            ) from exc

        try:
            with open(self.private_key_path, "rb") as handle:
                key_bytes = handle.read()
        except OSError as exc:
            raise KalshiConfigurationError(
                f"Unable to read Kalshi private key file: {self.private_key_path}"
            ) from exc

        self._private_key = serialization.load_pem_private_key(
            key_bytes,
            password=None,
            backend=default_backend(),
        )
        return self._private_key

    def sign_path(
        self,
        method: str,
        full_path: str,
        *,
        timestamp_ms: int | None = None,
    ) -> dict[str, str]:
        try:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding
        except ImportError as exc:
            raise KalshiDependencyError(
                "cryptography is required for authenticated Kalshi requests"
            ) from exc

        if not self.api_key_id:
            raise KalshiAuthenticationError("Missing Kalshi API key ID")

        timestamp = str(timestamp_ms or int(time.time() * 1000))
        signing_path = normalize_signing_path(full_path)
        message = f"{timestamp}{method.upper()}{signing_path}"
        private_key = self._load_private_key()
        signature = base64.b64encode(
            private_key.sign(
                message.encode("utf-8"),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
        ).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": signature,
        }

    def sign_headers(
        self,
        method: str,
        *,
        path: str,
        base_url: str | None = None,
        timestamp_ms: int | None = None,
    ) -> dict[str, str]:
        signing_path = normalize_signing_path(path, base_url=base_url)
        return self.sign_path(method, signing_path, timestamp_ms=timestamp_ms)
