from __future__ import annotations


class KalshiError(Exception):
    """Base exception for the mykalshi package."""


class KalshiConfigurationError(KalshiError):
    """Raised when client configuration is invalid or incomplete."""


class KalshiDependencyError(KalshiError):
    """Raised when an optional runtime dependency is missing."""


class KalshiAuthenticationError(KalshiError):
    """Raised when an authenticated request cannot be signed."""


class KalshiHTTPError(KalshiError):
    """Raised when the Kalshi API returns a non-success response."""

    def __init__(self, status_code: int, method: str, url: str, body: str = "") -> None:
        message = f"{method.upper()} {url} failed with status {status_code}"
        if body:
            message = f"{message}: {body}"
        super().__init__(message)
        self.status_code = status_code
        self.method = method.upper()
        self.url = url
        self.body = body
