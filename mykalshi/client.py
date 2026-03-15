from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from .auth import KalshiAuthSigner
from .config import KalshiConfig
from .exceptions import (
    KalshiAuthenticationError,
    KalshiDependencyError,
    KalshiHTTPError,
)
from .rate_limit import KalshiRateLimiter


def _parse_retry_after_seconds(value: str | None, *, now: datetime | None = None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_after_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None

    if retry_after_at.tzinfo is None:
        retry_after_at = retry_after_at.replace(tzinfo=timezone.utc)
    current_time = now or datetime.now(timezone.utc)
    return max(0.0, (retry_after_at - current_time).total_seconds())


class KalshiClient:
    def __init__(
        self,
        config: KalshiConfig | None = None,
        *,
        signer: KalshiAuthSigner | Any | None = None,
        session: Any | None = None,
        rate_limiter: KalshiRateLimiter | None = None,
        clock: Any | None = None,
        sleeper: Any | None = None,
    ) -> None:
        self.config = config or KalshiConfig.from_env()
        self._signer = signer
        self._session = session
        self._clock = clock or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._rate_limiter = rate_limiter
        self._limits_lock = threading.Lock()
        self._account_limits_refreshed_at: float | None = None

    @property
    def signer(self) -> KalshiAuthSigner | Any | None:
        if self._signer is not None:
            return self._signer

        if self.config.api_key_id and self.config.private_key_path:
            self._signer = KalshiAuthSigner(
                api_key_id=self.config.api_key_id,
                private_key_path=self.config.private_key_path,
            )
            return self._signer

        return None

    def _get_session(self) -> Any:
        if self._session is not None:
            return self._session

        try:
            import requests
        except ImportError as exc:
            raise KalshiDependencyError(
                "requests is required to perform HTTP calls. Use the repo virtual "
                "environment or install dependencies with "
                "`pip install -e .[analysis,storage,websocket]`."
            ) from exc

        self._session = requests.Session()
        return self._session

    @property
    def rate_limiter(self) -> KalshiRateLimiter | None:
        if not self.config.enable_rate_limiting:
            return None
        if self._rate_limiter is None:
            self._rate_limiter = KalshiRateLimiter(
                read_limit_per_second=self.config.read_limit_per_second,
                write_limit_per_second=self.config.write_limit_per_second,
                clock=self._clock,
                sleeper=self._sleeper,
            )
        return self._rate_limiter

    @staticmethod
    def _rate_limit_bucket(method: str) -> str:
        return "read" if method.upper() == "GET" else "write"

    @staticmethod
    def _rate_limit_weight(method: str, path: str, json_body: dict[str, Any] | None = None) -> float:
        normalized_method = method.upper()
        normalized_path = path.split("?", 1)[0]
        if normalized_method == "GET":
            return 1.0

        if normalized_path == "/portfolio/orders/batched":
            if normalized_method == "POST":
                orders = (json_body or {}).get("orders") or []
                return float(len(orders) or 1)
            if normalized_method == "DELETE":
                ids = (json_body or {}).get("ids") or []
                return max(0.2, 0.2 * float(len(ids) or 0))
        return 1.0

    def _request_once(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Any:
        session = self._get_session()
        return session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout or self.config.timeout_seconds,
        )

    def _refresh_account_limits(self) -> None:
        if not self.config.enable_rate_limiting or not self.config.auto_detect_account_limits:
            return

        signer = self.signer
        if signer is None:
            return

        request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self.config.user_agent,
        }
        request_headers.update(
            signer.sign_headers(
                "GET",
                path="/account/limits",
                base_url=self.config.resolved_rest_base_url,
            )
        )
        response = self._request_once(
            "GET",
            self.config.build_url("/account/limits"),
            headers=request_headers,
        )
        if getattr(response, "status_code", 0) >= 400:
            return
        content = getattr(response, "content", b"")
        if not content:
            return
        payload = response.json()
        read_limit = payload.get("read_limit")
        write_limit = payload.get("write_limit")
        limiter = self.rate_limiter
        if limiter is None:
            return
        updates = {}
        if isinstance(read_limit, (int, float)) and read_limit > 0:
            updates["read_limit_per_second"] = float(read_limit)
        if isinstance(write_limit, (int, float)) and write_limit > 0:
            updates["write_limit_per_second"] = float(write_limit)
        if updates:
            limiter.update_limits(**updates)

    def _ensure_account_limits(self, *, authenticated: bool) -> None:
        if not authenticated or not self.config.enable_rate_limiting or not self.config.auto_detect_account_limits:
            return

        now = self._clock()
        with self._limits_lock:
            refreshed_at = self._account_limits_refreshed_at
            if refreshed_at is not None and (now - refreshed_at) < self.config.account_limits_cache_seconds:
                return
            self._account_limits_refreshed_at = now

        try:
            self._refresh_account_limits()
        except Exception:
            return

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        authenticated: bool = False,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        return_status_on_empty: bool = False,
    ) -> Any:
        url = self.config.build_url(path)
        request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self.config.user_agent,
        }
        if headers:
            request_headers.update(headers)

        if authenticated:
            signer = self.signer
            if signer is None:
                raise KalshiAuthenticationError(
                    "This request requires Kalshi credentials, but none were configured."
                )
            request_headers.update(
                signer.sign_headers(
                    method,
                    path=path,
                    base_url=self.config.resolved_rest_base_url,
                )
            )

        limiter = self.rate_limiter
        self._ensure_account_limits(authenticated=authenticated)
        bucket = self._rate_limit_bucket(method)
        weight = self._rate_limit_weight(method, path, json_body)

        response = None
        retries_remaining = max(0, int(self.config.max_rate_limit_retries))
        while True:
            if limiter is not None:
                limiter.acquire(bucket, weight=weight)
            response = self._request_once(
                method,
                url,
                headers=request_headers,
                params=params,
                json_body=json_body,
                timeout=timeout,
            )
            if getattr(response, "status_code", 0) != 429 or retries_remaining <= 0:
                break
            retry_after_seconds = _parse_retry_after_seconds(getattr(response, "headers", {}).get("Retry-After"))
            fallback_limit = self.config.read_limit_per_second if bucket == "read" else self.config.write_limit_per_second
            fallback_sleep_seconds = max(float(weight) / max(float(fallback_limit), 0.001), 0.25)
            self._sleeper(retry_after_seconds if retry_after_seconds is not None else fallback_sleep_seconds)
            retries_remaining -= 1

        if response.status_code >= 400:
            body = getattr(response, "text", "")
            raise KalshiHTTPError(response.status_code, method, url, body)

        content = getattr(response, "content", b"")
        if content:
            return response.json()
        if return_status_on_empty:
            return getattr(response, "status_code", 200)
        return {}

    def get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        authenticated: bool = False,
    ) -> Any:
        return self.request(
            "GET",
            path,
            params=params,
            authenticated=authenticated,
        )

    def post(
        self,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        authenticated: bool = False,
    ) -> Any:
        return self.request(
            "POST",
            path,
            json_body=json_body,
            authenticated=authenticated,
        )

    def put(
        self,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        authenticated: bool = False,
    ) -> Any:
        return self.request(
            "PUT",
            path,
            json_body=json_body,
            authenticated=authenticated,
        )

    def delete(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        authenticated: bool = False,
    ) -> Any:
        return self.request(
            "DELETE",
            path,
            params=params,
            json_body=json_body,
            authenticated=authenticated,
            return_status_on_empty=True,
        )


_default_client: KalshiClient | None = None


def get_default_client() -> KalshiClient:
    global _default_client
    if _default_client is None:
        _default_client = KalshiClient()
    return _default_client


def set_default_client(client: KalshiClient) -> None:
    global _default_client
    _default_client = client


def reset_default_client() -> None:
    global _default_client
    _default_client = None
