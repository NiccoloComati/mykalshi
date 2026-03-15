from __future__ import annotations

from typing import Any

from .auth import KalshiAuthSigner
from .config import KalshiConfig
from .exceptions import (
    KalshiAuthenticationError,
    KalshiDependencyError,
    KalshiHTTPError,
)


class KalshiClient:
    def __init__(
        self,
        config: KalshiConfig | None = None,
        *,
        signer: KalshiAuthSigner | Any | None = None,
        session: Any | None = None,
    ) -> None:
        self.config = config or KalshiConfig.from_env()
        self._signer = signer
        self._session = session

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
                "requests is required to perform HTTP calls"
            ) from exc

        self._session = requests.Session()
        return self._session

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

        session = self._get_session()
        response = session.request(
            method=method.upper(),
            url=url,
            headers=request_headers,
            params=params,
            json=json_body,
            timeout=timeout or self.config.timeout_seconds,
        )

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
