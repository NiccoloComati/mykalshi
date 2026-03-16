from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .rate_limit import DEFAULT_READ_LIMIT_PER_SECOND, DEFAULT_WRITE_LIMIT_PER_SECOND


PRODUCTION_REST_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DEMO_REST_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
PRODUCTION_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
DEMO_WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"


def _load_dotenv_if_available() -> str | None:
    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:
        return None

    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path)
        return dotenv_path

    load_dotenv()
    return None


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _resolve_relative_path(path: str | None, *, dotenv_path: str | None = None) -> str | None:
    if not path:
        return None

    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return str(candidate)

    search_roots: list[Path] = []
    if dotenv_path:
        search_roots.append(Path(dotenv_path).resolve().parent)
    search_roots.append(Path.cwd())
    search_roots.append(Path(__file__).resolve().parent.parent)

    seen: set[Path] = set()
    for root in search_roots:
        resolved = (root / candidate).resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists():
            return str(resolved)

    return str(candidate)


class KalshiEnvironment(str, Enum):
    PRODUCTION = "production"
    DEMO = "demo"

    @classmethod
    def from_value(cls, value: str | None) -> "KalshiEnvironment":
        normalized = (value or "production").strip().lower()
        aliases = {
            "prod": cls.PRODUCTION,
            "production": cls.PRODUCTION,
            "live": cls.PRODUCTION,
            "demo": cls.DEMO,
            "sandbox": cls.DEMO,
        }
        try:
            return aliases[normalized]
        except KeyError as exc:
            raise ValueError(f"Unsupported Kalshi environment: {value}") from exc


@dataclass(frozen=True)
class KalshiConfig:
    environment: KalshiEnvironment = KalshiEnvironment.PRODUCTION
    api_key_id: str | None = None
    private_key_path: str | None = None
    rest_base_url: str | None = None
    ws_url: str | None = None
    timeout_seconds: float = 15.0
    user_agent: str = "mykalshi/0.2.0"
    enable_rate_limiting: bool = True
    auto_detect_account_limits: bool = True
    read_limit_per_second: float = DEFAULT_READ_LIMIT_PER_SECOND
    write_limit_per_second: float = DEFAULT_WRITE_LIMIT_PER_SECOND
    account_limits_cache_seconds: float = 300.0
    max_rate_limit_retries: int = 2

    @property
    def resolved_rest_base_url(self) -> str:
        if self.rest_base_url:
            return self.rest_base_url.rstrip("/")
        if self.environment is KalshiEnvironment.DEMO:
            return DEMO_REST_BASE_URL
        return PRODUCTION_REST_BASE_URL

    @property
    def resolved_ws_url(self) -> str:
        if self.ws_url:
            return self.ws_url.rstrip("/")
        if self.environment is KalshiEnvironment.DEMO:
            return DEMO_WS_URL
        return PRODUCTION_WS_URL

    def build_url(self, path: str) -> str:
        return f"{self.resolved_rest_base_url}/{path.lstrip('/')}"

    @classmethod
    def from_env(cls) -> "KalshiConfig":
        dotenv_path = _load_dotenv_if_available()

        environment = KalshiEnvironment.from_value(
            _first_env("KALSHI_ENV", "ENV")
        )

        if environment is KalshiEnvironment.DEMO:
            api_key_id = _first_env(
                "KALSHI_DEMO_API_KEY_ID",
                "DEMO_KEYID",
                "KALSHI_API_KEY_ID",
            )
            private_key_path = _first_env(
                "KALSHI_DEMO_PRIVATE_KEY_PATH",
                "DEMO_KEYFILE",
                "KALSHI_PRIVATE_KEY_PATH",
            )
        else:
            api_key_id = _first_env(
                "KALSHI_PROD_API_KEY_ID",
                "KALSHI_PRODUCTION_API_KEY_ID",
                "PROD_KEYID",
                "PRODUCTION_KEYID",
                "KALSHI_API_KEY_ID",
            )
            private_key_path = _first_env(
                "KALSHI_PROD_PRIVATE_KEY_PATH",
                "KALSHI_PRODUCTION_PRIVATE_KEY_PATH",
                "PROD_KEYFILE",
                "PRODUCTION_KEYFILE",
                "KALSHI_PRIVATE_KEY_PATH",
            )

        return cls(
            environment=environment,
            api_key_id=api_key_id,
            private_key_path=_resolve_relative_path(private_key_path, dotenv_path=dotenv_path),
            rest_base_url=_first_env("KALSHI_REST_BASE_URL"),
            ws_url=_first_env("KALSHI_WS_URL"),
            enable_rate_limiting=_bool_env("KALSHI_ENABLE_RATE_LIMITING", True),
            auto_detect_account_limits=_bool_env("KALSHI_AUTO_DETECT_ACCOUNT_LIMITS", True),
            read_limit_per_second=float(_first_env("KALSHI_READ_LIMIT_PER_SECOND") or DEFAULT_READ_LIMIT_PER_SECOND),
            write_limit_per_second=float(_first_env("KALSHI_WRITE_LIMIT_PER_SECOND") or DEFAULT_WRITE_LIMIT_PER_SECOND),
            account_limits_cache_seconds=float(
                _first_env("KALSHI_ACCOUNT_LIMITS_CACHE_SECONDS") or "300.0"
            ),
            max_rate_limit_retries=int(_first_env("KALSHI_MAX_RATE_LIMIT_RETRIES") or "2"),
        )
