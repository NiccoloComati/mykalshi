from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .. import discovery, historical
from ..exceptions import KalshiDependencyError


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("pandas is required for market universe snapshots") from exc
    return pd


def _serialize(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


@dataclass(frozen=True)
class UniverseSpec:
    category: str | None = None
    query: str | None = None
    series_ticker: str | None = None
    event_ticker: str | None = None
    status: str | None = None
    series_title_contains: str | None = None
    event_title_contains: str | None = None
    market_title_contains: str | None = None
    subtitle_contains: str | None = None
    market_ticker_contains: str | None = None
    tickers: tuple[str, ...] = ()
    include_historical: bool = False
    limit: int | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "query": self.query,
            "series_ticker": self.series_ticker,
            "event_ticker": self.event_ticker,
            "status": self.status,
            "series_title_contains": self.series_title_contains,
            "event_title_contains": self.event_title_contains,
            "market_title_contains": self.market_title_contains,
            "subtitle_contains": self.subtitle_contains,
            "market_ticker_contains": self.market_ticker_contains,
            "tickers": list(self.tickers),
            "include_historical": self.include_historical,
            "limit": self.limit,
        }

    def stable_key(self) -> str:
        encoded = json.dumps(self.summary(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


@dataclass
class MarketUniverseSnapshot:
    directory: Path
    spec: UniverseSpec
    manifest: dict[str, Any]
    markets: list[dict[str, Any]] = field(default_factory=list)

    @property
    def manifest_path(self) -> Path:
        return self.directory / "manifest.json"

    @property
    def markets_path(self) -> Path:
        return self.directory / "markets.jsonl"

    def summary(self) -> dict[str, Any]:
        return {
            "directory": str(self.directory),
            "market_count": len(self.markets),
            "spec": self.spec.summary(),
            "captured_at": self.manifest.get("captured_at"),
        }

    def to_dataframe(self) -> Any:
        pd = _require_pandas()
        return pd.json_normalize(self.markets)


def _dedupe_markets(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for market_item in markets:
        ticker = market_item.get("market_ticker") or market_item.get("ticker")
        if ticker is None:
            continue
        seen[str(ticker)] = dict(market_item)
    return list(seen.values())


def resolve_market_universe(spec: UniverseSpec) -> list[dict[str, Any]]:
    live_matches = discovery.search_markets(
        category=spec.category,
        query=spec.query,
        series_ticker=spec.series_ticker,
        event_ticker=spec.event_ticker,
        status=spec.status,
        series_title_contains=spec.series_title_contains,
        event_title_contains=spec.event_title_contains,
        market_title_contains=spec.market_title_contains,
        subtitle_contains=spec.subtitle_contains,
        market_ticker_contains=spec.market_ticker_contains,
        limit=spec.limit,
    )
    matches = list(live_matches)

    if spec.include_historical:
        historical_matches: list[dict[str, Any]] = []
        tickers = ",".join(spec.tickers) if spec.tickers else None
        if spec.event_ticker:
            historical_matches = historical.get_all_historical_markets(event_ticker=spec.event_ticker)
        elif tickers:
            historical_matches = historical.get_all_historical_markets(tickers=tickers)

        for item in historical_matches:
            matches.append(
                {
                    "category": None,
                    "series_ticker": None,
                    "series_title": None,
                    "event_ticker": item.get("event_ticker"),
                    "event_title": None,
                    "event_subtitle": None,
                    "market_ticker": item.get("ticker"),
                    "market_title": item.get("title"),
                    "market_subtitle": item.get("subtitle") or item.get("yes_sub_title"),
                    "status": item.get("status"),
                    "market": item,
                }
            )

    if spec.tickers:
        ticker_set = {ticker for ticker in spec.tickers}
        matches = [item for item in matches if str(item.get("market_ticker") or item.get("ticker")) in ticker_set]
    if spec.limit is not None:
        matches = matches[: spec.limit]
    return _dedupe_markets(matches)


def sync_market_universe(
    directory: str | Path,
    spec: UniverseSpec,
    *,
    overwrite: bool = True,
) -> MarketUniverseSnapshot:
    universe_dir = Path(directory)
    if universe_dir.exists():
        if not overwrite:
            raise FileExistsError(f"Market universe directory already exists: {universe_dir}")
    universe_dir.mkdir(parents=True, exist_ok=True)

    markets = resolve_market_universe(spec)
    manifest = {
        "schema_version": 1,
        "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "spec": spec.summary(),
        "stable_key": spec.stable_key(),
        "market_count": len(markets),
    }
    (universe_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=_serialize) + "\n", encoding="utf-8")
    with (universe_dir / "markets.jsonl").open("w", encoding="utf-8") as handle:
        for item in markets:
            handle.write(json.dumps(item, default=_serialize) + "\n")
    return MarketUniverseSnapshot(directory=universe_dir, spec=spec, manifest=manifest, markets=markets)


def open_market_universe(directory: str | Path) -> MarketUniverseSnapshot:
    universe_dir = Path(directory)
    manifest = json.loads((universe_dir / "manifest.json").read_text(encoding="utf-8"))
    spec = UniverseSpec(**manifest.get("spec", {}))
    markets = []
    markets_path = universe_dir / "markets.jsonl"
    if markets_path.exists():
        markets = [json.loads(line) for line in markets_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return MarketUniverseSnapshot(directory=universe_dir, spec=spec, manifest=manifest, markets=markets)
