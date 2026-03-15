from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from . import historical, market
from .exceptions import KalshiHTTPError


def _to_unix_timestamp(value: Any | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp())
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            return int(datetime.fromisoformat(normalized).timestamp())
        except ValueError:
            pass
        for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
            try:
                return int(datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp())
            except ValueError:
                continue
    raise ValueError(f"Unsupported timestamp value: {value!r}")


def _trade_sort_key(trade: dict[str, Any]) -> tuple[str, str]:
    return (
        str(trade.get("created_time") or trade.get("ts") or ""),
        str(trade.get("trade_id") or ""),
    )


def get_cutoff_timestamps() -> dict[str, int]:
    cutoff = historical.get_historical_cutoff()
    return {
        "market_settled_ts": _to_unix_timestamp(cutoff["market_settled_ts"]),
        "orders_updated_ts": _to_unix_timestamp(cutoff["orders_updated_ts"]),
        "trades_created_ts": _to_unix_timestamp(cutoff["trades_created_ts"]),
    }


def resolve_trade_source(
    ticker: str,
    *,
    start_ts: Any | None = None,
    end_ts: Any | None = None,
) -> dict[str, Any]:
    start_unix = _to_unix_timestamp(start_ts) if start_ts is not None else None
    end_unix = _to_unix_timestamp(end_ts) if end_ts is not None else None
    cutoff_ts = get_cutoff_timestamps()["trades_created_ts"]

    if start_unix is not None or end_unix is not None:
        use_historical = start_unix is None or start_unix < cutoff_ts
        use_live = end_unix is None or end_unix >= cutoff_ts
        return {
            "ticker": ticker,
            "cutoff_ts": cutoff_ts,
            "start_ts": start_unix,
            "end_ts": end_unix,
            "use_historical": use_historical,
            "use_live": use_live,
            "historical_range": {
                "min_ts": start_unix,
                "max_ts": end_unix if end_unix is not None and end_unix < cutoff_ts else cutoff_ts - 1,
            }
            if use_historical
            else None,
            "live_range": {
                "min_ts": start_unix if start_unix is not None and start_unix >= cutoff_ts else cutoff_ts,
                "max_ts": end_unix,
            }
            if use_live
            else None,
        }

    try:
        market_response = market.get_market(ticker)
        market_item = market_response.get("market", {})
        close_ts = _to_unix_timestamp(market_item.get("close_time")) if market_item.get("close_time") else None
        if close_ts is not None and close_ts < cutoff_ts:
            return {
                "ticker": ticker,
                "cutoff_ts": cutoff_ts,
                "start_ts": None,
                "end_ts": None,
                "use_historical": True,
                "use_live": False,
                "historical_range": {"min_ts": None, "max_ts": None},
                "live_range": None,
            }
        if str(market_item.get("status") or "").casefold() in {"finalized", "settled"}:
            try:
                historical_probe = historical.get_historical_trades(ticker=ticker, limit=1)
                if historical_probe.get("trades"):
                    return {
                        "ticker": ticker,
                        "cutoff_ts": cutoff_ts,
                        "start_ts": None,
                        "end_ts": None,
                        "use_historical": True,
                        "use_live": False,
                        "historical_range": {"min_ts": None, "max_ts": None},
                        "live_range": None,
                    }
            except KalshiHTTPError as exc:
                if exc.status_code != 404:
                    raise
        return {
            "ticker": ticker,
            "cutoff_ts": cutoff_ts,
            "start_ts": None,
            "end_ts": None,
            "use_historical": False,
            "use_live": True,
            "historical_range": None,
            "live_range": {"min_ts": None, "max_ts": None},
        }
    except KalshiHTTPError as exc:
        if exc.status_code != 404:
            raise

    return {
        "ticker": ticker,
        "cutoff_ts": cutoff_ts,
        "start_ts": None,
        "end_ts": None,
        "use_historical": True,
        "use_live": False,
        "historical_range": {"min_ts": None, "max_ts": None},
        "live_range": None,
    }


def get_trades_auto(
    ticker: str,
    *,
    start_ts: Any | None = None,
    end_ts: Any | None = None,
    historical_batch_size: int = 1000,
    live_batch_size: int = 100,
    calls_per_sec: int = 30,
) -> dict[str, Any]:
    route = resolve_trade_source(ticker, start_ts=start_ts, end_ts=end_ts)
    trades: list[dict[str, Any]] = []
    sources_used: list[str] = []

    if route["use_historical"] and route["historical_range"] is not None:
        historical_result = historical.get_all_historical_trades(
            ticker=ticker,
            min_ts=route["historical_range"]["min_ts"],
            max_ts=route["historical_range"]["max_ts"],
            batch_size=historical_batch_size,
        )
        trades.extend(historical_result.get("trades", []))
        sources_used.append("historical")

    if route["use_live"] and route["live_range"] is not None:
        live_result = market.get_all_trades(
            ticker=ticker,
            min_ts=route["live_range"]["min_ts"],
            max_ts=route["live_range"]["max_ts"],
            batch_size=live_batch_size,
            calls_per_sec=calls_per_sec,
        )
        trades.extend(live_result.get("trades", []))
        sources_used.append("live")

    ordered_trades = sorted(trades, key=_trade_sort_key)
    return {
        "ticker": ticker,
        "cutoff_ts": route["cutoff_ts"],
        "sources_used": sources_used,
        "trades": ordered_trades,
        "total_count": len(ordered_trades),
    }


def get_trades_dataframe_auto(
    ticker: str,
    *,
    start_ts: Any | None = None,
    end_ts: Any | None = None,
    historical_batch_size: int = 1000,
    live_batch_size: int = 100,
    calls_per_sec: int = 30,
):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for get_trades_dataframe_auto") from exc

    result = get_trades_auto(
        ticker,
        start_ts=start_ts,
        end_ts=end_ts,
        historical_batch_size=historical_batch_size,
        live_batch_size=live_batch_size,
        calls_per_sec=calls_per_sec,
    )
    dataframe = pd.DataFrame(result["trades"])
    if not dataframe.empty and "created_time" in dataframe.columns:
        dataframe["created_time"] = pd.to_datetime(dataframe["created_time"], utc=True)
        dataframe = dataframe.sort_values("created_time")
    return dataframe
