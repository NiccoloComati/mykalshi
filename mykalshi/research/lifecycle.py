from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from .. import historical, market
from ..exceptions import KalshiHTTPError
from ..fixed_point import dollars_to_cents


TERMINAL_STATUSES = {"closed", "expired", "finalized", "resolved", "settled"}
OPEN_STATUSES = {"active", "initialized", "open"}


def _is_present(value: Any) -> bool:
    return value not in (None, "", [], {}, ())


def _parse_iso_timestamp(value: Any) -> datetime | None:
    if not _is_present(value):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_iso_timestamp(value: Any) -> str | None:
    parsed = _parse_iso_timestamp(value)
    if parsed is None:
        return None
    return parsed.isoformat().replace("+00:00", "Z")


def _safe_live_market_lookup(market_ticker: str) -> dict[str, Any] | None:
    try:
        response = market.get_market(market_ticker)
    except KalshiHTTPError as exc:
        if exc.status_code == 404:
            return None
        raise
    return response.get("market")


def _safe_historical_market_lookup(market_ticker: str) -> dict[str, Any] | None:
    try:
        response = historical.get_historical_markets(tickers=market_ticker, limit=10)
    except KalshiHTTPError as exc:
        if exc.status_code == 404:
            return None
        raise
    for item in response.get("markets", []):
        if str(item.get("ticker") or "") == market_ticker:
            return item
    return None


def _merge_market_metadata(primary: dict[str, Any] | None, secondary: dict[str, Any] | None) -> dict[str, Any] | None:
    if primary is None and secondary is None:
        return None
    merged: dict[str, Any] = {}
    for payload in (primary, secondary):
        if payload is None:
            continue
        for key, value in payload.items():
            if _is_present(value):
                merged[key] = value
            elif key not in merged:
                merged[key] = value
    return merged


def resolve_replay_market_metadata(market_ticker: str) -> dict[str, Any] | None:
    """Resolve market lifecycle metadata from live and historical sources.

    Historical metadata is merged in when available because it tends to carry
    finalized-result fields such as `settlement_ts` and `settlement_value_dollars`.
    """

    live_market = _safe_live_market_lookup(market_ticker)
    historical_market = _safe_historical_market_lookup(market_ticker)
    return _merge_market_metadata(live_market, historical_market)


def derive_binary_market_payouts(metadata: dict[str, Any]) -> tuple[int | None, int | None]:
    settlement_value = metadata.get("settlement_value_dollars")
    if _is_present(settlement_value):
        try:
            yes_payout_cents = dollars_to_cents(settlement_value)
        except Exception:
            yes_payout_cents = None
        if yes_payout_cents is not None and 0 <= yes_payout_cents <= 100:
            return yes_payout_cents, 100 - yes_payout_cents

    result = str(metadata.get("result") or metadata.get("expiration_value") or "").strip().casefold()
    if result == "yes":
        return 100, 0
    if result == "no":
        return 0, 100
    return None, None


def _choose_expiration_timestamp(metadata: dict[str, Any]) -> str | None:
    for key in ("latest_expiration_time", "expiration_time", "close_time"):
        timestamp = _format_iso_timestamp(metadata.get(key))
        if timestamp is not None:
            return timestamp
    return None


def _choose_settlement_timestamp(metadata: dict[str, Any]) -> str | None:
    for key in ("settlement_ts", "settled_time", "updated_time"):
        timestamp = _format_iso_timestamp(metadata.get(key))
        if timestamp is not None:
            return timestamp
    return _choose_expiration_timestamp(metadata)


def _status(metadata: dict[str, Any]) -> str:
    return str(metadata.get("status") or "").strip().casefold()


def _should_enrich_from_metadata(metadata: dict[str, Any], *, reference_time: datetime) -> bool:
    status = _status(metadata)
    if status in TERMINAL_STATUSES:
        return True
    yes_payout_cents, no_payout_cents = derive_binary_market_payouts(metadata)
    if yes_payout_cents is not None and no_payout_cents is not None:
        return True
    settlement_timestamp = _parse_iso_timestamp(_choose_settlement_timestamp(metadata))
    if settlement_timestamp is not None and settlement_timestamp <= reference_time:
        return True
    expiration_timestamp = _parse_iso_timestamp(_choose_expiration_timestamp(metadata))
    if expiration_timestamp is not None and expiration_timestamp <= reference_time and status not in OPEN_STATUSES:
        return True
    return False


def _event_sort_key(event: dict[str, Any]) -> tuple[str, str, int]:
    return (
        str(event.get("captured_at") or event.get("event_ts") or ""),
        str(event.get("market_ticker") or ""),
        int(event.get("sequence") or 0),
    )


def _synthetic_settlement_event(
    *,
    market_ticker: str,
    timestamp: str,
    sequence: int,
    yes_payout_cents: int | None,
    no_payout_cents: int | None,
    reason: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "captured_at": timestamp,
        "event_ts": timestamp,
        "event_type": "settlement",
        "channel": "settlement",
        "market_ticker": market_ticker,
        "sequence": sequence,
        "yes_payout_cents": yes_payout_cents,
        "no_payout_cents": no_payout_cents,
        "reason": reason,
        "raw_message": {
            "type": "synthetic_settlement",
            "reason": reason,
            "metadata": metadata,
        },
    }


def enrich_replay_events_with_market_lifecycle(
    replay_events: Iterable[dict[str, Any]],
    *,
    market_metadata: dict[str, dict[str, Any]] | None = None,
    metadata_resolver: Callable[[str], dict[str, Any] | None] | None = None,
    reference_time: datetime | str | None = None,
) -> list[dict[str, Any]]:
    """Append synthetic settlement/expiration events when replay data is incomplete."""

    ordered_events = sorted(list(replay_events), key=_event_sort_key)
    if not ordered_events:
        return ordered_events

    resolver = metadata_resolver or resolve_replay_market_metadata
    if isinstance(reference_time, str):
        normalized_reference_time = _parse_iso_timestamp(reference_time)
    else:
        normalized_reference_time = _parse_iso_timestamp(reference_time)
    if normalized_reference_time is None:
        normalized_reference_time = datetime.now(timezone.utc)

    events_by_market: dict[str, list[dict[str, Any]]] = {}
    for event in ordered_events:
        market_ticker = str(event.get("market_ticker") or "")
        if not market_ticker:
            continue
        events_by_market.setdefault(market_ticker, []).append(event)

    synthetic_events: list[dict[str, Any]] = []
    for market_ticker, market_events in events_by_market.items():
        actual_settlement_present = any(
            event.get("event_type") == "settlement"
            and event.get("yes_payout_cents") is not None
            and event.get("no_payout_cents") is not None
            for event in market_events
        )
        pending_settlement_present = any(event.get("event_type") == "settlement" for event in market_events)
        if actual_settlement_present:
            continue

        metadata = None
        if market_metadata is not None:
            metadata = market_metadata.get(market_ticker)
        if metadata is None:
            metadata = resolver(market_ticker)
        if metadata is None or not _should_enrich_from_metadata(metadata, reference_time=normalized_reference_time):
            continue

        yes_payout_cents, no_payout_cents = derive_binary_market_payouts(metadata)
        expiration_timestamp = _choose_expiration_timestamp(metadata)
        settlement_timestamp = _choose_settlement_timestamp(metadata)
        next_sequence = max((int(event.get("sequence") or 0) for event in market_events), default=0) + 1

        if (
            not pending_settlement_present
            and expiration_timestamp is not None
            and (
                yes_payout_cents is None
                or no_payout_cents is None
                or (
                    settlement_timestamp is not None
                    and _parse_iso_timestamp(settlement_timestamp) is not None
                    and _parse_iso_timestamp(expiration_timestamp) is not None
                    and _parse_iso_timestamp(settlement_timestamp) > _parse_iso_timestamp(expiration_timestamp)
                )
            )
        ):
            synthetic_events.append(
                _synthetic_settlement_event(
                    market_ticker=market_ticker,
                    timestamp=expiration_timestamp,
                    sequence=next_sequence,
                    yes_payout_cents=None,
                    no_payout_cents=None,
                    reason="synthetic_expiration",
                    metadata=metadata,
                )
            )
            next_sequence += 1

        if yes_payout_cents is not None and no_payout_cents is not None:
            actual_timestamp = settlement_timestamp or expiration_timestamp
            if actual_timestamp is not None:
                synthetic_events.append(
                    _synthetic_settlement_event(
                        market_ticker=market_ticker,
                        timestamp=actual_timestamp,
                        sequence=next_sequence,
                        yes_payout_cents=yes_payout_cents,
                        no_payout_cents=no_payout_cents,
                        reason="synthetic_settlement",
                        metadata=metadata,
                    )
                )

    return sorted([*ordered_events, *synthetic_events], key=_event_sort_key)
