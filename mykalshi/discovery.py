from __future__ import annotations

from typing import Any, Iterable

from . import events, market
from .transport import collect_cursor_pages


def _normalized(value: Any) -> str:
    return str(value or "").casefold()


def _contains(value: Any, needle: str | None) -> bool:
    if needle is None:
        return True
    return _normalized(needle) in _normalized(value)


def _contains_any(values: Iterable[Any], needle: str | None) -> bool:
    if needle is None:
        return True
    return any(_contains(value, needle) for value in values)


def _market_subtitle(item: dict[str, Any]) -> str:
    return str(
        item.get("subtitle")
        or item.get("yes_sub_title")
        or item.get("no_sub_title")
        or ""
    )


def _truncate(items: list[dict[str, Any]], limit: int | None) -> list[dict[str, Any]]:
    if limit is None:
        return items
    return items[:limit]


def _get_all_markets_for_event(
    event_ticker: str,
    *,
    status: str | None = None,
    batch_size: int = 200,
) -> list[dict[str, Any]]:
    return collect_cursor_pages(
        lambda cursor: market.get_markets(
            event_ticker=event_ticker,
            status=status,
            limit=batch_size,
            cursor=cursor,
        ),
        item_key="markets",
    )


def search_series(
    *,
    category: str | None = None,
    ticker: str | None = None,
    ticker_contains: str | None = None,
    title_contains: str | None = None,
    tag_contains: str | None = None,
    include_product_metadata: bool = False,
    limit: int | None = None,
    batch_size: int = 100,
) -> list[dict[str, Any]]:
    series_items = events.get_all_series(
        category=category,
        include_product_metadata=include_product_metadata,
        batch_size=batch_size,
    )
    results = []
    for item in series_items:
        if ticker is not None and item.get("ticker") != ticker:
            continue
        if not _contains(item.get("ticker"), ticker_contains):
            continue
        if not _contains(item.get("title"), title_contains):
            continue
        if not _contains_any(item.get("tags", []), tag_contains):
            continue
        results.append(item)
        if limit is not None and len(results) >= limit:
            break
    return results


def search_events(
    *,
    category: str | None = None,
    series_ticker: str | None = None,
    status: str | None = None,
    event_ticker: str | None = None,
    series_title_contains: str | None = None,
    event_title_contains: str | None = None,
    subtitle_contains: str | None = None,
    with_nested_markets: bool = False,
    limit: int | None = None,
    batch_size: int = 200,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    if (
        event_ticker is not None
        and category is None
        and series_ticker is None
        and series_title_contains is None
        and event_title_contains is None
        and subtitle_contains is None
    ):
        response = events.get_event(event_ticker, with_nested_markets=with_nested_markets)
        event_item = response.get("event", {})
        if status is not None and event_item.get("status") not in (None, status):
            return []
        return [event_item] if event_item else []

    if category is not None or series_ticker is not None or series_title_contains is not None:
        series_items = search_series(
            category=category,
            ticker=series_ticker,
            title_contains=series_title_contains,
            limit=None,
        )
        for series_item in series_items:
            event_items = events.get_all_events(
                status=status,
                series_ticker=series_item["ticker"],
                with_nested_markets=with_nested_markets,
                batch_size=batch_size,
            )
            for item in event_items:
                if event_ticker is not None and item.get("event_ticker") != event_ticker:
                    continue
                if not _contains(item.get("title"), event_title_contains):
                    continue
                if not _contains(item.get("sub_title"), subtitle_contains):
                    continue
                results.append(item)
                if limit is not None and len(results) >= limit:
                    return results
        return results

    event_items = events.get_all_events(
        status=status,
        series_ticker=series_ticker,
        with_nested_markets=with_nested_markets,
        batch_size=batch_size,
    )
    for item in event_items:
        if event_ticker is not None and item.get("event_ticker") != event_ticker:
            continue
        if not _contains(item.get("title"), event_title_contains):
            continue
        if not _contains(item.get("sub_title"), subtitle_contains):
            continue
        results.append(item)
        if limit is not None and len(results) >= limit:
            break
    return results


def search_markets(
    *,
    category: str | None = None,
    series_ticker: str | None = None,
    event_ticker: str | None = None,
    status: str | None = "open",
    series_title_contains: str | None = None,
    event_title_contains: str | None = None,
    market_title_contains: str | None = None,
    subtitle_contains: str | None = None,
    market_ticker_contains: str | None = None,
    limit: int | None = None,
    batch_size: int = 200,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    use_event_scope = any(
        value is not None
        for value in (
            category,
            series_ticker,
            event_ticker,
            series_title_contains,
            event_title_contains,
        )
    )

    if use_event_scope:
        event_items = search_events(
            category=category,
            series_ticker=series_ticker,
            status=status,
            event_ticker=event_ticker,
            series_title_contains=series_title_contains,
            event_title_contains=event_title_contains,
            limit=None,
            batch_size=batch_size,
        )
        series_map: dict[str, dict[str, Any]] = {}
        if category is not None or series_ticker is not None or series_title_contains is not None:
            for item in search_series(
                category=category,
                ticker=series_ticker,
                title_contains=series_title_contains,
                limit=None,
            ):
                series_map[item["ticker"]] = item

        for event_item in event_items:
            market_items = _get_all_markets_for_event(
                event_item["event_ticker"],
                status=status,
                batch_size=batch_size,
            )
            for market_item in market_items:
                if not _contains(market_item.get("ticker"), market_ticker_contains):
                    continue
                if not _contains(market_item.get("title"), market_title_contains):
                    continue
                if not _contains(_market_subtitle(market_item), subtitle_contains):
                    continue
                results.append(
                    {
                        "category": event_item.get("category"),
                        "series_ticker": event_item.get("series_ticker"),
                        "series_title": series_map.get(event_item.get("series_ticker"), {}).get("title"),
                        "event_ticker": event_item.get("event_ticker"),
                        "event_title": event_item.get("title"),
                        "event_subtitle": event_item.get("sub_title"),
                        "market_ticker": market_item.get("ticker"),
                        "market_title": market_item.get("title"),
                        "market_subtitle": _market_subtitle(market_item),
                        "status": market_item.get("status"),
                        "series": series_map.get(event_item.get("series_ticker")),
                        "event": event_item,
                        "market": market_item,
                    }
                )
                if limit is not None and len(results) >= limit:
                    return results
        return results

    market_items = market.get_all_markets(status=status, batch_size=batch_size)
    for market_item in market_items:
        if not _contains(market_item.get("ticker"), market_ticker_contains):
            continue
        if not _contains(market_item.get("title"), market_title_contains):
            continue
        if not _contains(_market_subtitle(market_item), subtitle_contains):
            continue
        results.append(
            {
                "category": None,
                "series_ticker": None,
                "series_title": None,
                "event_ticker": market_item.get("event_ticker"),
                "event_title": None,
                "event_subtitle": None,
                "market_ticker": market_item.get("ticker"),
                "market_title": market_item.get("title"),
                "market_subtitle": _market_subtitle(market_item),
                "status": market_item.get("status"),
                "series": None,
                "event": None,
                "market": market_item,
            }
        )
        if limit is not None and len(results) >= limit:
            break
    return results


def resolve_market(**kwargs: Any) -> dict[str, Any]:
    matches = search_markets(**kwargs)
    if not matches:
        raise LookupError("No Kalshi markets matched the supplied discovery filters.")
    if len(matches) > 1:
        raise LookupError(
            f"Discovery filters matched {len(matches)} markets. Narrow the filters and try again."
        )
    return matches[0]
