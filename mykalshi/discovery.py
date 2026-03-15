from __future__ import annotations

from typing import Any, Callable, Iterable

from . import events, market


MARKET_STATUS_FILTER_ALIASES = {
    "active": "open",
    "all": None,
    "initialized": "unopened",
    "inactive": "unopened",
    "determined": "settled",
    "disputed": "settled",
    "amended": "settled",
    "finalized": "settled",
}


def _normalized(value: Any) -> str:
    return str(value or "").casefold()


def _query_tokens(query: str | None) -> list[str]:
    return [token for token in _normalized(query).split() if token]


def _contains(value: Any, needle: str | None) -> bool:
    if needle is None:
        return True
    return _normalized(needle) in _normalized(value)


def _contains_any(values: Iterable[Any], needle: str | None) -> bool:
    if needle is None:
        return True
    return any(_contains(value, needle) for value in values)


def _matches_query(query: str | None, *values: Any) -> bool:
    tokens = _query_tokens(query)
    if not tokens:
        return True
    haystack = " ".join(_normalized(value) for value in values if value is not None)
    return all(token in haystack for token in tokens)


def _market_subtitle(item: dict[str, Any]) -> str:
    return str(item.get("subtitle") or item.get("yes_sub_title") or item.get("no_sub_title") or "")


def _normalize_market_status_filter(status: str | None) -> str | None:
    if status is None:
        return None
    return MARKET_STATUS_FILTER_ALIASES.get(_normalized(status), status)


def _collect_filtered_pages(
    fetch_page: Callable[[str | None], dict[str, Any]],
    *,
    item_key: str,
    predicate: Callable[[dict[str, Any]], bool],
    limit: int | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    cursor = None
    while True:
        response = fetch_page(cursor)
        for item in response.get(item_key, []):
            if not predicate(item):
                continue
            results.append(item)
            if limit is not None and len(results) >= limit:
                return results
        cursor = response.get("cursor")
        if not cursor:
            break
    return results


def _series_matches_query(item: dict[str, Any], query: str | None) -> bool:
    return _matches_query(
        query,
        item.get("ticker"),
        item.get("title"),
        item.get("category"),
        *(item.get("tags") or []),
    )


def _event_matches_query(
    item: dict[str, Any],
    query: str | None,
    *,
    series_item: dict[str, Any] | None = None,
) -> bool:
    return _matches_query(
        query,
        item.get("event_ticker"),
        item.get("title"),
        item.get("sub_title"),
        item.get("category"),
        item.get("series_ticker"),
        series_item.get("ticker") if series_item else None,
        series_item.get("title") if series_item else None,
    )


def _market_matches_query(
    market_item: dict[str, Any],
    query: str | None,
    *,
    event_item: dict[str, Any] | None = None,
    series_item: dict[str, Any] | None = None,
) -> bool:
    return _matches_query(
        query,
        market_item.get("ticker"),
        market_item.get("title"),
        _market_subtitle(market_item),
        market_item.get("status"),
        event_item.get("event_ticker") if event_item else None,
        event_item.get("title") if event_item else None,
        event_item.get("sub_title") if event_item else None,
        event_item.get("category") if event_item else None,
        series_item.get("ticker") if series_item else None,
        series_item.get("title") if series_item else None,
        series_item.get("category") if series_item else None,
    )


def search_series(
    *,
    category: str | None = None,
    ticker: str | None = None,
    ticker_contains: str | None = None,
    title_contains: str | None = None,
    tag_contains: str | None = None,
    query: str | None = None,
    include_product_metadata: bool = False,
    limit: int | None = None,
    batch_size: int = 100,
) -> list[dict[str, Any]]:
    if (
        ticker is not None
        and ticker_contains is None
        and title_contains is None
        and tag_contains is None
        and query is None
    ):
        response = events.get_series(ticker)
        item = response.get("series", {})
        if item and (category is None or item.get("category") == category):
            return [item]
        return []

    return _collect_filtered_pages(
        lambda cursor: events.get_series_list(
            category=category,
            include_product_metadata=include_product_metadata,
            limit=batch_size,
            cursor=cursor,
        ),
        item_key="series",
        predicate=lambda item: (
            (ticker is None or item.get("ticker") == ticker)
            and _contains(item.get("ticker"), ticker_contains)
            and _contains(item.get("title"), title_contains)
            and _contains_any(item.get("tags", []), tag_contains)
            and _series_matches_query(item, query)
        ),
        limit=limit,
    )


def resolve_series(**kwargs: Any) -> dict[str, Any]:
    matches = search_series(**kwargs)
    if not matches:
        raise LookupError("No Kalshi series matched the supplied discovery filters.")
    if len(matches) > 1:
        raise LookupError(
            f"Discovery filters matched {len(matches)} series. Narrow the filters and try again."
        )
    return matches[0]


def search_events(
    *,
    category: str | None = None,
    series_ticker: str | None = None,
    status: str | None = None,
    event_ticker: str | None = None,
    series_title_contains: str | None = None,
    event_title_contains: str | None = None,
    subtitle_contains: str | None = None,
    query: str | None = None,
    with_nested_markets: bool = False,
    limit: int | None = None,
    batch_size: int = 200,
) -> list[dict[str, Any]]:
    if (
        event_ticker is not None
        and category is None
        and series_ticker is None
        and series_title_contains is None
        and event_title_contains is None
        and subtitle_contains is None
        and query is None
    ):
        response = events.get_event(event_ticker, with_nested_markets=with_nested_markets)
        event_item = response.get("event", {})
        if status is not None and event_item.get("status") not in (None, status):
            return []
        return [event_item] if event_item else []

    if category is not None or series_ticker is not None or series_title_contains is not None:
        results: list[dict[str, Any]] = []
        series_items = search_series(
            category=category,
            ticker=series_ticker,
            title_contains=series_title_contains,
            query=None,
            limit=None,
            batch_size=batch_size,
        )
        for series_item in series_items:
            remaining = None if limit is None else max(limit - len(results), 0)
            if remaining == 0:
                break
            results.extend(
                _collect_filtered_pages(
                    lambda cursor: events.get_events(
                        status=status,
                        series_ticker=series_item["ticker"],
                        with_nested_markets=with_nested_markets,
                        limit=batch_size,
                        cursor=cursor,
                    ),
                    item_key="events",
                    predicate=lambda item: (
                        (event_ticker is None or item.get("event_ticker") == event_ticker)
                        and _contains(item.get("title"), event_title_contains)
                        and _contains(item.get("sub_title"), subtitle_contains)
                        and _event_matches_query(item, query, series_item=series_item)
                    ),
                    limit=remaining,
                )
            )
            if limit is not None and len(results) >= limit:
                return results[:limit]
        return results

    return _collect_filtered_pages(
        lambda cursor: events.get_events(
            status=status,
            series_ticker=series_ticker,
            with_nested_markets=with_nested_markets,
            limit=batch_size,
            cursor=cursor,
        ),
        item_key="events",
        predicate=lambda item: (
            (event_ticker is None or item.get("event_ticker") == event_ticker)
            and _contains(item.get("title"), event_title_contains)
            and _contains(item.get("sub_title"), subtitle_contains)
            and _event_matches_query(item, query)
        ),
        limit=limit,
    )


def resolve_event(**kwargs: Any) -> dict[str, Any]:
    matches = search_events(**kwargs)
    if not matches:
        raise LookupError("No Kalshi events matched the supplied discovery filters.")
    if len(matches) > 1:
        raise LookupError(
            f"Discovery filters matched {len(matches)} events. Narrow the filters and try again."
        )
    return matches[0]


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
    query: str | None = None,
    limit: int | None = None,
    batch_size: int = 200,
) -> list[dict[str, Any]]:
    normalized_status = _normalize_market_status_filter(status)
    use_event_scope = any(
        value is not None
        for value in (
            category,
            series_ticker,
            event_ticker,
            series_title_contains,
            event_title_contains,
        )
    ) or query is not None

    if use_event_scope:
        results: list[dict[str, Any]] = []
        event_items = search_events(
            category=category,
            series_ticker=series_ticker,
            status=normalized_status,
            event_ticker=event_ticker,
            series_title_contains=series_title_contains,
            event_title_contains=event_title_contains,
            query=query,
            limit=None,
            batch_size=batch_size,
        )
        series_map: dict[str, dict[str, Any]] = {}
        series_filters_active = any(value is not None for value in (category, series_ticker, series_title_contains))
        if series_filters_active:
            for item in search_series(
                category=category,
                ticker=series_ticker,
                title_contains=series_title_contains,
                query=None,
                limit=None,
                batch_size=batch_size,
            ):
                series_map[item["ticker"]] = item
        elif query is not None:
            for event_item in event_items:
                event_series_ticker = event_item.get("series_ticker")
                if not event_series_ticker or event_series_ticker in series_map:
                    continue
                response = events.get_series(event_series_ticker)
                series_item = response.get("series", {})
                if series_item:
                    series_map[event_series_ticker] = series_item

        for event_item in event_items:
            remaining = None if limit is None else max(limit - len(results), 0)
            if remaining == 0:
                break
            series_item = series_map.get(event_item.get("series_ticker"))
            market_items = _collect_filtered_pages(
                lambda cursor: market.get_markets(
                    event_ticker=event_item["event_ticker"],
                    status=normalized_status,
                    limit=batch_size,
                    cursor=cursor,
                ),
                item_key="markets",
                predicate=lambda market_item: (
                    _contains(market_item.get("ticker"), market_ticker_contains)
                    and _contains(market_item.get("title"), market_title_contains)
                    and _contains(_market_subtitle(market_item), subtitle_contains)
                    and _market_matches_query(
                        market_item,
                        query,
                        event_item=event_item,
                        series_item=series_item,
                    )
                ),
                limit=remaining,
            )
            for market_item in market_items:
                results.append(
                    {
                        "category": event_item.get("category"),
                        "series_ticker": event_item.get("series_ticker"),
                        "series_title": series_item.get("title") if series_item else None,
                        "event_ticker": event_item.get("event_ticker"),
                        "event_title": event_item.get("title"),
                        "event_subtitle": event_item.get("sub_title"),
                        "market_ticker": market_item.get("ticker"),
                        "market_title": market_item.get("title"),
                        "market_subtitle": _market_subtitle(market_item),
                        "status": market_item.get("status"),
                        "series": series_item,
                        "event": event_item,
                        "market": market_item,
                    }
                )
                if limit is not None and len(results) >= limit:
                    return results
        return results

    return [
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
        for market_item in _collect_filtered_pages(
            lambda cursor: market.get_markets(
                status=normalized_status,
                limit=batch_size,
                cursor=cursor,
            ),
            item_key="markets",
            predicate=lambda market_item: (
                _contains(market_item.get("ticker"), market_ticker_contains)
                and _contains(market_item.get("title"), market_title_contains)
                and _contains(_market_subtitle(market_item), subtitle_contains)
                and _market_matches_query(market_item, query)
            ),
            limit=limit,
        )
    ]


def resolve_market(**kwargs: Any) -> dict[str, Any]:
    matches = search_markets(**kwargs)
    if not matches:
        raise LookupError("No Kalshi markets matched the supplied discovery filters.")
    if len(matches) > 1:
        raise LookupError(
            f"Discovery filters matched {len(matches)} markets. Narrow the filters and try again."
        )
    return matches[0]
