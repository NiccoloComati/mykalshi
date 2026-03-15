from __future__ import annotations

from typing import Any, Callable

from .client import get_default_client


def kalshi_get(endpoint, params=None, authenticated=False):
    return get_default_client().get(endpoint, params=params, authenticated=authenticated)


def kalshi_post(endpoint, body=None, authenticated=False):
    return get_default_client().post(endpoint, json_body=body, authenticated=authenticated)


def kalshi_put(endpoint, body=None, authenticated=False):
    return get_default_client().put(endpoint, json_body=body, authenticated=authenticated)


def kalshi_delete(endpoint, body=None, params=None, authenticated=False):
    return get_default_client().delete(
        endpoint,
        params=params,
        json_body=body,
        authenticated=authenticated,
    )


def collect_cursor_pages(
    fetch_page: Callable[[str | None], dict[str, Any]],
    *,
    item_key: str,
    cursor_key: str = "cursor",
):
    items = []
    cursor = None
    while True:
        response = fetch_page(cursor)
        items.extend(response.get(item_key, []))
        cursor = response.get(cursor_key)
        if not cursor:
            break
    return items
