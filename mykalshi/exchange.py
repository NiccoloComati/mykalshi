from __future__ import annotations

from .transport import kalshi_get


def get_exchange_announcements():
    return kalshi_get("/exchange/announcements")


def get_exchange_schedule():
    return kalshi_get("/exchange/schedule")


def get_exchange_status():
    return kalshi_get("/exchange/status")


def get_user_data_timestamp():
    return kalshi_get("/exchange/user_data_timestamp")


def get_series_fee_changes(series_ticker=None, show_historical=False):
    params = {
        "series_ticker": series_ticker,
        "show_historical": show_historical,
    }
    return kalshi_get("/series/fee_changes", {k: v for k, v in params.items() if v is not None})
