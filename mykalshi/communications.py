from __future__ import annotations

from .transport import kalshi_delete, kalshi_get, kalshi_post, kalshi_put


def get_api_version():
    return kalshi_get("/api_version")


def get_communications_id():
    return kalshi_get("/communications/id", authenticated=True)


def get_quotes():
    return kalshi_get("/communications/quotes", authenticated=True)


def create_quote(rfq_id, yes_bid=None, no_bid=None, rest_remainder=False):
    body = {
        "rfq_id": rfq_id,
        "yes_bid": yes_bid,
        "no_bid": no_bid,
        "rest_remainder": rest_remainder,
    }
    return kalshi_post("/communications/quotes", body, authenticated=True)


def get_quote(quote_id):
    return kalshi_get(f"/communications/quotes/{quote_id}", authenticated=True)


def delete_quote(quote_id):
    return kalshi_delete(f"/communications/quotes/{quote_id}", authenticated=True)


def accept_quote(quote_id, accepted_side):
    return kalshi_put(
        f"/communications/quotes/{quote_id}/accept",
        {"accepted_side": accepted_side},
        authenticated=True,
    )


def confirm_quote(quote_id):
    return kalshi_put(f"/communications/quotes/{quote_id}/confirm", authenticated=True)


def get_rfqs():
    return kalshi_get("/communications/rfqs", authenticated=True)


def create_rfq(market_ticker, contracts, rest_remainder=False):
    body = {
        "market_ticker": market_ticker,
        "contracts": contracts,
        "rest_remainder": rest_remainder,
    }
    return kalshi_post("/communications/rfqs", body, authenticated=True)


def get_rfq(rfq_id):
    return kalshi_get(f"/communications/rfqs/{rfq_id}", authenticated=True)


def delete_rfq(rfq_id):
    return kalshi_delete(f"/communications/rfqs/{rfq_id}", authenticated=True)
