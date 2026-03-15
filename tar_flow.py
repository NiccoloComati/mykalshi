from __future__ import annotations

import sys

from mykalshi import events, market


def main() -> None:
    series_ticker = sys.argv[1] if len(sys.argv) > 1 else None
    event_response = events.get_events(
        series_ticker=series_ticker,
        status="open",
        limit=10,
    )
    event_items = event_response.get("events", [])
    if not event_items:
        scope = f" for series_ticker={series_ticker}" if series_ticker else ""
        raise SystemExit(
            f"No open events found{scope}. Run with a real series ticker, for example: "
            r".\.venv\Scripts\python tar_flow.py HIGHMIA"
        )

    event = event_items[0]
    market_response = market.get_markets(
        event_ticker=event["event_ticker"],
        status="open",
        limit=10,
    )
    market_items = market_response.get("markets", [])
    if not market_items:
        raise SystemExit(f"No open markets found for event_ticker={event['event_ticker']}")

    ticker = market_items[0]["ticker"]
    print("series_ticker:", event["series_ticker"])
    print("event_ticker:", event["event_ticker"])
    print("market_ticker:", ticker)
    print(market.get_market_orderbook(ticker))


if __name__ == "__main__":
    main()
