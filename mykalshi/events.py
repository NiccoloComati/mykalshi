import pandas as pd
from datetime import datetime, timedelta
from .transport import kalshi_get
from .formatting import format_timestamp

def get_event(event_ticker, with_nested_markets=False):
    """
    Get details for a specific event.

    Parameters:
        event_ticker (str): The event identifier
        with_nested_markets (bool): Include related markets
    """
    return kalshi_get(f"/events/{event_ticker}", {"with_nested_markets": with_nested_markets})

def get_events(limit=100, cursor=None, status=None, series_ticker=None, with_nested_markets=False):
    """
    Get a list of events with optional filtering.

    Parameters:
        limit (int): Max number of results (1-200)
        cursor (str): Used for pagination
        status (str): Filter events by status: unopened, open, closed, settled
        series_ticker (str): Filter events by series
        with_nested_markets (bool): Include nested market objects
    """
    params = {
        "limit": limit,
        "cursor": cursor,
        "status": status,
        "series_ticker": series_ticker,
        "with_nested_markets": with_nested_markets
    }
    return kalshi_get("/events", {k: v for k, v in params.items() if v is not None})


def get_all_events(status=None, series_ticker=None, with_nested_markets=False, batch_size=200):
    """
    Fetch every event by paging through /events.

    Filters (all optional):
      • status: unopened, open, closed, settled  
      • series_ticker: only those in a given series  
      • with_nested_markets: include nested market objects  

    Internally pages with `limit=batch_size` until no cursor remains.
    """
    all_events = []
    cursor = None
    while True:
        resp = get_events(
            cursor=cursor,
            status=status,
            series_ticker=series_ticker,
            with_nested_markets=with_nested_markets,
            limit=batch_size
        )
        all_events.extend(resp["events"])
        cursor = resp.get("cursor")
        if not cursor:
            break
    return all_events

def get_series_list(category=None, include_product_metadata=False):
    """
    Get a list of series optionally filtered by category.

    Parameters:
        category (str): Filter series by category
        include_product_metadata (bool): Include metadata
    """
    params = {
        "category": category,
        "include_product_metadata": include_product_metadata
    }
    return kalshi_get("/series/", {k: v for k, v in params.items() if v is not None})

def get_series(series_ticker):
    """
    Get a specific series by its ticker.

    Parameters:
        series_ticker (str): The series identifier
    """
    return kalshi_get(f"/series/{series_ticker}")

def get_all_series(category=None, include_product_metadata=False, batch_size=100):
    """
    Fetch every series by paging through /series/.

    Filters (all optional):
      • category: only series in this category  
      • include_product_metadata: include extra metadata  

    Internally pages with `limit=batch_size` until no cursor remains.
    """
    all_series = []
    cursor = None
    while True:
        params = {
            "category": category,
            "include_product_metadata": include_product_metadata,
            "limit": batch_size,
            "cursor": cursor
        }
        resp = kalshi_get("/series/", {k: v for k, v in params.items() if v is not None})
        all_series.extend(resp["series"])
        cursor = resp.get("cursor")
        if not cursor:
            break
    return all_series

def get_event_collection(collection_ticker):
    """
    Retrieve a specific multivariate event collection.

    Parameters:
        collection_ticker (str): Collection identifier.

    Returns:
        Metadata and structure of the event collection.
    """
    return kalshi_get(f"/multivariate_event_collections/{collection_ticker}")

def get_event_collections(status=None, associated_event_ticker=None, series_ticker=None, limit=100, cursor=None):
    """
    Retrieve multivariate event collections.

    Parameters:
        status (str): Filter collections by status (unopened, open, closed).
        associated_event_ticker (str): Filter by related event ticker.
        series_ticker (str): Filter by series ticker.
        limit (int): Number of results (1 to 200).
        cursor (str): Pagination cursor.

    Returns:
        List of event collection metadata.
    """
    params = {
        "status": status,
        "associated_event_ticker": associated_event_ticker,
        "series_ticker": series_ticker,
        "limit": limit,
        "cursor": cursor
    }
    return kalshi_get("/multivariate_event_collections/", {k: v for k, v in params.items() if v is not None})

def get_milestone(milestone_id):
    """
    Retrieve details for a specific milestone by its ID.

    Parameters:
        milestone_id (str): Unique milestone identifier.

    Returns:
        Milestone details.
    """
    return kalshi_get(f"/milestones/{milestone_id}")

def get_milestones(limit, minimum_start_date=None, category=None, type=None, related_event_ticker=None, cursor=None):
    """
    Retrieve a list of milestones, with optional filters.

    Parameters:
        limit (int): Number of results (1 to 500) — required.
        minimum_start_date (str): ISO format datetime to filter by start date.
        category (str): Filter milestones by category.
        type (str): Filter milestones by type.
        related_event_ticker (str): Filter by related event.
        cursor (str): Pagination cursor.

    Returns:
        List of milestone metadata.
    """
    params = {
        "limit": limit,
        "minimum_start_date": minimum_start_date,
        "category": category,
        "type": type,
        "related_event_ticker": related_event_ticker,
        "cursor": cursor
    }
    return kalshi_get("/milestones/", {k: v for k, v in params.items() if v is not None})

def event_info(event_ticker):
    data = get_event(event_ticker)
    event = data["event"]
    markets = data["markets"]

    # Create a summary DataFrame of markets
    market_df = pd.DataFrame([{
        "market_ticker": m["ticker"],
        "yes_sub_title": m.get("yes_sub_title"),
        "range": m.get("subtitle") or f"{m.get('floor_strike', '')} – {m.get('cap_strike', '')}",
        "strike_type": m.get("strike_type"),
        "last_price": m.get("last_price"),
        "yes_bid": m.get("yes_bid"),
        "yes_ask": m.get("yes_ask"),
        "no_bid": m.get("no_bid"),
        "no_ask": m.get("no_ask"),
        "volume": m.get("volume"),
        "open_time": m.get("open_time"),
        "close_time": m.get("close_time"),
        "status": m.get("status"),
        "rules_primary": m.get("rules_primary"),
    } for m in markets])

    # Convert relevant timestamps
    if "strike_date" in event:
        event["strike_date"] = datetime.fromisoformat(event["strike_date"].replace("Z", "+00:00"))

    event_info = {
        "event_ticker": event["event_ticker"],
        "series_ticker": event["series_ticker"],
        "title": event["title"],
        "subtitle": event.get("sub_title", ""),
        "strike_date": event.get("strike_date"),
        "category": event.get("category", ""),
        "market_count": len(market_df),
    }

    return {
        "event_info": event_info,
        "markets": market_df
    }

def get_structured_target(structured_target_id):
    """Retrieve a structured target by ID.

    Args:
        structured_target_id (str): The ID of the structured target.

    Returns:
        JSON response containing structured target metadata.
    """
    return kalshi_get(f"/structured_targets/{structured_target_id}")