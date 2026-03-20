from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .. import market
from ..exceptions import KalshiDependencyError


def _require_numpy():
    try:
        import numpy as np
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("numpy is required for order book analysis") from exc
    return np


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("pandas is required for order book analysis") from exc
    return pd


def _coerce_levels(payload: Any) -> list[tuple[int, float]]:
    levels: list[tuple[int, float]] = []
    for level in payload or ():
        price_cents = None
        size = None
        if isinstance(level, Mapping):
            price_cents = level.get("price_cents")
            size = level.get("count_fp") if "count_fp" in level else level.get("size")
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price_cents = level[0]
            size = level[1]
        if price_cents is None or size is None:
            continue
        levels.append((int(price_cents), float(size)))
    return levels


def _extract_yes_side_levels(snapshot: Mapping[str, Any]) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    if "orderbook" in snapshot:
        book = snapshot["orderbook"]
        yes_bids = sorted(_coerce_levels(book.get("yes")), key=lambda item: item[0])
        yes_asks = sorted([(100 - price, size) for price, size in _coerce_levels(book.get("no"))], key=lambda item: item[0])
        return yes_bids, yes_asks
    if "yes_levels" in snapshot or "no_levels" in snapshot:
        yes_bids = sorted(_coerce_levels(snapshot.get("yes_levels")), key=lambda item: item[0])
        yes_asks = sorted([(100 - price, size) for price, size in _coerce_levels(snapshot.get("no_levels"))], key=lambda item: item[0])
        return yes_bids, yes_asks
    if "bids" in snapshot or "asks" in snapshot:
        yes_bids = sorted(_coerce_levels(({"price_cents": key, "size": value} for key, value in dict(snapshot.get("bids") or {}).items())), key=lambda item: item[0])
        yes_asks = sorted(_coerce_levels(({"price_cents": key, "size": value} for key, value in dict(snapshot.get("asks") or {}).items())), key=lambda item: item[0])
        return yes_bids, yes_asks
    return [], []


@dataclass(frozen=True)
class OrderbookSnapshot:
    market_ticker: str | None
    yes_bids: list[tuple[int, float]]
    yes_asks: list[tuple[int, float]]

    @property
    def best_bid_cents(self) -> int | None:
        return self.yes_bids[-1][0] if self.yes_bids else None

    @property
    def best_ask_cents(self) -> int | None:
        return self.yes_asks[0][0] if self.yes_asks else None

    @property
    def spread_cents(self) -> int | None:
        if self.best_bid_cents is None or self.best_ask_cents is None:
            return None
        return self.best_ask_cents - self.best_bid_cents

    def summary(self) -> dict[str, Any]:
        return {
            "market_ticker": self.market_ticker,
            "best_bid_cents": self.best_bid_cents,
            "best_ask_cents": self.best_ask_cents,
            "spread_cents": self.spread_cents,
            "bid_levels": len(self.yes_bids),
            "ask_levels": len(self.yes_asks),
            "visible_bid_size": float(sum(size for _, size in self.yes_bids)),
            "visible_ask_size": float(sum(size for _, size in self.yes_asks)),
        }


def get_orderbook_snapshot(ticker: str | None = None, *, response: Mapping[str, Any] | None = None) -> OrderbookSnapshot:
    payload = response if response is not None else market.get_market_orderbook(str(ticker))
    yes_bids, yes_asks = _extract_yes_side_levels(payload)
    market_ticker = payload.get("market_ticker") if isinstance(payload, Mapping) else None
    return OrderbookSnapshot(
        market_ticker=str(market_ticker or ticker) if (market_ticker or ticker) else None,
        yes_bids=yes_bids,
        yes_asks=yes_asks,
    )


def render_orderbook_text(snapshot: OrderbookSnapshot, *, max_levels: int | None = None) -> str:
    lines = []
    if snapshot.market_ticker:
        lines.append(f"Ticker: {snapshot.market_ticker}")
    if not snapshot.yes_bids and not snapshot.yes_asks:
        lines.append("No visible YES-side depth in the current book snapshot.")
        return "\n".join(lines)

    lines.append("Bids:")
    bids = snapshot.yes_bids[-max_levels:] if max_levels is not None else snapshot.yes_bids
    for price, quantity in bids:
        lines.append(f"  YES @ {price}c x {quantity:,.2f} contracts")

    lines.append("Asks:")
    asks = snapshot.yes_asks[:max_levels] if max_levels is not None else snapshot.yes_asks
    for price, quantity in asks:
        lines.append(f"  YES @ {price}c x {quantity:,.2f} contracts")
    return "\n".join(lines)


def plot_orderbook_depth(snapshot_or_ticker: OrderbookSnapshot | str, *, ax: Any | None = None, title: str | None = None) -> tuple[Any, Any]:
    np = _require_numpy()
    try:
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ModuleNotFoundError as exc:
        raise KalshiDependencyError("matplotlib is required for order book plots") from exc

    snapshot = snapshot_or_ticker if isinstance(snapshot_or_ticker, OrderbookSnapshot) else get_orderbook_snapshot(snapshot_or_ticker)

    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 6))
    else:
        fig = ax.figure

    bid_prices = [price for price, _ in snapshot.yes_bids]
    bid_sizes = [size for _, size in snapshot.yes_bids]
    bid_cum = list(np.cumsum(bid_sizes[::-1]))[::-1] if bid_sizes else []

    ask_prices = [price for price, _ in snapshot.yes_asks]
    ask_sizes = [size for _, size in snapshot.yes_asks]
    ask_cum = list(np.cumsum(ask_sizes)) if ask_sizes else []

    if not bid_prices and not ask_prices:
        ax.text(0.5, 0.5, "No visible YES-side depth", ha="center", va="center", transform=ax.transAxes)
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 1)
    else:
        if bid_prices:
            bid_prices_ext = bid_prices + [bid_prices[-1]]
            bid_cum_ext = bid_cum + [0]
            ax.step(bid_prices_ext, bid_cum_ext, label="Bids", color="green", where="post")
            ax.fill_between(bid_prices_ext, bid_cum_ext, step="post", color="green", alpha=0.3, hatch="//")
        if ask_prices:
            ask_prices_ext = [ask_prices[0]] + ask_prices
            ask_cum_ext = [0] + ask_cum
            ax.step(ask_prices_ext, ask_cum_ext, label="Asks", color="red", where="post")
            ax.fill_between(ask_prices_ext, ask_cum_ext, step="post", color="red", alpha=0.3, hatch="\\\\")
        ax.legend(loc="upper center")

    ax.set_xlabel("Price (c)")
    ax.set_ylabel("Cumulative Size")
    ax.set_title(title or "YES Order Book Depth")
    ax.set_xlim(0, 100)
    ax.grid(True)

    max_val = max(max(bid_cum, default=0), max(ask_cum, default=0))
    if max_val >= 1_000_000:
        divisor, suffix = 1_000_000, "M"
    elif max_val >= 1_000:
        divisor, suffix = 1_000, "K"
    else:
        divisor, suffix = 1, ""
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x / divisor:.1f}{suffix}"))
    fig.tight_layout()
    return fig, ax


def orderbook_snapshots_to_matrices(snapshots: Iterable[Mapping[str, Any]]) -> tuple[Any, Any]:
    pd = _require_pandas()
    bid_rows = []
    ask_rows = []
    timestamps = []

    for snapshot in snapshots:
        yes_bids, yes_asks = _extract_yes_side_levels(snapshot)
        timestamp = (
            snapshot.get("captured_at")
            or snapshot.get("timestamp")
            or snapshot.get("event_ts")
        )
        timestamps.append(pd.to_datetime(timestamp, utc=True) if timestamp is not None else pd.NaT)

        bid_row = {price: size for price, size in yes_bids}
        ask_row = {price: size for price, size in yes_asks}
        bid_rows.append(bid_row)
        ask_rows.append(ask_row)

    bids_frame = pd.DataFrame.from_records(bid_rows, index=timestamps).sort_index().fillna(0.0)
    asks_frame = pd.DataFrame.from_records(ask_rows, index=timestamps).sort_index().fillna(0.0)
    bids_frame.index.name = "timestamp"
    asks_frame.index.name = "timestamp"
    bids_frame = bids_frame.reindex(sorted(bids_frame.columns), axis=1, fill_value=0.0)
    asks_frame = asks_frame.reindex(sorted(asks_frame.columns), axis=1, fill_value=0.0)
    return bids_frame, asks_frame


def plot_orderbook_matrix_snapshot(
    index: int,
    bids_frame: Any,
    asks_frame: Any,
    *,
    ax: Any | None = None,
    title: str | None = None,
) -> tuple[Any, Any]:
    snapshot = OrderbookSnapshot(
        market_ticker=None,
        yes_bids=[(int(price), float(size)) for price, size in bids_frame.iloc[index].items() if float(size) > 0],
        yes_asks=[(int(price), float(size)) for price, size in asks_frame.iloc[index].items() if float(size) > 0],
    )
    timestamp = bids_frame.index[index]
    return plot_orderbook_depth(snapshot, ax=ax, title=title or f"YES Order Book Depth at {timestamp}")
