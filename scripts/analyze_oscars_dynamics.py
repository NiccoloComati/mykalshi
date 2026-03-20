from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

from mykalshi import events, market
from mykalshi.fixed_point import dollars_to_cents
from mykalshi.research import (
    build_event_closeup,
    get_orderbook_snapshot,
    load_market_history,
    load_trade_history,
    plot_event_closeup,
    plot_market_candles,
    plot_market_comparison,
    plot_orderbook_depth,
    plot_trade_activity,
    render_orderbook_text,
    summarize_trade_history,
)


OSCARS_SERIES: dict[str, str] = {
    "Best Picture": "KXOSCARPIC",
    "Best Actor": "KXOSCARACTO",
    "Best Actress": "KXOSCARACTR",
    "Best Supporting Actor": "KXOSCARSUPACTO",
    "Best Supporting Actress": "KXOSCARSUPACTR",
    "Best Film Editing": "KXOSCAREDIT",
    "Best Original Score": "KXOSCARSCORE",
    "Best Original Song": "KXOSCARSONG",
}

DEEP_DIVE_CATEGORIES = ("Best Picture", "Best Actor", "Best Actress")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an Oscars Kalshi dynamics road-test report.")
    parser.add_argument(
        "--output-dir",
        default=str(Path("docs") / "analysis" / "oscars-2026-road-test"),
        help="Directory where the markdown report and plots will be written.",
    )
    parser.add_argument(
        "--as-of",
        default=datetime.now(timezone.utc).isoformat(),
        help="Reference timestamp in ISO-8601 format. Default: current UTC time.",
    )
    return parser.parse_args()


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _to_cents(value: Any | None) -> float | None:
    if value in (None, ""):
        return None
    return float(dollars_to_cents(value))


def _volume(payload: dict[str, Any]) -> float:
    for key in ("volume", "volume_fp"):
        if payload.get(key) not in (None, ""):
            return float(payload[key])
    return 0.0


def _quote_mid_cents(payload: dict[str, Any]) -> float | None:
    yes_bid = _to_cents(payload.get("yes_bid_dollars"))
    yes_ask = _to_cents(payload.get("yes_ask_dollars"))
    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / 2.0
    return yes_bid if yes_bid is not None else yes_ask


def _pre_resolution_cents(payload: dict[str, Any]) -> float | None:
    previous_price = _to_cents(payload.get("previous_price_dollars"))
    if previous_price is not None:
        return previous_price
    previous_bid = _to_cents(payload.get("previous_yes_bid_dollars"))
    previous_ask = _to_cents(payload.get("previous_yes_ask_dollars"))
    if previous_bid is not None and previous_ask is not None:
        return (previous_bid + previous_ask) / 2.0
    if previous_bid is not None:
        return previous_bid
    if previous_ask is not None:
        return previous_ask
    return _to_cents(payload.get("last_price_dollars"))


def _event_last_close(markets: list[dict[str, Any]]) -> datetime | None:
    close_times = [_parse_iso_timestamp(item.get("close_time")) for item in markets if item.get("close_time")]
    if not close_times:
        return None
    return max(close_times)


def _select_recent_finalized_event(series_ticker: str, *, as_of: datetime) -> dict[str, Any]:
    response = events.get_events(series_ticker=series_ticker, limit=10, with_nested_markets=True)
    candidates = []
    for event in response.get("events", []):
        markets = list(event.get("markets") or [])
        if not markets:
            continue
        last_close = _event_last_close(markets)
        resolved_count = sum(1 for market_item in markets if str(market_item.get("result") or "").lower() in {"yes", "no"})
        finalized_count = sum(1 for market_item in markets if str(market_item.get("status") or "").lower() in {"finalized", "settled"})
        closes_before_as_of = last_close is not None and last_close <= as_of
        candidates.append(
            (
                1 if closes_before_as_of else 0,
                resolved_count,
                finalized_count,
                last_close or datetime.min.replace(tzinfo=timezone.utc),
                event,
            )
        )
    if not candidates:
        raise LookupError(f"No event candidates with nested markets found for {series_ticker}")
    candidates.sort(key=lambda item: item[:-1], reverse=True)
    return dict(candidates[0][-1])


def _fetch_pre_resolution_map(series_ticker: str, event: dict[str, Any]) -> dict[str, float | None]:
    markets = [dict(item) for item in event.get("markets") or []]
    if not markets:
        return {}
    close_time = _event_last_close(markets)
    if close_time is None:
        return {}
    tickers = [str(item["ticker"]) for item in markets if item.get("ticker")]
    response = market.batch_get_market_candlesticks(
        tickers,
        start_ts=(close_time - timedelta(days=3)).strftime("%m/%d/%Y %H:%M:%S"),
        end_ts=(close_time + timedelta(hours=1)).strftime("%m/%d/%Y %H:%M:%S"),
        period_interval=60,
        include_latest_before_start=True,
    )
    pre_resolution: dict[str, float | None] = {}
    for item in response.get("markets", []):
        ticker = item.get("market_ticker")
        candles = item.get("candlesticks") or []
        value = None
        if candles:
            last_candle = candles[-1]
            price_payload = last_candle.get("price", {})
            previous_dollars = price_payload.get("previous_dollars")
            if previous_dollars not in (None, ""):
                value = _to_cents(previous_dollars)
            elif len(candles) >= 2:
                value = _to_cents(candles[-2].get("price", {}).get("close_dollars"))
            else:
                value = _to_cents(price_payload.get("close_dollars"))
        if ticker:
            pre_resolution[str(ticker)] = value
    return pre_resolution


def _build_category_summary(
    category: str,
    series_ticker: str,
    event: dict[str, Any],
    *,
    pre_resolution_map: dict[str, float | None] | None = None,
) -> dict[str, Any]:
    markets = [dict(item) for item in event.get("markets") or []]
    if not markets:
        raise ValueError(f"Event {event.get('event_ticker')} does not contain any nominee markets")

    for market_item in markets:
        ticker = str(market_item.get("ticker"))
        resolved_pre = None if pre_resolution_map is None else pre_resolution_map.get(ticker)
        market_item["pre_resolution_cents"] = resolved_pre if resolved_pre is not None else _pre_resolution_cents(market_item)
        market_item["current_mid_cents"] = _quote_mid_cents(market_item)
        market_item["volume_contracts"] = _volume(market_item)

    pre_ranked = sorted(
        markets,
        key=lambda item: (
            item.get("pre_resolution_cents") is not None,
            item.get("pre_resolution_cents") or -1.0,
            item.get("volume_contracts") or 0.0,
        ),
        reverse=True,
    )
    winner = next((item for item in markets if str(item.get("result") or "").lower() == "yes"), None)
    if winner is None:
        winner = pre_ranked[0]
    favorite = pre_ranked[0]
    runner_up = pre_ranked[1] if len(pre_ranked) > 1 else None
    favorite_gap = None
    if favorite.get("pre_resolution_cents") is not None and runner_up and runner_up.get("pre_resolution_cents") is not None:
        favorite_gap = float(favorite["pre_resolution_cents"] - runner_up["pre_resolution_cents"])

    close_time = _event_last_close(markets)
    total_volume = float(sum(item.get("volume_contracts") or 0.0 for item in markets))
    return {
        "category": category,
        "series_ticker": series_ticker,
        "event_ticker": event.get("event_ticker"),
        "event_title": event.get("title"),
        "nominee_count": len(markets),
        "settled_at_utc": None if close_time is None else close_time.isoformat(),
        "winner": winner.get("yes_sub_title") or winner.get("subtitle") or winner.get("title") or winner.get("ticker"),
        "winner_ticker": winner.get("ticker"),
        "winner_volume_contracts": float(winner.get("volume_contracts") or 0.0),
        "winner_pre_resolution_cents": winner.get("pre_resolution_cents"),
        "pre_resolution_favorite": favorite.get("yes_sub_title") or favorite.get("subtitle") or favorite.get("title") or favorite.get("ticker"),
        "pre_resolution_favorite_ticker": favorite.get("ticker"),
        "pre_resolution_favorite_cents": favorite.get("pre_resolution_cents"),
        "runner_up": None if runner_up is None else runner_up.get("yes_sub_title") or runner_up.get("subtitle") or runner_up.get("title") or runner_up.get("ticker"),
        "runner_up_ticker": None if runner_up is None else runner_up.get("ticker"),
        "runner_up_pre_resolution_cents": None if runner_up is None else runner_up.get("pre_resolution_cents"),
        "favorite_correct": favorite.get("ticker") == winner.get("ticker"),
        "favorite_gap_cents": favorite_gap,
        "event_total_volume_contracts": total_volume,
        "top_three_pre_resolution": ", ".join(
            f"{item.get('yes_sub_title') or item.get('title') or item.get('ticker')} ({item.get('pre_resolution_cents'):.1f}c)"
            for item in pre_ranked[:3]
            if item.get("pre_resolution_cents") is not None
        ),
    }


def _select_active_oscar_market(series_ticker: str, *, as_of: datetime) -> dict[str, Any]:
    response = events.get_events(series_ticker=series_ticker, limit=10, with_nested_markets=True)
    active_event = None
    active_markets: list[dict[str, Any]] = []
    for event in response.get("events", []):
        markets = list(event.get("markets") or [])
        if not markets:
            continue
        last_close = _event_last_close(markets)
        if last_close is not None and last_close <= as_of:
            continue
        if any(str(item.get("status") or "").lower() == "active" for item in markets):
            active_event = event
            active_markets = markets
            break
    if active_event is None or not active_markets:
        raise LookupError(f"Unable to resolve an active Oscars event for {series_ticker}")

    for market_item in active_markets:
        market_item["current_mid_cents"] = _quote_mid_cents(market_item)
    active_markets.sort(
        key=lambda item: (
            item.get("current_mid_cents") is not None,
            item.get("current_mid_cents") or -1.0,
            _volume(item),
        ),
        reverse=True,
    )
    return dict(active_markets[0])


def _save_figure(fig: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _render_table(frame: pd.DataFrame) -> str:
    return frame.to_html(index=False, border=0, classes="dataframe")


def _write_summary_plots(summary_df: pd.DataFrame, output_dir: Path) -> dict[str, str]:
    assets: dict[str, str] = {}

    ordered = summary_df.copy()
    ordered["favorite_gap_cents"] = pd.to_numeric(ordered["favorite_gap_cents"], errors="coerce").fillna(0.0)
    ordered = ordered.sort_values("favorite_gap_cents", ascending=True).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = ["tab:green" if bool(value) else "tab:red" for value in ordered["favorite_correct"]]
    ax.barh(ordered["category"], ordered["favorite_gap_cents"], color=colors)
    ax.set_xlabel("Pre-resolution favorite gap (cents)")
    ax.set_title("Oscars 2026: how decisive each category looked before settlement")
    ax.grid(True, axis="x")
    gap_path = output_dir / "favorite-gap-by-category.png"
    _save_figure(fig, gap_path)
    assets["favorite_gap"] = gap_path.name

    ordered_volume = summary_df.copy()
    ordered_volume["winner_volume_contracts"] = pd.to_numeric(
        ordered_volume["winner_volume_contracts"], errors="coerce"
    ).fillna(0.0)
    ordered_volume = ordered_volume.sort_values("winner_volume_contracts", ascending=True).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(ordered_volume["category"], ordered_volume["winner_volume_contracts"], color="tab:blue")
    ax.set_xlabel("Winner total traded contracts")
    ax.set_title("Oscars 2026: winner-side traded volume by category")
    ax.grid(True, axis="x")
    volume_path = output_dir / "winner-volume-by-category.png"
    _save_figure(fig, volume_path)
    assets["winner_volume"] = volume_path.name

    return assets


def _write_winner_comparison_plot(summary_df: pd.DataFrame, output_dir: Path) -> str:
    histories = {}
    end_ts = summary_df["settled_at_utc"].dropna().max()
    start_ts = (_parse_iso_timestamp(end_ts) - timedelta(days=30)).isoformat() if end_ts else None
    for row in summary_df.to_dict(orient="records"):
        histories[row["winner_ticker"]] = load_market_history(
            row["winner_ticker"],
            series_ticker=row["series_ticker"],
            label=row["category"],
            period_interval="d",
            start_ts=start_ts,
            end_ts=end_ts,
        )
    fig, _ = plot_market_comparison(
        histories,
        metric="mid",
        include_aggregate=False,
        title="Oscars 2026 winners: final 30-day YES mid comparison",
    )
    path = output_dir / "winner-mid-comparison-final-30d.png"
    _save_figure(fig, path)
    return path.name


def _write_deep_dive_assets(
    category: str,
    summary_row: dict[str, Any],
    output_dir: Path,
) -> dict[str, str]:
    closeup = build_event_closeup(summary_row["event_ticker"], period_interval="h")
    assets: dict[str, str] = {}

    fig, _ = plot_event_closeup(closeup, metric="mid", include_aggregate=False, title=f"{category}: nominee hourly mid quotes")
    closeup_path = output_dir / f"{summary_row['event_ticker'].lower()}-closeup.png"
    _save_figure(fig, closeup_path)
    assets["closeup"] = closeup_path.name

    winner_history = load_market_history(
        summary_row["winner_ticker"],
        series_ticker=summary_row["series_ticker"],
        label=summary_row["winner"],
        period_interval="d",
    )
    fig, _ = plot_market_candles(
        winner_history,
        title=f"{category}: {summary_row['winner']} daily candlestick",
        volume=True,
        hlines=[50.0],
    )
    candle_path = output_dir / f"{summary_row['winner_ticker'].lower()}-candles.png"
    _save_figure(fig, candle_path)
    assets["candles"] = candle_path.name

    if summary_row.get("runner_up_ticker") and summary_row["runner_up_ticker"] in closeup.histories:
        comparison_histories = {
            summary_row["winner_ticker"]: closeup.histories[summary_row["winner_ticker"]],
            summary_row["runner_up_ticker"]: closeup.histories[summary_row["runner_up_ticker"]],
        }
        fig, _ = plot_market_comparison(
            comparison_histories,
            metric="mid",
            title=f"{category}: winner vs runner-up hourly mid quotes",
        )
        compare_path = output_dir / f"{summary_row['event_ticker'].lower()}-winner-vs-runner-up.png"
        _save_figure(fig, compare_path)
        assets["comparison"] = compare_path.name

    settled_at = _parse_iso_timestamp(summary_row["settled_at_utc"])
    trade_window_start = None if settled_at is None else settled_at - timedelta(days=45)
    trade_history = load_trade_history(
        summary_row["winner_ticker"],
        start_ts=trade_window_start,
        end_ts=settled_at,
    )
    fig, _ = plot_trade_activity(
        trade_history,
        freq="1D",
        title=f"{category}: {summary_row['winner']} daily trade activity (final 45 days)",
    )
    trades_path = output_dir / f"{summary_row['winner_ticker'].lower()}-trade-activity.png"
    _save_figure(fig, trades_path)
    assets["trade_activity"] = trades_path.name
    return assets


def _write_live_orderbook_asset(output_dir: Path, *, as_of: datetime) -> tuple[str, dict[str, Any], str]:
    active_market = _select_active_oscar_market("KXOSCARPIC", as_of=as_of)
    snapshot = get_orderbook_snapshot(active_market["ticker"])
    fig, _ = plot_orderbook_depth(
        snapshot,
        title=f"Live YES depth for {active_market.get('yes_sub_title') or active_market['ticker']}",
    )
    path = output_dir / f"{active_market['ticker'].lower()}-live-orderbook.png"
    _save_figure(fig, path)
    return path.name, active_market, render_orderbook_text(snapshot, max_levels=8)


def _write_report(
    *,
    output_dir: Path,
    as_of: datetime,
    summary_df: pd.DataFrame,
    summary_assets: dict[str, str],
    winner_comparison_asset: str,
    deep_dive_assets: dict[str, dict[str, str]],
    trade_summaries: pd.DataFrame,
    live_orderbook_asset: str,
    live_orderbook_market: dict[str, Any],
    live_orderbook_text: str,
) -> Path:
    report_path = output_dir / "report.md"
    favorite_correct_rate = float(summary_df["favorite_correct"].mean()) if not summary_df.empty else 0.0

    lines = [
        "# Oscars Dynamics Road Test",
        "",
        f"Generated from live Kalshi data on {as_of.astimezone(timezone.utc).isoformat()}.",
        "",
        "This road test uses the current `mykalshi` research stack rather than notebook-only code. It demonstrates:",
        "",
        "- multi-category event selection across recurring series",
        "- event close-up quote panels",
        "- market candlestick and comparison plots",
        "- full trade-history loading plus trade-flow summaries",
        "- live order book inspection on the next Oscars cycle",
        "",
        "It also shows the current limitation clearly: without a prior websocket capture session, the toolkit cannot reconstruct **historical** Oscars order books tick by tick after the fact. It can analyze historical trades and quote candles, and it can inspect **current** live order books.",
        "",
        "## Cross-category summary",
        "",
        f"- Categories analyzed: {len(summary_df)}",
        f"- Pre-resolution favorite was correct in {favorite_correct_rate:.0%} of the analyzed categories.",
        f"- Most actively traded winner market: `{summary_df.sort_values('winner_volume_contracts', ascending=False).iloc[0]['winner_ticker']}`",
        "",
        _render_table(
            summary_df[
                [
                    "category",
                    "event_ticker",
                    "winner",
                    "pre_resolution_favorite",
                    "favorite_correct",
                    "favorite_gap_cents",
                    "winner_volume_contracts",
                ]
            ]
        ),
        "",
        "![Favorite gap by category](./" + summary_assets["favorite_gap"] + ")",
        "",
        "![Winner volume by category](./" + summary_assets["winner_volume"] + ")",
        "",
        "![Winner final 30-day mid comparison](./" + winner_comparison_asset + ")",
        "",
        "## Winner trade summaries",
        "",
        _render_table(trade_summaries),
        "",
        "Trade summaries above are based on the final 45 days before settlement for the deep-dive winner markets. Full lifecycle volume is reported separately in the cross-category table.",
        "",
    ]

    for category in DEEP_DIVE_CATEGORIES:
        if category not in deep_dive_assets:
            continue
        assets = deep_dive_assets[category]
        row = summary_df.loc[summary_df["category"] == category].iloc[0]
        lines.extend(
            [
                f"## Deep dive: {category}",
                "",
                f"- Event: `{row['event_ticker']}`",
                f"- Winner: **{row['winner']}**",
                f"- Favorite just before settlement: **{row['pre_resolution_favorite']}** at {row['pre_resolution_favorite_cents']:.1f}c",
                f"- Runner-up just before settlement: **{row['runner_up']}** at {row['runner_up_pre_resolution_cents']:.1f}c"
                if pd.notna(row["runner_up_pre_resolution_cents"])
                else "- Runner-up pre-resolution quote unavailable",
                "",
                "![Close-up mid quotes](./" + assets["closeup"] + ")",
                "",
                "![Winner candlestick](./" + assets["candles"] + ")",
                "",
            ]
        )
        if "comparison" in assets:
            lines.extend(["![Winner vs runner-up](./" + assets["comparison"] + ")", ""])
        lines.extend(["![Winner trade activity](./" + assets["trade_activity"] + ")", ""])

    lines.extend(
        [
            "## Live order book check on the next Oscars cycle",
            "",
            "This is not historical March 2026 depth. It is a live sanity check showing that the new order-book inspection tooling works on current Oscars futures too.",
            "",
            f"- Market: `{live_orderbook_market['ticker']}`",
            f"- Nominee: **{live_orderbook_market.get('yes_sub_title') or live_orderbook_market.get('title')}**",
            f"- Current quoted YES mid: {_quote_mid_cents(live_orderbook_market)}c",
            "",
            "![Live orderbook depth](./" + live_orderbook_asset + ")",
            "",
            "```text",
            live_orderbook_text,
            "```",
            "",
            "## What the current code can and cannot do here",
            "",
            "Can do now:",
            "- pull recurring Oscars series and select the recent finalized category event",
            "- analyze nominee-level quote and candlestick dynamics per category",
            "- download full trade histories for specific nominee markets",
            "- summarize cross-category favorites, winners, and trading intensity",
            "- inspect current live Oscars order book depth",
            "",
            "Cannot do now unless data was captured in advance:",
            "- reconstruct historical Oscars order-book microstructure from settlement week",
            "- replay quote-by-quote or depth-by-depth Oscar dynamics after the fact without a stored websocket session",
            "",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main() -> None:
    args = _parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    as_of = _parse_iso_timestamp(args.as_of)
    if as_of is None:
        raise ValueError("Unable to parse --as-of timestamp")

    category_rows: list[dict[str, Any]] = []
    events_by_category: dict[str, dict[str, Any]] = {}
    for category, series_ticker in OSCARS_SERIES.items():
        event = _select_recent_finalized_event(series_ticker, as_of=as_of)
        events_by_category[category] = event
        pre_resolution_map = _fetch_pre_resolution_map(series_ticker, event)
        category_rows.append(_build_category_summary(category, series_ticker, event, pre_resolution_map=pre_resolution_map))

    summary_df = pd.DataFrame(category_rows).sort_values("category").reset_index(drop=True)
    summary_df.to_csv(output_dir / "oscars-category-summary.csv", index=False)

    summary_assets = _write_summary_plots(summary_df, output_dir)
    winner_comparison_asset = _write_winner_comparison_plot(summary_df, output_dir)

    trade_summary_rows: list[dict[str, Any]] = []
    deep_dive_assets: dict[str, dict[str, str]] = {}
    for category in DEEP_DIVE_CATEGORIES:
        row = summary_df.loc[summary_df["category"] == category].iloc[0].to_dict()
        settled_at = _parse_iso_timestamp(row["settled_at_utc"])
        trade_window_start = None if settled_at is None else settled_at - timedelta(days=45)
        trade_history = load_trade_history(
            row["winner_ticker"],
            start_ts=trade_window_start,
            end_ts=settled_at,
        )
        trade_summary = summarize_trade_history(trade_history)
        trade_summary["category"] = category
        trade_summary_rows.append(trade_summary)
        deep_dive_assets[category] = _write_deep_dive_assets(category, row, output_dir)

    trade_summaries = pd.DataFrame(trade_summary_rows)[
        [
            "category",
            "ticker",
            "trade_count",
            "total_contracts",
            "vwap_yes_price_cents",
            "avg_trade_size",
            "yes_taker_contract_share",
        ]
    ]
    trade_summaries.to_csv(output_dir / "oscars-deep-dive-trade-summary.csv", index=False)

    live_orderbook_asset, live_orderbook_market, live_orderbook_text = _write_live_orderbook_asset(output_dir, as_of=as_of)
    report_path = _write_report(
        output_dir=output_dir,
        as_of=as_of,
        summary_df=summary_df,
        summary_assets=summary_assets,
        winner_comparison_asset=winner_comparison_asset,
        deep_dive_assets=deep_dive_assets,
        trade_summaries=trade_summaries,
        live_orderbook_asset=live_orderbook_asset,
        live_orderbook_market=live_orderbook_market,
        live_orderbook_text=live_orderbook_text,
    )
    print(f"Wrote Oscars road-test report to {report_path}")


if __name__ == "__main__":
    main()
