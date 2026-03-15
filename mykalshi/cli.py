from __future__ import annotations

import argparse
import importlib
import io
import json
import sys
from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from .exceptions import KalshiError
from .research import (
    KalshiWebsocketClient,
    MultiMarketDataSink,
    MultiOrderbookSink,
    ParquetMarketDataSink,
    ParquetOrderbookSink,
    ReplayBacktester,
    ResearchSession,
    SQLiteMarketDataSink,
    SQLiteOrderbookSink,
    TradeBacktester,
)
from .trading_workflows import OrderIntent, TradingSafetyPolicy, TradingSession


def _json_default(value: Any) -> Any:
    if hasattr(value, "summary") and callable(value.summary):
        return value.summary()
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _emit_json(value: Any, stdout: io.TextIOBase) -> None:
    json.dump(value, stdout, indent=2, default=_json_default)
    stdout.write("\n")


def _parse_json_object(value: str | None) -> dict[str, Any]:
    if value is None:
        return {}
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object")
    return payload


def _load_object(import_path: str) -> Any:
    module_name, separator, attribute_path = import_path.partition(":")
    if not separator or not module_name or not attribute_path:
        raise ValueError("Import paths must use the form 'module.submodule:attribute'")
    module = importlib.import_module(module_name)
    target = module
    for part in attribute_path.split("."):
        target = getattr(target, part)
    return target


def _build_strategy(import_path: str, kwargs_json: str | None) -> Any:
    kwargs = _parse_json_object(kwargs_json)
    target = _load_object(import_path)
    if isinstance(target, type):
        return target(**kwargs)
    if kwargs:
        return target(**kwargs)
    return target


def _capture_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(event.get("channel") or event.get("event_type") or "unknown") for event in events)
    return {
        "event_count": len(events),
        "channels": dict(sorted(counts.items())),
        "first_event": events[0] if events else None,
        "last_event": events[-1] if events else None,
    }


def _serialize_collection(items: Sequence[Any]) -> list[Any]:
    serialized: list[Any] = []
    for item in items:
        if hasattr(item, "summary") and callable(item.summary):
            serialized.append(item.summary())
        else:
            serialized.append(item)
    return serialized


def _build_trading_policy(args: argparse.Namespace, *, mutation_default_dry_run: bool) -> TradingSafetyPolicy:
    execute = getattr(args, "execute", False)
    dry_run = not execute if mutation_default_dry_run else False
    return TradingSafetyPolicy(
        allow_production_writes=getattr(args, "allow_production_writes", False),
        dry_run=dry_run,
        max_order_quantity=getattr(args, "max_order_quantity", None),
        max_order_risk_cents=getattr(args, "max_order_risk_cents", None),
        max_total_resting_value_cents=getattr(args, "max_total_resting_value_cents", None),
        max_open_orders_per_market=getattr(args, "max_open_orders_per_market", None),
        allowed_tickers=tuple(getattr(args, "allowed_ticker", []) or ()),
        blocked_tickers=tuple(getattr(args, "blocked_ticker", []) or ()),
        audit_log_path=getattr(args, "audit_log_path", None),
    )


def _build_market_data_sink(args: argparse.Namespace):
    sinks: list[Any] = []
    if getattr(args, "sqlite_path", None):
        sinks.append(SQLiteMarketDataSink(args.sqlite_path))
    if getattr(args, "parquet_dir", None):
        sinks.append(ParquetMarketDataSink(args.parquet_dir))
    if not sinks:
        return None, []
    if len(sinks) == 1:
        return sinks[0], sinks
    return MultiMarketDataSink(*sinks), sinks


def _build_orderbook_sink(args: argparse.Namespace):
    sinks: list[Any] = []
    if getattr(args, "sqlite_path", None):
        sinks.append(SQLiteOrderbookSink(args.sqlite_path))
    if getattr(args, "parquet_dir", None):
        sinks.append(ParquetOrderbookSink(args.parquet_dir))
    if not sinks:
        return None, []
    if len(sinks) == 1:
        return sinks[0], sinks
    return MultiOrderbookSink(*sinks), sinks


def _handle_discover(args: argparse.Namespace) -> Any:
    session = ResearchSession()
    if args.discover_command == "series":
        items = session.search_series(
            category=args.category,
            ticker=args.ticker,
            ticker_contains=args.ticker_contains,
            title_contains=args.title_contains,
            tag_contains=args.tag_contains,
            query=args.query,
            limit=args.limit,
        )
        return _serialize_collection(items)
    if args.discover_command == "events":
        items = session.search_events(
            category=args.category,
            series_ticker=args.series_ticker,
            status=args.status,
            event_ticker=args.event_ticker,
            series_title_contains=args.series_title_contains,
            event_title_contains=args.event_title_contains,
            subtitle_contains=args.subtitle_contains,
            query=args.query,
            limit=args.limit,
        )
        return _serialize_collection(items)
    if args.discover_command == "markets":
        items = session.search_markets(
            category=args.category,
            series_ticker=args.series_ticker,
            event_ticker=args.event_ticker,
            status=args.status,
            series_title_contains=args.series_title_contains,
            event_title_contains=args.event_title_contains,
            market_title_contains=args.market_title_contains,
            subtitle_contains=args.subtitle_contains,
            market_ticker_contains=args.market_ticker_contains,
            query=args.query,
            limit=args.limit,
        )
        return _serialize_collection(items)
    items = session.search_market_universes(
        category=args.category,
        series_ticker=args.series_ticker,
        event_ticker=args.event_ticker,
        status=args.status,
        series_title_contains=args.series_title_contains,
        event_title_contains=args.event_title_contains,
        market_title_contains=args.market_title_contains,
        subtitle_contains=args.subtitle_contains,
        market_ticker_contains=args.market_ticker_contains,
        query=args.query,
        limit=args.limit,
    )
    return _serialize_collection(items)


def _handle_capture(args: argparse.Namespace) -> Any:
    client = KalshiWebsocketClient()
    if args.capture_command == "market-data":
        channels = [channel.strip() for channel in args.channels if channel.strip()]
        sink, owned_sinks = _build_market_data_sink(args)
        try:
            events = client.capture_market_data_sync(
                channels=channels,
                market_ticker=args.market_ticker,
                market_tickers=args.market_tickers,
                sink=sink,
                max_events=args.max_events,
                duration_secs=args.duration_secs,
                receive_timeout=args.receive_timeout,
                send_initial_snapshot=args.send_initial_snapshot,
                include_book_state=args.include_book_state,
                authenticated=not args.public,
            )
        finally:
            for item in owned_sinks:
                if hasattr(item, "close"):
                    item.close()
        return _capture_summary(events)

    sink, owned_sinks = _build_orderbook_sink(args)
    try:
        events = client.capture_orderbook_sync(
            args.market_ticker,
            sink=sink,
            max_events=args.max_events,
            duration_secs=args.duration_secs,
            receive_timeout=args.receive_timeout,
            include_book_state=args.include_book_state,
        )
    finally:
        for item in owned_sinks:
            if hasattr(item, "close"):
                item.close()
    return _capture_summary(events)


def _handle_replay(args: argparse.Namespace) -> Any:
    dataset = ResearchSession().load_replay_dataset(
        market_data_source=args.market_data_source,
        orderbook_source=args.orderbook_source,
        market_ticker=args.market_ticker,
        channel=args.channel,
        include_replayed_orderbook_levels=not args.exclude_replayed_orderbook_levels,
        limit=args.limit,
    )
    payload = dataset.summary()
    if args.include_events:
        payload["events"] = dataset.replay_events
    return payload


def _handle_backtest(args: argparse.Namespace) -> Any:
    strategy = _build_strategy(args.strategy, args.strategy_kwargs)
    if args.backtest_command == "historical":
        result = TradeBacktester().run_on_historical_trades(
            args.ticker,
            strategy,
            min_ts=args.min_ts,
            max_ts=args.max_ts,
            batch_size=args.batch_size,
            initial_cash_cents=args.initial_cash_cents,
            initial_yes_position=args.initial_yes_position,
            initial_no_position=args.initial_no_position,
        )
        return result.summary()

    result = ReplayBacktester().run_on_captured_dataset(
        strategy,
        market_data_source=args.market_data_source,
        orderbook_source=args.orderbook_source,
        market_ticker=args.market_ticker,
        include_replayed_orderbook_levels=not args.exclude_replayed_orderbook_levels,
        limit=args.limit,
        enrich_market_lifecycle=not args.disable_lifecycle_enrichment,
        initial_cash_cents=args.initial_cash_cents,
        initial_yes_position=args.initial_yes_position,
        initial_no_position=args.initial_no_position,
    )
    payload = result.summary()
    if args.include_market_summaries:
        payload["markets"] = [summary.summary() for summary in result.market_summaries()]
    return payload


def _handle_trading(args: argparse.Namespace) -> Any:
    session = TradingSession(
        policy=_build_trading_policy(args, mutation_default_dry_run=args.trading_command in {"plan-order", "replace-order", "flatten", "cancel-stale"})
    )
    if args.trading_command == "snapshot":
        return session.snapshot(
            ticker=args.ticker,
            event_ticker=args.event_ticker,
            order_status=args.order_status,
        ).summary()
    if args.trading_command == "market":
        return session.market_snapshot(
            args.market_ticker,
            include_orderbook=not args.no_orderbook,
        ).summary()
    if args.trading_command == "plan-order":
        intent = OrderIntent(
            ticker=args.market_ticker,
            action=args.action,
            side=args.side,
            quantity=args.quantity,
            limit_price_cents=args.limit_price_cents,
            time_in_force=args.time_in_force,
            expiration_ts=args.expiration_ts,
            client_order_id=args.client_order_id,
            buy_max_cost_cents=args.buy_max_cost_cents,
            post_only=args.post_only,
            reduce_only=args.reduce_only,
            self_trade_prevention_type=args.self_trade_prevention_type,
            order_group_id=args.order_group_id,
        )
        return session.submit_order(intent).summary()
    if args.trading_command == "replace-order":
        return session.replace_order(
            args.order_id,
            quantity=args.quantity,
            limit_price_cents=args.limit_price_cents,
            client_order_id=args.client_order_id,
        ).summary()
    if args.trading_command == "flatten":
        return session.flatten_market(
            args.market_ticker,
            limit_price_cents=args.limit_price_cents,
            client_order_id=args.client_order_id,
        ).summary()
    return session.cancel_stale_orders(
        max_age_seconds=args.max_age_seconds,
        ticker=args.ticker,
        event_ticker=args.event_ticker,
    ).summary()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mykalshi", description="Kalshi discovery, research, backtest, and trading workflows.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    discover_parser = subparsers.add_parser("discover", help="Discover series, events, markets, and grouped market universes.")
    discover_subparsers = discover_parser.add_subparsers(dest="discover_command", required=True)
    for name in ("series", "events", "markets", "universes"):
        subparser = discover_subparsers.add_parser(name, help=f"Discover {name}.")
        subparser.add_argument("--query")
        subparser.add_argument("--category")
        subparser.add_argument("--limit", type=int)
        if name == "series":
            subparser.add_argument("--ticker")
            subparser.add_argument("--ticker-contains")
            subparser.add_argument("--title-contains")
            subparser.add_argument("--tag-contains")
        else:
            subparser.add_argument("--series-ticker")
            subparser.add_argument("--status")
            if name != "events":
                subparser.add_argument("--event-ticker")
                subparser.add_argument("--market-title-contains")
                subparser.add_argument("--market-ticker-contains")
            else:
                subparser.add_argument("--event-ticker")
            subparser.add_argument("--series-title-contains")
            subparser.add_argument("--event-title-contains")
            subparser.add_argument("--subtitle-contains")

    capture_parser = subparsers.add_parser("capture", help="Capture live websocket data into memory or sinks.")
    capture_subparsers = capture_parser.add_subparsers(dest="capture_command", required=True)
    capture_market_data = capture_subparsers.add_parser("market-data", help="Capture ticker/trade/orderbook market-data channels.")
    capture_market_data.add_argument("--channels", nargs="+", required=True)
    capture_market_data.add_argument("--market-ticker")
    capture_market_data.add_argument("--market-tickers", nargs="+")
    capture_market_data.add_argument("--sqlite-path")
    capture_market_data.add_argument("--parquet-dir")
    capture_market_data.add_argument("--max-events", type=int)
    capture_market_data.add_argument("--duration-secs", type=float)
    capture_market_data.add_argument("--receive-timeout", type=float, default=30.0)
    capture_market_data.add_argument("--send-initial-snapshot", action="store_true")
    capture_market_data.add_argument("--include-book-state", action="store_true")
    capture_market_data.add_argument("--public", action="store_true", help="Disable websocket authentication for public channels.")

    capture_orderbook = capture_subparsers.add_parser("orderbook", help="Capture orderbook events for one market.")
    capture_orderbook.add_argument("market_ticker")
    capture_orderbook.add_argument("--sqlite-path")
    capture_orderbook.add_argument("--parquet-dir")
    capture_orderbook.add_argument("--max-events", type=int)
    capture_orderbook.add_argument("--duration-secs", type=float)
    capture_orderbook.add_argument("--receive-timeout", type=float, default=30.0)
    capture_orderbook.add_argument("--include-book-state", action="store_true")

    replay_parser = subparsers.add_parser("replay", help="Inspect stored replay datasets.")
    replay_subparsers = replay_parser.add_subparsers(dest="replay_command", required=True)
    replay_summary = replay_subparsers.add_parser("summary", help="Summarize a stored replay dataset.")
    replay_summary.add_argument("--market-data-source")
    replay_summary.add_argument("--orderbook-source")
    replay_summary.add_argument("--market-ticker")
    replay_summary.add_argument("--channel")
    replay_summary.add_argument("--limit", type=int)
    replay_summary.add_argument("--exclude-replayed-orderbook-levels", action="store_true")
    replay_summary.add_argument("--include-events", action="store_true")

    backtest_parser = subparsers.add_parser("backtest", help="Run historical or replay backtests.")
    backtest_subparsers = backtest_parser.add_subparsers(dest="backtest_command", required=True)
    backtest_historical = backtest_subparsers.add_parser("historical", help="Run a historical-trade backtest.")
    backtest_historical.add_argument("ticker")
    backtest_historical.add_argument("--strategy", required=True)
    backtest_historical.add_argument("--strategy-kwargs")
    backtest_historical.add_argument("--min-ts")
    backtest_historical.add_argument("--max-ts")
    backtest_historical.add_argument("--batch-size", type=int, default=1000)
    backtest_historical.add_argument("--initial-cash-cents", type=int, default=0)
    backtest_historical.add_argument("--initial-yes-position", type=int, default=0)
    backtest_historical.add_argument("--initial-no-position", type=int, default=0)

    backtest_replay = backtest_subparsers.add_parser("replay", help="Run a replay backtest on captured datasets.")
    backtest_replay.add_argument("--market-data-source")
    backtest_replay.add_argument("--orderbook-source")
    backtest_replay.add_argument("--market-ticker")
    backtest_replay.add_argument("--limit", type=int)
    backtest_replay.add_argument("--strategy", required=True)
    backtest_replay.add_argument("--strategy-kwargs")
    backtest_replay.add_argument("--exclude-replayed-orderbook-levels", action="store_true")
    backtest_replay.add_argument("--disable-lifecycle-enrichment", action="store_true")
    backtest_replay.add_argument("--initial-cash-cents", type=int, default=0)
    backtest_replay.add_argument("--initial-yes-position", type=int, default=0)
    backtest_replay.add_argument("--initial-no-position", type=int, default=0)
    backtest_replay.add_argument("--include-market-summaries", action="store_true")

    trading_parser = subparsers.add_parser("trading", help="Inspect or plan live trading workflows.")
    trading_subparsers = trading_parser.add_subparsers(dest="trading_command", required=True)
    trading_snapshot = trading_subparsers.add_parser("snapshot", help="Inspect normalized account state.")
    trading_snapshot.add_argument("--ticker")
    trading_snapshot.add_argument("--event-ticker")
    trading_snapshot.add_argument("--order-status", default="resting")

    trading_market = trading_subparsers.add_parser("market", help="Inspect one market with account context.")
    trading_market.add_argument("market_ticker")
    trading_market.add_argument("--no-orderbook", action="store_true")

    for name in ("plan-order", "replace-order", "flatten", "cancel-stale"):
        sub = trading_subparsers.add_parser(name, help=f"{name.replace('-', ' ').title()} workflow.")
        sub.add_argument("--allow-production-writes", action="store_true")
        sub.add_argument("--max-order-quantity", type=int)
        sub.add_argument("--max-order-risk-cents", type=int)
        sub.add_argument("--max-total-resting-value-cents", type=int)
        sub.add_argument("--max-open-orders-per-market", type=int)
        sub.add_argument("--allowed-ticker", action="append")
        sub.add_argument("--blocked-ticker", action="append")
        sub.add_argument("--audit-log-path")
        sub.add_argument("--execute", action="store_true", help="Actually submit the write instead of dry-run planning.")

    trading_plan = trading_subparsers.choices["plan-order"]
    trading_plan.add_argument("market_ticker")
    trading_plan.add_argument("--action", choices=("buy", "sell"), required=True)
    trading_plan.add_argument("--side", choices=("yes", "no"), required=True)
    trading_plan.add_argument("--quantity", type=int, required=True)
    trading_plan.add_argument("--limit-price-cents", type=int)
    trading_plan.add_argument("--time-in-force")
    trading_plan.add_argument("--expiration-ts")
    trading_plan.add_argument("--client-order-id")
    trading_plan.add_argument("--buy-max-cost-cents", type=int)
    trading_plan.add_argument("--post-only", action="store_true")
    trading_plan.add_argument("--reduce-only", action="store_true")
    trading_plan.add_argument("--self-trade-prevention-type")
    trading_plan.add_argument("--order-group-id")

    trading_replace = trading_subparsers.choices["replace-order"]
    trading_replace.add_argument("order_id")
    trading_replace.add_argument("--quantity", type=int)
    trading_replace.add_argument("--limit-price-cents", type=int)
    trading_replace.add_argument("--client-order-id")

    trading_flatten = trading_subparsers.choices["flatten"]
    trading_flatten.add_argument("market_ticker")
    trading_flatten.add_argument("--limit-price-cents", type=int)
    trading_flatten.add_argument("--client-order-id")

    trading_cancel_stale = trading_subparsers.choices["cancel-stale"]
    trading_cancel_stale.add_argument("--max-age-seconds", type=float, required=True)
    trading_cancel_stale.add_argument("--ticker")
    trading_cancel_stale.add_argument("--event-ticker")

    return parser


def run(argv: Sequence[str] | None = None, *, stdout: io.TextIOBase | None = None, stderr: io.TextIOBase | None = None) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    parser = build_parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
        if args.command == "discover":
            payload = _handle_discover(args)
        elif args.command == "capture":
            payload = _handle_capture(args)
        elif args.command == "replay":
            payload = _handle_replay(args)
        elif args.command == "backtest":
            payload = _handle_backtest(args)
        else:
            payload = _handle_trading(args)
        _emit_json(payload, stdout)
        return 0
    except KeyboardInterrupt:
        stderr.write("Interrupted\n")
        return 130
    except (KalshiError, LookupError, ModuleNotFoundError, ValueError, TypeError, json.JSONDecodeError) as exc:
        stderr.write(f"{type(exc).__name__}: {exc}\n")
        return 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":
    raise SystemExit(main())
