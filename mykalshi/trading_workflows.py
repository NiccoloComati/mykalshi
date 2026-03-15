from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from . import market, trading
from .client import get_default_client
from .config import KalshiEnvironment
from .exceptions import KalshiHTTPError, KalshiSafetyError, KalshiWorkflowError
from .fixed_point import dollars_to_cents, quantize_count, to_decimal
from .formatting import parse_timestamp


ZERO_COUNT = Decimal("0.00")
ONE_HUNDRED_CENTS = 100


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        return datetime.fromisoformat(normalized)
    raise TypeError(f"Unsupported timestamp value: {value!r}")


def _parse_count(value: Any) -> Decimal:
    if value in (None, ""):
        return ZERO_COUNT
    return quantize_count(to_decimal(value))


def _parse_optional_cents(integer_key: str, dollar_key: str, payload: dict[str, Any]) -> int | None:
    if payload.get(integer_key) is not None:
        return int(payload[integer_key])
    if payload.get(dollar_key) is not None:
        return dollars_to_cents(payload[dollar_key])
    return None


def _normalize_quantity(quantity: Any) -> int:
    normalized = to_decimal(quantity)
    if normalized != normalized.to_integral_value():
        raise ValueError("Live order workflows currently require whole-contract quantities")
    count = int(normalized)
    if count <= 0:
        raise ValueError("Order quantity must be positive")
    return count


def _build_price_payload(
    side: str,
    *,
    limit_price_cents: int | None = None,
    limit_price_dollars: str | Decimal | None = None,
) -> dict[str, Any]:
    if limit_price_cents is not None and limit_price_dollars is not None:
        raise ValueError("Specify either limit_price_cents or limit_price_dollars, not both")

    normalized_side = str(side).lower()
    if normalized_side not in {"yes", "no"}:
        raise ValueError(f"Unsupported market side: {side}")

    if limit_price_cents is None and limit_price_dollars is None:
        return {}
    if limit_price_cents is not None:
        if not 1 <= int(limit_price_cents) <= 99:
            raise ValueError("Kalshi limit prices must be between 1 and 99 cents")
        return {f"{normalized_side}_price": int(limit_price_cents)}
    return {f"{normalized_side}_price_dollars": str(limit_price_dollars)}


@dataclass(frozen=True)
class AccountLimitsSnapshot:
    read_limit_per_second: float | None = None
    write_limit_per_second: float | None = None
    usage_tier: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "AccountLimitsSnapshot | None":
        if not payload:
            return None
        return cls(
            read_limit_per_second=float(payload["read_limit"]) if payload.get("read_limit") is not None else None,
            write_limit_per_second=float(payload["write_limit"]) if payload.get("write_limit") is not None else None,
            usage_tier=payload.get("usage_tier"),
            raw=dict(payload),
        )

    def summary(self) -> dict[str, Any]:
        return {
            "read_limit_per_second": self.read_limit_per_second,
            "write_limit_per_second": self.write_limit_per_second,
            "usage_tier": self.usage_tier,
        }


@dataclass(frozen=True)
class BalanceSnapshot:
    balance_cents: int
    portfolio_value_cents: int | None = None
    updated_at: datetime | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BalanceSnapshot":
        return cls(
            balance_cents=int(payload.get("balance", 0)),
            portfolio_value_cents=int(payload["portfolio_value"]) if payload.get("portfolio_value") is not None else None,
            updated_at=_parse_iso_datetime(payload.get("updated_ts")),
            raw=dict(payload),
        )

    def summary(self) -> dict[str, Any]:
        return {
            "balance_cents": self.balance_cents,
            "portfolio_value_cents": self.portfolio_value_cents,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


@dataclass(frozen=True)
class TradingOrder:
    order_id: str
    ticker: str
    side: str
    action: str
    status: str | None
    order_type: str | None
    client_order_id: str | None
    initial_quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal
    yes_price_cents: int | None
    no_price_cents: int | None
    created_at: datetime | None
    updated_at: datetime | None
    expiration_at: datetime | None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TradingOrder":
        return cls(
            order_id=str(payload.get("order_id") or ""),
            ticker=str(payload.get("ticker") or ""),
            side=str(payload.get("side") or "").lower(),
            action=str(payload.get("action") or "").lower(),
            status=payload.get("status"),
            order_type=payload.get("type"),
            client_order_id=payload.get("client_order_id"),
            initial_quantity=_parse_count(payload.get("initial_count_fp") or payload.get("count_fp") or payload.get("count")),
            filled_quantity=_parse_count(payload.get("fill_count_fp")),
            remaining_quantity=_parse_count(payload.get("remaining_count_fp")),
            yes_price_cents=_parse_optional_cents("yes_price", "yes_price_dollars", payload),
            no_price_cents=_parse_optional_cents("no_price", "no_price_dollars", payload),
            created_at=_parse_iso_datetime(payload.get("created_time")),
            updated_at=_parse_iso_datetime(payload.get("last_update_time")),
            expiration_at=_parse_iso_datetime(payload.get("expiration_time")),
            raw=dict(payload),
        )

    @property
    def active_price_cents(self) -> int | None:
        return self.yes_price_cents if self.side == "yes" else self.no_price_cents

    @property
    def is_resting(self) -> bool:
        return (self.status or "").lower() == "resting" and self.remaining_quantity > ZERO_COUNT

    @property
    def estimated_resting_risk_cents(self) -> int | None:
        if not self.is_resting:
            return 0
        price_cents = self.active_price_cents
        if price_cents is None:
            return None
        quantity = int(self.remaining_quantity)
        if self.action == "buy":
            return price_cents * quantity
        return (ONE_HUNDRED_CENTS - price_cents) * quantity

    def summary(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "ticker": self.ticker,
            "side": self.side,
            "action": self.action,
            "status": self.status,
            "order_type": self.order_type,
            "remaining_quantity": format(self.remaining_quantity, ".2f"),
            "price_cents": self.active_price_cents,
        }


@dataclass(frozen=True)
class TradingPosition:
    ticker: str
    signed_quantity: Decimal
    market_exposure_cents: int | None
    realized_pnl_cents: int | None
    fees_paid_cents: int | None
    resting_orders_count: int
    updated_at: datetime | None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TradingPosition":
        return cls(
            ticker=str(payload.get("ticker") or ""),
            signed_quantity=_parse_count(payload.get("position_fp")),
            market_exposure_cents=_parse_optional_cents("market_exposure", "market_exposure_dollars", payload),
            realized_pnl_cents=_parse_optional_cents("realized_pnl", "realized_pnl_dollars", payload),
            fees_paid_cents=_parse_optional_cents("fees_paid", "fees_paid_dollars", payload),
            resting_orders_count=int(payload.get("resting_orders_count") or 0),
            updated_at=_parse_iso_datetime(payload.get("last_updated_ts")),
            raw=dict(payload),
        )

    @property
    def yes_quantity(self) -> Decimal:
        return self.signed_quantity if self.signed_quantity > ZERO_COUNT else ZERO_COUNT

    @property
    def no_quantity(self) -> Decimal:
        return abs(self.signed_quantity) if self.signed_quantity < ZERO_COUNT else ZERO_COUNT

    @property
    def is_flat(self) -> bool:
        return self.signed_quantity == ZERO_COUNT

    def summary(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "signed_quantity": format(self.signed_quantity, ".2f"),
            "market_exposure_cents": self.market_exposure_cents,
            "realized_pnl_cents": self.realized_pnl_cents,
            "fees_paid_cents": self.fees_paid_cents,
            "resting_orders_count": self.resting_orders_count,
        }


@dataclass(frozen=True)
class TradingSnapshot:
    balance: BalanceSnapshot
    limits: AccountLimitsSnapshot | None
    orders: tuple[TradingOrder, ...]
    positions: tuple[TradingPosition, ...]
    event_positions: tuple[dict[str, Any], ...]
    resting_order_value_cents: int | None
    captured_at: datetime = field(default_factory=_utcnow)

    def position_for(self, ticker: str) -> TradingPosition | None:
        for position in self.positions:
            if position.ticker == ticker:
                return position
        return None

    def orders_for(self, ticker: str, *, status: str | None = None) -> list[TradingOrder]:
        normalized_status = status.lower() if status else None
        return [
            order
            for order in self.orders
            if order.ticker == ticker and (normalized_status is None or (order.status or "").lower() == normalized_status)
        ]

    def resting_orders_for(self, ticker: str | None = None) -> list[TradingOrder]:
        return [
            order
            for order in self.orders
            if order.is_resting and (ticker is None or order.ticker == ticker)
        ]

    def computed_resting_order_value_cents(self) -> int | None:
        total = 0
        for order in self.resting_orders_for():
            risk_cents = order.estimated_resting_risk_cents
            if risk_cents is None:
                return None
            total += risk_cents
        return total

    def effective_resting_order_value_cents(self) -> int | None:
        if self.resting_order_value_cents is not None:
            return self.resting_order_value_cents
        return self.computed_resting_order_value_cents()

    def resting_exposure_by_market(self) -> dict[str, int | None]:
        exposure: dict[str, int | None] = {}
        grouped: dict[str, list[TradingOrder]] = defaultdict(list)
        for order in self.resting_orders_for():
            grouped[order.ticker].append(order)
        for ticker, orders in grouped.items():
            total = 0
            unknown = False
            for order in orders:
                risk_cents = order.estimated_resting_risk_cents
                if risk_cents is None:
                    unknown = True
                    break
                total += risk_cents
            exposure[ticker] = None if unknown else total
        return exposure

    def summary(self) -> dict[str, Any]:
        return {
            "captured_at": self.captured_at.isoformat(),
            "balance_cents": self.balance.balance_cents,
            "portfolio_value_cents": self.balance.portfolio_value_cents,
            "resting_order_count": len(self.resting_orders_for()),
            "resting_order_value_cents": self.effective_resting_order_value_cents(),
            "open_position_count": len([position for position in self.positions if not position.is_flat]),
            "read_limit_per_second": self.limits.read_limit_per_second if self.limits else None,
            "write_limit_per_second": self.limits.write_limit_per_second if self.limits else None,
        }


@dataclass(frozen=True)
class MarketContextSnapshot:
    market_ticker: str
    account: TradingSnapshot
    market: dict[str, Any] | None
    orderbook: dict[str, Any] | None

    def summary(self) -> dict[str, Any]:
        return {
            "market_ticker": self.market_ticker,
            "account": self.account.summary(),
            "market_status": (self.market or {}).get("status"),
            "has_orderbook": self.orderbook is not None,
        }


@dataclass(frozen=True)
class OrderIntent:
    ticker: str
    action: str
    side: str
    quantity: int | str | Decimal = 1
    limit_price_cents: int | None = None
    limit_price_dollars: str | Decimal | None = None
    time_in_force: str | None = None
    expiration_ts: int | str | None = None
    client_order_id: str | None = None
    buy_max_cost_cents: int | None = None
    post_only: bool | None = None
    reduce_only: bool | None = None
    self_trade_prevention_type: str | None = None
    order_group_id: str | None = None
    cancel_order_on_pause: bool | None = None
    subaccount: int | None = None

    def __post_init__(self) -> None:
        normalized_action = self.action.lower()
        normalized_side = self.side.lower()
        if normalized_action not in {"buy", "sell"}:
            raise ValueError(f"Unsupported order action: {self.action}")
        if normalized_side not in {"yes", "no"}:
            raise ValueError(f"Unsupported order side: {self.side}")
        _normalize_quantity(self.quantity)

    @property
    def order_type(self) -> str:
        if self.limit_price_cents is not None or self.limit_price_dollars is not None:
            return "limit"
        return "market"

    @property
    def estimated_risk_cents(self) -> int | None:
        quantity = _normalize_quantity(self.quantity)
        price_cents = self.limit_price_cents
        if price_cents is None and self.limit_price_dollars is not None:
            price_cents = dollars_to_cents(self.limit_price_dollars)
        if self.action.lower() == "buy":
            if self.buy_max_cost_cents is not None:
                return int(self.buy_max_cost_cents)
            if price_cents is None:
                return None
            return int(price_cents) * quantity
        if price_cents is None:
            return None
        return (ONE_HUNDRED_CENTS - int(price_cents)) * quantity

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ticker": self.ticker,
            "action": self.action.lower(),
            "side": self.side.lower(),
            "count": _normalize_quantity(self.quantity),
            "type": self.order_type,
        }
        payload.update(
            _build_price_payload(
                self.side,
                limit_price_cents=self.limit_price_cents,
                limit_price_dollars=self.limit_price_dollars,
            )
        )
        if self.time_in_force is not None:
            payload["time_in_force"] = self.time_in_force
        if self.expiration_ts is not None:
            payload["expiration_ts"] = parse_timestamp(self.expiration_ts)
        if self.client_order_id is not None:
            payload["client_order_id"] = self.client_order_id
        if self.buy_max_cost_cents is not None:
            payload["buy_max_cost"] = int(self.buy_max_cost_cents)
        if self.post_only is not None:
            payload["post_only"] = bool(self.post_only)
        if self.reduce_only is not None:
            payload["reduce_only"] = bool(self.reduce_only)
        if self.self_trade_prevention_type is not None:
            payload["self_trade_prevention_type"] = self.self_trade_prevention_type
        if self.order_group_id is not None:
            payload["order_group_id"] = self.order_group_id
        if self.cancel_order_on_pause is not None:
            payload["cancel_order_on_pause"] = bool(self.cancel_order_on_pause)
        if self.subaccount is not None:
            payload["subaccount"] = int(self.subaccount)
        return payload

    def summary(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "action": self.action.lower(),
            "side": self.side.lower(),
            "quantity": _normalize_quantity(self.quantity),
            "order_type": self.order_type,
            "estimated_risk_cents": self.estimated_risk_cents,
            "limit_price_cents": self.limit_price_cents,
            "client_order_id": self.client_order_id,
            "reduce_only": self.reduce_only,
        }


@dataclass(frozen=True)
class TradingSafetyPolicy:
    allow_production_writes: bool = False
    dry_run: bool = False
    max_order_quantity: int | None = None
    max_order_risk_cents: int | None = None
    max_total_resting_value_cents: int | None = None
    max_open_orders_per_market: int | None = None
    allowed_tickers: tuple[str, ...] = ()
    blocked_tickers: tuple[str, ...] = ()
    audit_log_path: str | None = None


@dataclass(frozen=True)
class SafetyCheckResult:
    ok: bool
    reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    estimated_risk_cents: int | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "reasons": list(self.reasons),
            "warnings": list(self.warnings),
            "estimated_risk_cents": self.estimated_risk_cents,
        }


@dataclass(frozen=True)
class TradingActionResult:
    operation: str
    status: str
    dry_run: bool
    request_payload: dict[str, Any] | None = None
    response_payload: dict[str, Any] | int | None = None
    message: str | None = None
    order_id: str | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "status": self.status,
            "dry_run": self.dry_run,
            "order_id": self.order_id,
            "message": self.message,
        }


@dataclass(frozen=True)
class TradingWorkflowResult:
    operation: str
    dry_run: bool
    steps: tuple[TradingActionResult, ...]
    snapshot: TradingSnapshot | None = None
    safety: SafetyCheckResult | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "dry_run": self.dry_run,
            "step_count": len(self.steps),
            "steps": [step.summary() for step in self.steps],
            "safety": self.safety.summary() if self.safety else None,
            "snapshot": self.snapshot.summary() if self.snapshot else None,
        }


class TradingSession:
    """User-facing helper for safer trading state snapshots and execution workflows."""

    def __init__(
        self,
        *,
        policy: TradingSafetyPolicy | None = None,
        clock: Any | None = None,
    ) -> None:
        self.policy = policy or TradingSafetyPolicy()
        self._clock = clock or _utcnow

    @property
    def client(self):
        return get_default_client()

    @property
    def environment(self) -> KalshiEnvironment:
        return self.client.config.environment

    def _audit(self, event_type: str, payload: dict[str, Any]) -> None:
        if not self.policy.audit_log_path:
            return
        path = Path(self.policy.audit_log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "event_type": event_type,
            "recorded_at": self._clock().isoformat(),
            "environment": self.environment.value,
            **payload,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str, sort_keys=True))
            handle.write("\n")

    def _fetch_orders(
        self,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        status: str | Iterable[str] | None = "resting",
        page_size: int = 100,
        max_items: int = 500,
    ) -> list[TradingOrder]:
        if status is None:
            statuses = [None]
        elif isinstance(status, str):
            statuses = [status]
        else:
            statuses = list(status)

        seen: set[str] = set()
        results: list[TradingOrder] = []
        for order_status in statuses:
            cursor = None
            while len(results) < max_items:
                remaining = max_items - len(results)
                response = trading.get_orders(
                    ticker=ticker,
                    event_ticker=event_ticker,
                    status=order_status,
                    limit=min(page_size, remaining),
                    cursor=cursor,
                )
                batch = [TradingOrder.from_payload(item) for item in response.get("orders", [])]
                for order in batch:
                    if order.order_id in seen:
                        continue
                    seen.add(order.order_id)
                    results.append(order)
                cursor = response.get("cursor")
                if not cursor or not batch:
                    break
        return results

    def _fetch_positions(
        self,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        page_size: int = 100,
        max_items: int = 500,
    ) -> tuple[list[TradingPosition], list[dict[str, Any]]]:
        cursor = None
        positions: list[TradingPosition] = []
        event_positions: list[dict[str, Any]] = []
        while len(positions) < max_items:
            remaining = max_items - len(positions)
            response = trading.get_positions(
                ticker=ticker,
                event_ticker=event_ticker,
                settlement_status="unsettled",
                limit=min(page_size, remaining),
                cursor=cursor,
            )
            batch = [TradingPosition.from_payload(item) for item in response.get("market_positions", [])]
            positions.extend(batch)
            event_positions.extend(response.get("event_positions", []))
            cursor = response.get("cursor")
            if not cursor or not batch:
                break
        return positions, event_positions

    def snapshot(
        self,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        order_status: str | Iterable[str] | None = "resting",
        page_size: int = 100,
        max_items: int = 500,
    ) -> TradingSnapshot:
        balance_payload = trading.get_balance()
        limits_payload = trading.get_account_limits()
        orders = self._fetch_orders(
            ticker=ticker,
            event_ticker=event_ticker,
            status=order_status,
            page_size=page_size,
            max_items=max_items,
        )
        positions, event_positions = self._fetch_positions(
            ticker=ticker,
            event_ticker=event_ticker,
            page_size=page_size,
            max_items=max_items,
        )

        resting_order_value_cents: int | None = None
        try:
            resting_payload = trading.get_total_resting_order_value()
        except KalshiHTTPError as exc:
            if exc.status_code != 403:
                raise
        else:
            if isinstance(resting_payload, dict):
                resting_order_value_cents = _parse_optional_cents(
                    "total_resting_order_value",
                    "total_resting_order_value_dollars",
                    resting_payload,
                )

        return TradingSnapshot(
            balance=BalanceSnapshot.from_payload(balance_payload),
            limits=AccountLimitsSnapshot.from_payload(limits_payload),
            orders=tuple(orders),
            positions=tuple(positions),
            event_positions=tuple(event_positions),
            resting_order_value_cents=resting_order_value_cents,
            captured_at=self._clock(),
        )

    def market_snapshot(self, ticker: str, *, include_orderbook: bool = True) -> MarketContextSnapshot:
        account = self.snapshot(ticker=ticker)
        market_payload = market.get_market(ticker)
        orderbook_payload = market.get_market_orderbook(ticker) if include_orderbook else None
        return MarketContextSnapshot(
            market_ticker=ticker,
            account=account,
            market=market_payload.get("market") if isinstance(market_payload, dict) else None,
            orderbook=orderbook_payload,
        )

    def _evaluate_safety(
        self,
        intent: OrderIntent,
        snapshot: TradingSnapshot,
        *,
        dry_run: bool,
    ) -> SafetyCheckResult:
        reasons: list[str] = []
        warnings: list[str] = []
        estimated_risk_cents = intent.estimated_risk_cents

        if self.environment is KalshiEnvironment.PRODUCTION and not self.policy.allow_production_writes and not dry_run:
            reasons.append(
                "Production writes are blocked by default. Set TradingSafetyPolicy(allow_production_writes=True) "
                "or use dry_run=True."
            )

        if self.policy.allowed_tickers and intent.ticker not in self.policy.allowed_tickers:
            reasons.append(f"Ticker {intent.ticker} is not in the allowed_tickers policy")
        if self.policy.blocked_tickers and intent.ticker in self.policy.blocked_tickers:
            reasons.append(f"Ticker {intent.ticker} is blocked by the safety policy")

        quantity = _normalize_quantity(intent.quantity)
        if self.policy.max_order_quantity is not None and quantity > self.policy.max_order_quantity:
            reasons.append(
                f"Order quantity {quantity} exceeds max_order_quantity={self.policy.max_order_quantity}"
            )

        if self.policy.max_open_orders_per_market is not None:
            open_orders = len(snapshot.resting_orders_for(intent.ticker))
            if open_orders >= self.policy.max_open_orders_per_market:
                reasons.append(
                    f"Ticker {intent.ticker} already has {open_orders} resting orders, "
                    f"at or above max_open_orders_per_market={self.policy.max_open_orders_per_market}"
                )

        if self.policy.max_order_risk_cents is not None:
            if estimated_risk_cents is None:
                reasons.append(
                    "Unable to estimate maximum order risk for this order, but max_order_risk_cents is enforced"
                )
            elif estimated_risk_cents > self.policy.max_order_risk_cents:
                reasons.append(
                    f"Estimated order risk {estimated_risk_cents}c exceeds "
                    f"max_order_risk_cents={self.policy.max_order_risk_cents}"
                )

        if self.policy.max_total_resting_value_cents is not None:
            existing_resting = snapshot.effective_resting_order_value_cents()
            if existing_resting is None or estimated_risk_cents is None:
                reasons.append(
                    "Unable to evaluate total resting exposure against max_total_resting_value_cents"
                )
            elif (existing_resting + estimated_risk_cents) > self.policy.max_total_resting_value_cents:
                reasons.append(
                    f"Projected resting exposure {existing_resting + estimated_risk_cents}c exceeds "
                    f"max_total_resting_value_cents={self.policy.max_total_resting_value_cents}"
                )

        if intent.order_type == "market":
            warnings.append("Market orders can fill at prices worse than the current book")
        if intent.action == "sell" and not intent.reduce_only:
            warnings.append("Sell orders without reduce_only can open new short-equivalent exposure on Kalshi")
        if estimated_risk_cents is None:
            warnings.append("Estimated order risk is unknown for this order")

        return SafetyCheckResult(
            ok=not reasons,
            reasons=tuple(reasons),
            warnings=tuple(warnings),
            estimated_risk_cents=estimated_risk_cents,
        )

    def submit_order(
        self,
        intent: OrderIntent,
        *,
        dry_run: bool | None = None,
        snapshot: TradingSnapshot | None = None,
    ) -> TradingWorkflowResult:
        effective_dry_run = self.policy.dry_run or bool(dry_run)
        account_snapshot = snapshot or self.snapshot(ticker=intent.ticker)
        safety = self._evaluate_safety(intent, account_snapshot, dry_run=effective_dry_run)
        if not safety.ok:
            self._audit(
                "submit_order_blocked",
                {
                    "intent": intent.summary(),
                    "safety": safety.summary(),
                },
            )
            raise KalshiSafetyError("; ".join(safety.reasons))

        payload = intent.to_payload()
        if effective_dry_run:
            result = TradingActionResult(
                operation="submit_order",
                status="planned",
                dry_run=True,
                request_payload=payload,
                message="Dry-run only; no order was submitted",
            )
            self._audit(
                "submit_order_planned",
                {
                    "intent": intent.summary(),
                    "safety": safety.summary(),
                    "request_payload": payload,
                },
            )
            return TradingWorkflowResult(
                operation="submit_order",
                dry_run=True,
                steps=(result,),
                snapshot=account_snapshot,
                safety=safety,
            )

        response = trading.create_order(payload)
        order_id = None
        if isinstance(response, dict):
            order_id = response.get("order_id") or (response.get("order") or {}).get("order_id")
        result = TradingActionResult(
            operation="submit_order",
            status="executed",
            dry_run=False,
            request_payload=payload,
            response_payload=response if isinstance(response, dict) else None,
            order_id=order_id,
        )
        self._audit(
            "submit_order_executed",
            {
                "intent": intent.summary(),
                "safety": safety.summary(),
                "request_payload": payload,
                "response_payload": response if isinstance(response, dict) else response,
            },
        )
        return TradingWorkflowResult(
            operation="submit_order",
            dry_run=False,
            steps=(result,),
            snapshot=account_snapshot,
            safety=safety,
        )

    def buy_yes(self, ticker: str, quantity=1, **kwargs: Any) -> TradingWorkflowResult:
        dry_run = kwargs.pop("dry_run", None)
        snapshot = kwargs.pop("snapshot", None)
        return self.submit_order(
            OrderIntent(ticker=ticker, action="buy", side="yes", quantity=quantity, **kwargs),
            dry_run=dry_run,
            snapshot=snapshot,
        )

    def buy_no(self, ticker: str, quantity=1, **kwargs: Any) -> TradingWorkflowResult:
        dry_run = kwargs.pop("dry_run", None)
        snapshot = kwargs.pop("snapshot", None)
        return self.submit_order(
            OrderIntent(ticker=ticker, action="buy", side="no", quantity=quantity, **kwargs),
            dry_run=dry_run,
            snapshot=snapshot,
        )

    def sell_yes(self, ticker: str, quantity=1, **kwargs: Any) -> TradingWorkflowResult:
        dry_run = kwargs.pop("dry_run", None)
        snapshot = kwargs.pop("snapshot", None)
        return self.submit_order(
            OrderIntent(ticker=ticker, action="sell", side="yes", quantity=quantity, **kwargs),
            dry_run=dry_run,
            snapshot=snapshot,
        )

    def sell_no(self, ticker: str, quantity=1, **kwargs: Any) -> TradingWorkflowResult:
        dry_run = kwargs.pop("dry_run", None)
        snapshot = kwargs.pop("snapshot", None)
        return self.submit_order(
            OrderIntent(ticker=ticker, action="sell", side="no", quantity=quantity, **kwargs),
            dry_run=dry_run,
            snapshot=snapshot,
        )

    def cancel_order(self, order_id: str, *, dry_run: bool | None = None) -> TradingWorkflowResult:
        effective_dry_run = self.policy.dry_run or bool(dry_run)
        if effective_dry_run:
            result = TradingActionResult(
                operation="cancel_order",
                status="planned",
                dry_run=True,
                request_payload={"order_id": order_id},
                order_id=order_id,
                message="Dry-run only; no order was cancelled",
            )
            self._audit("cancel_order_planned", {"order_id": order_id})
            return TradingWorkflowResult(operation="cancel_order", dry_run=True, steps=(result,))

        response = trading.cancel_order(order_id)
        result = TradingActionResult(
            operation="cancel_order",
            status="executed",
            dry_run=False,
            request_payload={"order_id": order_id},
            response_payload=response if isinstance(response, dict) else None,
            order_id=order_id,
        )
        self._audit("cancel_order_executed", {"order_id": order_id, "response_payload": response})
        return TradingWorkflowResult(operation="cancel_order", dry_run=False, steps=(result,))

    def cancel_stale_orders(
        self,
        *,
        max_age_seconds: float,
        ticker: str | None = None,
        event_ticker: str | None = None,
        age_field: str = "created_at",
        dry_run: bool | None = None,
        page_size: int = 100,
        max_items: int = 500,
    ) -> TradingWorkflowResult:
        effective_dry_run = self.policy.dry_run or bool(dry_run)
        now = self._clock()
        stale_before = now - timedelta(seconds=max_age_seconds)
        steps: list[TradingActionResult] = []
        for order in self._fetch_orders(
            ticker=ticker,
            event_ticker=event_ticker,
            status="resting",
            page_size=page_size,
            max_items=max_items,
        ):
            order_time = getattr(order, age_field, None)
            if order_time is None or order_time > stale_before:
                continue
            if effective_dry_run:
                steps.append(
                    TradingActionResult(
                        operation="cancel_order",
                        status="planned",
                        dry_run=True,
                        request_payload={"order_id": order.order_id},
                        order_id=order.order_id,
                        message="Dry-run only; stale order identified for cancellation",
                    )
                )
            else:
                response = trading.cancel_order(order.order_id)
                steps.append(
                    TradingActionResult(
                        operation="cancel_order",
                        status="executed",
                        dry_run=False,
                        request_payload={"order_id": order.order_id},
                        response_payload=response if isinstance(response, dict) else None,
                        order_id=order.order_id,
                    )
                )

        self._audit(
            "cancel_stale_orders",
            {
                "ticker": ticker,
                "event_ticker": event_ticker,
                "max_age_seconds": max_age_seconds,
                "dry_run": effective_dry_run,
                "cancelled_order_ids": [step.order_id for step in steps if step.order_id],
            },
        )
        return TradingWorkflowResult(
            operation="cancel_stale_orders",
            dry_run=effective_dry_run,
            steps=tuple(steps),
        )

    def amend_resting_order(
        self,
        order_id: str,
        *,
        quantity: Any | None = None,
        limit_price_cents: int | None = None,
        limit_price_dollars: str | Decimal | None = None,
        updated_client_order_id: str | None = None,
        dry_run: bool | None = None,
    ) -> TradingWorkflowResult:
        effective_dry_run = self.policy.dry_run or bool(dry_run)
        if self.environment is KalshiEnvironment.PRODUCTION and not self.policy.allow_production_writes and not effective_dry_run:
            raise KalshiSafetyError(
                "Production order amendments are blocked by default. "
                "Set TradingSafetyPolicy(allow_production_writes=True) or use dry_run=True."
            )

        existing = TradingOrder.from_payload(trading.get_order(order_id).get("order", {}))
        if not existing.order_id:
            raise KalshiWorkflowError(f"Could not load order {order_id} for amendment")
        if not existing.is_resting:
            raise KalshiWorkflowError(f"Order {order_id} is not resting and cannot be amended")

        payload = {
            "ticker": existing.ticker,
            "action": existing.action,
            "side": existing.side,
            "count": _normalize_quantity(quantity) if quantity is not None else int(existing.remaining_quantity),
            "client_order_id": existing.client_order_id,
        }
        payload.update(
            _build_price_payload(
                existing.side,
                limit_price_cents=limit_price_cents if limit_price_cents is not None else existing.active_price_cents,
                limit_price_dollars=limit_price_dollars,
            )
        )
        if updated_client_order_id is not None:
            payload["updated_client_order_id"] = updated_client_order_id

        if effective_dry_run:
            result = TradingActionResult(
                operation="amend_order",
                status="planned",
                dry_run=True,
                request_payload=payload,
                order_id=existing.order_id,
                message="Dry-run only; no order amendment was submitted",
            )
            self._audit("amend_order_planned", {"order_id": existing.order_id, "request_payload": payload})
            return TradingWorkflowResult(operation="amend_order", dry_run=True, steps=(result,))

        response = trading.amend_order(existing.order_id, **payload)
        result = TradingActionResult(
            operation="amend_order",
            status="executed",
            dry_run=False,
            request_payload=payload,
            response_payload=response if isinstance(response, dict) else None,
            order_id=existing.order_id,
        )
        self._audit(
            "amend_order_executed",
            {"order_id": existing.order_id, "request_payload": payload, "response_payload": response},
        )
        return TradingWorkflowResult(operation="amend_order", dry_run=False, steps=(result,))

    def replace_order(
        self,
        order_id: str,
        *,
        quantity: Any | None = None,
        limit_price_cents: int | None = None,
        limit_price_dollars: str | Decimal | None = None,
        client_order_id: str | None = None,
        dry_run: bool | None = None,
        **intent_overrides: Any,
    ) -> TradingWorkflowResult:
        effective_dry_run = self.policy.dry_run or bool(dry_run)
        existing = TradingOrder.from_payload(trading.get_order(order_id).get("order", {}))
        if not existing.order_id:
            raise KalshiWorkflowError(f"Could not load order {order_id} for replacement")

        replacement_intent = OrderIntent(
            ticker=existing.ticker,
            action=existing.action,
            side=existing.side,
            quantity=quantity if quantity is not None else int(existing.remaining_quantity),
            limit_price_cents=limit_price_cents if limit_price_cents is not None else existing.active_price_cents,
            limit_price_dollars=limit_price_dollars,
            client_order_id=client_order_id,
            **intent_overrides,
        )

        steps: list[TradingActionResult] = []
        cancel_result = self.cancel_order(existing.order_id, dry_run=effective_dry_run)
        steps.extend(cancel_result.steps)
        submit_result = self.submit_order(replacement_intent, dry_run=effective_dry_run)
        steps.extend(submit_result.steps)
        self._audit(
            "replace_order",
            {
                "old_order_id": existing.order_id,
                "new_intent": replacement_intent.summary(),
                "dry_run": effective_dry_run,
            },
        )
        return TradingWorkflowResult(
            operation="replace_order",
            dry_run=effective_dry_run,
            steps=tuple(steps),
            safety=submit_result.safety,
        )

    def flatten_market(
        self,
        ticker: str,
        *,
        limit_price_cents: int | None = None,
        limit_price_dollars: str | Decimal | None = None,
        client_order_id: str | None = None,
        dry_run: bool | None = None,
    ) -> TradingWorkflowResult:
        snapshot = self.snapshot(ticker=ticker)
        position = snapshot.position_for(ticker)
        if position is None or position.is_flat:
            step = TradingActionResult(
                operation="flatten_market",
                status="skipped",
                dry_run=self.policy.dry_run or bool(dry_run),
                request_payload={"ticker": ticker},
                message="No open position to flatten",
            )
            return TradingWorkflowResult(
                operation="flatten_market",
                dry_run=self.policy.dry_run or bool(dry_run),
                steps=(step,),
                snapshot=snapshot,
            )

        if position.signed_quantity > ZERO_COUNT:
            intent = OrderIntent(
                ticker=ticker,
                action="sell",
                side="yes",
                quantity=int(position.yes_quantity),
                limit_price_cents=limit_price_cents,
                limit_price_dollars=limit_price_dollars,
                client_order_id=client_order_id,
                reduce_only=True,
            )
        else:
            intent = OrderIntent(
                ticker=ticker,
                action="sell",
                side="no",
                quantity=int(position.no_quantity),
                limit_price_cents=limit_price_cents,
                limit_price_dollars=limit_price_dollars,
                client_order_id=client_order_id,
                reduce_only=True,
            )
        return self.submit_order(intent, dry_run=dry_run, snapshot=snapshot)


__all__ = [
    "AccountLimitsSnapshot",
    "BalanceSnapshot",
    "MarketContextSnapshot",
    "OrderIntent",
    "SafetyCheckResult",
    "TradingActionResult",
    "TradingOrder",
    "TradingPosition",
    "TradingSafetyPolicy",
    "TradingSession",
    "TradingSnapshot",
    "TradingWorkflowResult",
]
