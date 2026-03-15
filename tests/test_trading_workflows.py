from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from mykalshi.config import KalshiConfig, KalshiEnvironment
from mykalshi.exceptions import KalshiHTTPError, KalshiSafetyError
from mykalshi.trading_workflows import TradingSafetyPolicy, TradingSession


@dataclass
class ClientStub:
    config: KalshiConfig


def make_order(
    *,
    order_id: str = "order-1",
    ticker: str = "TEST-YES",
    action: str = "buy",
    side: str = "yes",
    status: str = "resting",
    count: str = "2.00",
    filled: str = "0.00",
    remaining: str = "2.00",
    price_cents: int = 40,
    created_time: str = "2026-03-15T11:00:00Z",
    client_order_id: str = "cid-1",
) -> dict:
    payload = {
        "order_id": order_id,
        "ticker": ticker,
        "action": action,
        "side": side,
        "status": status,
        "type": "limit",
        "client_order_id": client_order_id,
        "initial_count_fp": count,
        "fill_count_fp": filled,
        "remaining_count_fp": remaining,
        "created_time": created_time,
        "last_update_time": created_time,
        "expiration_time": None,
    }
    payload[f"{side}_price"] = price_cents
    return payload


def make_position(
    *,
    ticker: str = "TEST-YES",
    signed_quantity: str = "2.00",
) -> dict:
    return {
        "ticker": ticker,
        "position_fp": signed_quantity,
        "market_exposure_dollars": "0.8000",
        "realized_pnl_dollars": "0.1200",
        "fees_paid_dollars": "0.0100",
        "resting_orders_count": 0,
        "last_updated_ts": "2026-03-15T11:05:00Z",
    }


class TradingWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)

    def _install_base_patches(
        self,
        stack: ExitStack,
        *,
        environment: KalshiEnvironment = KalshiEnvironment.DEMO,
        orders: list[dict] | None = None,
        positions: list[dict] | None = None,
        resting_payload: dict | None = None,
        resting_side_effect: Exception | None = None,
    ) -> None:
        stack.enter_context(
            patch(
                "mykalshi.trading_workflows.get_default_client",
                return_value=ClientStub(KalshiConfig(environment=environment)),
            )
        )
        stack.enter_context(
            patch(
                "mykalshi.trading_workflows.trading.get_balance",
                return_value={"balance": 2500, "portfolio_value": 3200, "updated_ts": "2026-03-15T11:59:00Z"},
            )
        )
        stack.enter_context(
            patch(
                "mykalshi.trading_workflows.trading.get_account_limits",
                return_value={"read_limit": 20, "write_limit": 10, "usage_tier": "basic"},
            )
        )
        stack.enter_context(
            patch(
                "mykalshi.trading_workflows.trading.get_orders",
                return_value={"orders": orders or [], "cursor": ""},
            )
        )
        stack.enter_context(
            patch(
                "mykalshi.trading_workflows.trading.get_positions",
                return_value={"market_positions": positions or [], "event_positions": [], "cursor": ""},
            )
        )
        if resting_side_effect is not None:
            stack.enter_context(
                patch(
                    "mykalshi.trading_workflows.trading.get_total_resting_order_value",
                    side_effect=resting_side_effect,
                )
            )
        else:
            stack.enter_context(
                patch(
                    "mykalshi.trading_workflows.trading.get_total_resting_order_value",
                    return_value=resting_payload or {"total_resting_order_value_dollars": "0.0000"},
                )
            )

    def test_snapshot_normalizes_orders_positions_and_falls_back_to_computed_resting_value(self):
        orders = [
            make_order(order_id="buy-resting", action="buy", side="yes", price_cents=40, count="2.00"),
            make_order(
                order_id="sell-resting",
                ticker="TEST-NO",
                action="sell",
                side="no",
                price_cents=70,
                count="1.00",
                remaining="1.00",
            ),
        ]
        positions = [make_position()]

        with ExitStack() as stack:
            self._install_base_patches(
                stack,
                orders=orders,
                positions=positions,
                resting_side_effect=KalshiHTTPError(403, "GET", "/portfolio/summary/total_resting_order_value", "blocked"),
            )
            snapshot = TradingSession(clock=lambda: self.now).snapshot()

        self.assertEqual(snapshot.balance.balance_cents, 2500)
        self.assertEqual(snapshot.limits.write_limit_per_second, 10.0)
        self.assertEqual(snapshot.position_for("TEST-YES").yes_quantity, 2)
        self.assertEqual(snapshot.effective_resting_order_value_cents(), 110)
        self.assertEqual(snapshot.resting_exposure_by_market()["TEST-YES"], 80)

    def test_submit_order_blocks_production_writes_without_override(self):
        with ExitStack() as stack:
            self._install_base_patches(stack, environment=KalshiEnvironment.PRODUCTION)
            session = TradingSession(clock=lambda: self.now)
            with self.assertRaises(KalshiSafetyError):
                session.buy_yes("TEST-YES", quantity=1, limit_price_cents=40)

    def test_submit_order_dry_run_plans_without_mutation(self):
        with ExitStack() as stack:
            self._install_base_patches(stack, environment=KalshiEnvironment.PRODUCTION)
            mocked_create = stack.enter_context(
                patch("mykalshi.trading_workflows.trading.create_order")
            )
            result = TradingSession(clock=lambda: self.now).buy_yes(
                "TEST-YES",
                quantity=1,
                limit_price_cents=40,
                dry_run=True,
            )

        mocked_create.assert_not_called()
        self.assertTrue(result.dry_run)
        self.assertEqual(result.steps[0].request_payload["yes_price"], 40)

    def test_max_open_orders_per_market_blocks_new_submit(self):
        with ExitStack() as stack:
            self._install_base_patches(
                stack,
                orders=[make_order(order_id="resting-1")],
            )
            session = TradingSession(
                policy=TradingSafetyPolicy(max_open_orders_per_market=1),
                clock=lambda: self.now,
            )
            with self.assertRaises(KalshiSafetyError):
                session.buy_yes("TEST-YES", quantity=1, limit_price_cents=40)

    def test_cancel_stale_orders_only_cancels_old_orders(self):
        old_order = make_order(order_id="old-order", created_time="2026-03-15T10:00:00Z")
        fresh_order = make_order(order_id="fresh-order", created_time="2026-03-15T11:59:30Z")

        with ExitStack() as stack:
            self._install_base_patches(stack, orders=[old_order, fresh_order])
            mocked_cancel = stack.enter_context(
                patch("mykalshi.trading_workflows.trading.cancel_order", return_value={})
            )
            result = TradingSession(clock=lambda: self.now).cancel_stale_orders(max_age_seconds=60)

        mocked_cancel.assert_called_once_with("old-order")
        self.assertEqual([step.order_id for step in result.steps], ["old-order"])

    def test_amend_resting_order_uses_current_order_shape(self):
        order_payload = make_order(order_id="order-1", price_cents=40, count="2.00", remaining="2.00")
        with ExitStack() as stack:
            self._install_base_patches(stack)
            stack.enter_context(
                patch("mykalshi.trading_workflows.trading.get_order", return_value={"order": order_payload})
            )
            mocked_amend = stack.enter_context(
                patch("mykalshi.trading_workflows.trading.amend_order", return_value={"order_id": "order-1"})
            )
            TradingSession(clock=lambda: self.now).amend_resting_order(
                "order-1",
                quantity=3,
                limit_price_cents=41,
                updated_client_order_id="cid-2",
            )

        mocked_amend.assert_called_once_with(
            "order-1",
            ticker="TEST-YES",
            action="buy",
            side="yes",
            count=3,
            client_order_id="cid-1",
            yes_price=41,
            updated_client_order_id="cid-2",
        )

    def test_replace_order_cancels_then_submits_new_order(self):
        order_payload = make_order(order_id="order-1", price_cents=40, count="2.00", remaining="2.00")
        with ExitStack() as stack:
            self._install_base_patches(stack)
            stack.enter_context(
                patch("mykalshi.trading_workflows.trading.get_order", return_value={"order": order_payload})
            )
            mocked_cancel = stack.enter_context(
                patch("mykalshi.trading_workflows.trading.cancel_order", return_value={})
            )
            mocked_create = stack.enter_context(
                patch("mykalshi.trading_workflows.trading.create_order", return_value={"order_id": "order-2"})
            )
            result = TradingSession(clock=lambda: self.now).replace_order(
                "order-1",
                limit_price_cents=39,
                client_order_id="cid-2",
            )

        mocked_cancel.assert_called_once_with("order-1")
        mocked_create.assert_called_once_with(
            {
                "ticker": "TEST-YES",
                "action": "buy",
                "side": "yes",
                "count": 2,
                "type": "limit",
                "yes_price": 39,
                "client_order_id": "cid-2",
            }
        )
        self.assertEqual([step.operation for step in result.steps], ["cancel_order", "submit_order"])

    def test_flatten_market_uses_sell_yes_for_positive_positions(self):
        with ExitStack() as stack:
            self._install_base_patches(stack, positions=[make_position(signed_quantity="3.00")])
            mocked_create = stack.enter_context(
                patch("mykalshi.trading_workflows.trading.create_order", return_value={"order_id": "flat-1"})
            )
            TradingSession(
                policy=TradingSafetyPolicy(allow_production_writes=True),
                clock=lambda: self.now,
            ).flatten_market("TEST-YES", limit_price_cents=60)

        mocked_create.assert_called_once_with(
            {
                "ticker": "TEST-YES",
                "action": "sell",
                "side": "yes",
                "count": 3,
                "type": "limit",
                "yes_price": 60,
                "reduce_only": True,
            }
        )

    def test_flatten_market_uses_sell_no_for_negative_positions_in_dry_run(self):
        with ExitStack() as stack:
            self._install_base_patches(stack, positions=[make_position(signed_quantity="-4.00")])
            result = TradingSession(clock=lambda: self.now).flatten_market(
                "TEST-YES",
                limit_price_cents=58,
                dry_run=True,
            )

        self.assertEqual(
            result.steps[0].request_payload,
            {
                "ticker": "TEST-YES",
                "action": "sell",
                "side": "no",
                "count": 4,
                "type": "limit",
                "no_price": 58,
                "reduce_only": True,
            },
        )

    def test_audit_log_writes_jsonl_records(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            audit_path = Path(tmpdir) / "audit.jsonl"
            with ExitStack() as stack:
                self._install_base_patches(stack, environment=KalshiEnvironment.PRODUCTION)
                session = TradingSession(
                    policy=TradingSafetyPolicy(dry_run=True, audit_log_path=str(audit_path)),
                    clock=lambda: self.now,
                )
                session.buy_yes("TEST-YES", quantity=1, limit_price_cents=40)

            records = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(records[0]["event_type"], "submit_order_planned")
            self.assertEqual(records[0]["environment"], "production")


if __name__ == "__main__":
    unittest.main()
