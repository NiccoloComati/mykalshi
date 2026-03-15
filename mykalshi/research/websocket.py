from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Sequence

from ..auth import KalshiAuthSigner
from ..config import KalshiConfig
from ..exceptions import KalshiAuthenticationError, KalshiDependencyError
from ..fixed_point import dollars_to_cents
from ..orderbook import OrderbookState


@dataclass(frozen=True)
class SubscriptionRequest:
    channels: Sequence[str]
    market_ticker: str | None = None
    market_tickers: Sequence[str] | None = None
    market_id: str | None = None
    market_ids: Sequence[str] | None = None
    send_initial_snapshot: bool | None = None
    skip_ticker_ack: bool | None = None
    shard_factor: int | None = None
    shard_key: str | None = None

    def to_payload(self, request_id: int) -> dict[str, Any]:
        params: dict[str, Any] = {
            "channels": list(self.channels),
        }
        if self.market_ticker is not None:
            params["market_ticker"] = self.market_ticker
        if self.market_tickers is not None:
            params["market_tickers"] = list(self.market_tickers)
        if self.market_id is not None:
            params["market_id"] = self.market_id
        if self.market_ids is not None:
            params["market_ids"] = list(self.market_ids)
        if self.send_initial_snapshot is not None:
            params["send_initial_snapshot"] = self.send_initial_snapshot
        if self.skip_ticker_ack is not None:
            params["skip_ticker_ack"] = self.skip_ticker_ack
        if self.shard_factor is not None:
            params["shard_factor"] = self.shard_factor
        if self.shard_key is not None:
            params["shard_key"] = self.shard_key

        return {
            "id": request_id,
            "cmd": "subscribe",
            "params": params,
        }


def build_orderbook_event(
    message: dict[str, Any],
    state: OrderbookState,
    *,
    captured_at: str | None = None,
    include_book_state: bool = False,
) -> dict[str, Any]:
    msg = message.get("msg", {})
    event_type = message.get("type")
    is_snapshot = event_type == "orderbook_snapshot"
    include_levels = is_snapshot or include_book_state
    return {
        "captured_at": captured_at or datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "event_type": event_type,
        "channel": "orderbook_delta",
        "subscription_id": message.get("sid"),
        "sequence": message.get("seq"),
        "market_ticker": msg.get("market_ticker") or state.market_ticker,
        "market_id": msg.get("market_id") or state.market_id,
        "event_ts": msg.get("ts"),
        "side": msg.get("side"),
        "price_cents": dollars_to_cents(msg["price_dollars"]) if msg.get("price_dollars") is not None else None,
        "delta_fp": msg.get("delta_fp"),
        "best_yes_bid_cents": state.best_yes_bid_cents,
        "best_yes_ask_cents": state.best_yes_ask_cents,
        "best_no_bid_cents": state.best_no_bid_cents,
        "best_no_ask_cents": state.best_no_ask_cents,
        "yes_levels": state.serialized_yes_levels() if include_levels else None,
        "no_levels": state.serialized_no_levels() if include_levels else None,
        "raw_message": message,
    }


class KalshiWebsocketClient:
    def __init__(
        self,
        config: KalshiConfig | None = None,
        *,
        signer: KalshiAuthSigner | Any | None = None,
        websocket_connect: Any | None = None,
        open_timeout: float = 15.0,
        close_timeout: float = 5.0,
    ) -> None:
        self.config = config or KalshiConfig.from_env()
        self._signer = signer
        self._websocket_connect = websocket_connect
        self.open_timeout = open_timeout
        self.close_timeout = close_timeout

    @property
    def signer(self) -> KalshiAuthSigner | Any | None:
        if self._signer is not None:
            return self._signer
        if self.config.api_key_id and self.config.private_key_path:
            self._signer = KalshiAuthSigner(
                api_key_id=self.config.api_key_id,
                private_key_path=self.config.private_key_path,
            )
            return self._signer
        return None

    def _get_websocket_connect(self) -> Any:
        if self._websocket_connect is not None:
            return self._websocket_connect

        try:
            import websockets
        except ImportError as exc:
            raise KalshiDependencyError(
                "websockets is required for streaming Kalshi market data"
            ) from exc

        self._websocket_connect = websockets.connect
        return self._websocket_connect

    def build_headers(self, *, authenticated: bool = True) -> dict[str, str]:
        headers = {"User-Agent": self.config.user_agent}
        if not authenticated:
            return headers

        signer = self.signer
        if signer is None:
            raise KalshiAuthenticationError(
                "WebSocket authentication requested, but no Kalshi credentials were configured."
            )
        headers.update(signer.sign_headers("GET", path=self.config.resolved_ws_url))
        return headers

    async def iter_messages(
        self,
        subscription: SubscriptionRequest,
        *,
        request_id: int = 1,
        max_messages: int | None = None,
        duration_secs: float | None = None,
        receive_timeout: float = 30.0,
        authenticated: bool = True,
    ) -> AsyncIterator[dict[str, Any]]:
        connect = self._get_websocket_connect()
        deadline = time.monotonic() + duration_secs if duration_secs is not None else None
        message_count = 0

        async with connect(
            self.config.resolved_ws_url,
            additional_headers=self.build_headers(authenticated=authenticated),
            open_timeout=self.open_timeout,
            close_timeout=self.close_timeout,
        ) as websocket:
            await websocket.send(json.dumps(subscription.to_payload(request_id)))

            while True:
                if max_messages is not None and message_count >= max_messages:
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    break

                timeout = receive_timeout
                if deadline is not None:
                    timeout = min(timeout, max(0.0, deadline - time.monotonic()))
                    if timeout <= 0:
                        break

                try:
                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break

                message_count += 1
                yield json.loads(raw_message)

    async def iter_orderbook_events(
        self,
        market_ticker: str,
        *,
        max_events: int | None = None,
        duration_secs: float | None = None,
        receive_timeout: float = 30.0,
        include_book_state: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        state = OrderbookState()
        events_emitted = 0
        subscription = SubscriptionRequest(
            channels=["orderbook_delta"],
            market_ticker=market_ticker,
            send_initial_snapshot=True,
        )
        async for message in self.iter_messages(
            subscription,
            duration_secs=duration_secs,
            receive_timeout=receive_timeout,
        ):
            message_type = message.get("type")
            if message_type == "orderbook_snapshot":
                state.apply_snapshot(message)
            elif message_type == "orderbook_delta":
                state.apply_delta(message)
            else:
                continue

            yield build_orderbook_event(
                message,
                state,
                include_book_state=include_book_state,
            )
            events_emitted += 1
            if max_events is not None and events_emitted >= max_events:
                break

    async def capture_orderbook(
        self,
        market_ticker: str,
        *,
        sink: Any | None = None,
        max_events: int | None = None,
        duration_secs: float | None = None,
        receive_timeout: float = 30.0,
        include_book_state: bool = False,
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        async for event in self.iter_orderbook_events(
            market_ticker,
            max_events=max_events,
            duration_secs=duration_secs,
            receive_timeout=receive_timeout,
            include_book_state=include_book_state,
        ):
            events.append(event)
            if sink is not None:
                sink.write_orderbook_event(event)

        if sink is not None and hasattr(sink, "flush"):
            sink.flush()
        return events

    def capture_orderbook_sync(
        self,
        market_ticker: str,
        *,
        sink: Any | None = None,
        max_events: int | None = None,
        duration_secs: float | None = None,
        receive_timeout: float = 30.0,
        include_book_state: bool = False,
    ) -> list[dict[str, Any]]:
        return asyncio.run(
            self.capture_orderbook(
                market_ticker,
                sink=sink,
                max_events=max_events,
                duration_secs=duration_secs,
                receive_timeout=receive_timeout,
                include_book_state=include_book_state,
            )
        )
