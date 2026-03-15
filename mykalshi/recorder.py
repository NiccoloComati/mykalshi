from __future__ import annotations

import json
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from datetime import datetime, time as dt_time, timedelta, timezone
from zoneinfo import ZoneInfo

from . import exchange, market
from .orderbook import extract_orderbook_levels


def seconds_until_exchange_close(
    *,
    schedule: dict | None = None,
    now: datetime | None = None,
    timezone_name: str = "America/New_York",
) -> float:
    now_utc = now or datetime.now(timezone.utc)
    exchange_tz = ZoneInfo(timezone_name)
    local_now = now_utc.astimezone(exchange_tz)

    if schedule is None:
        schedule = exchange.get_exchange_schedule()

    day_name = local_now.strftime("%A").lower()
    standard_hours = schedule["schedule"]["standard_hours"][0]
    todays_sessions = standard_hours.get(day_name, [])

    close_times_utc = []
    for session in todays_sessions:
        hour, minute = map(int, session["close_time"].split(":"))
        candidate = datetime.combine(local_now.date(), dt_time(hour, minute), tzinfo=exchange_tz)
        if candidate <= local_now:
            candidate += timedelta(days=1)
        close_times_utc.append(candidate.astimezone(timezone.utc))

    if not close_times_utc:
        raise RuntimeError("No exchange close time found for the current day.")

    latest_close_utc = max(close_times_utc)
    return max(0.0, (latest_close_utc - now_utc).total_seconds())


class MarketLOBRecorder:
    def __init__(
        self,
        tickers,
        interval_secs: float = 10.0,
        max_workers: int | None = None,
        max_retries: int = 5,
        base_backoff: float = 0.1,
        calls_per_sec: int = 30,
        output_path: str = "lob_stream.jsonl",
    ):
        self.tickers = list(tickers)
        self.interval_secs = interval_secs
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.max_workers = max_workers or min(32, max(1, len(self.tickers)))
        self.min_interval = 1.0 / calls_per_sec
        self.error_counts = {ticker: 0 for ticker in self.tickers}

        self._executor = ThreadPoolExecutor(self.max_workers)
        self._rate_limit_lock = threading.Lock()
        self._last_call = 0.0
        self._write_queue = queue.Queue(maxsize=10000)
        self._output_handle = open(output_path, "w", encoding="utf-8")
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer_thread.start()

    @staticmethod
    def _decimal_to_number(value: Decimal) -> int | float:
        if value == value.to_integral_value():
            return int(value)
        return float(value)

    def _writer_loop(self):
        while True:
            record = self._write_queue.get()
            if record is None:
                break

            self._output_handle.write(json.dumps(record) + "\n")
            if self._write_queue.qsize() < 100:
                self._output_handle.flush()

        self._output_handle.flush()
        self._output_handle.close()

    def _wait_rate_limit(self):
        with self._rate_limit_lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.time()

    def _fetch_one(self, ticker: str) -> dict:
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                self._wait_rate_limit()
                response = market.get_market_orderbook(ticker=ticker)
                yes_levels, no_levels = extract_orderbook_levels(response)

                if not (yes_levels or no_levels):
                    raise ValueError("Empty orderbook arrays")

                bids = {
                    int(price): self._decimal_to_number(size)
                    for price, size in yes_levels.items()
                    if size > 0
                }
                asks = {
                    int(100 - price): self._decimal_to_number(size)
                    for price, size in no_levels.items()
                    if size > 0
                }

                return {
                    "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    "ticker": ticker,
                    "bids": bids,
                    "asks": asks,
                }
            except Exception as exc:
                last_error = exc
                if attempt < self.max_retries:
                    delay = self.base_backoff * (2 ** (attempt - 1)) * random.uniform(0.8, 1.2)
                    time.sleep(delay)

        self.error_counts[ticker] += 1
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "ticker": ticker,
            "bids": {},
            "asks": {},
            "error": repr(last_error),
        }

    def _fetch_all(self):
        futures = {self._executor.submit(self._fetch_one, ticker): ticker for ticker in self.tickers}
        records = [future.result() for future in as_completed(futures)]
        for record in records:
            self._write_queue.put(record)
        return records

    def start(self, duration_secs: float):
        end_time = time.time() + duration_secs
        while time.time() < end_time:
            cycle_start = time.time()
            self._fetch_all()
            elapsed = time.time() - cycle_start
            time.sleep(max(0.0, self.interval_secs - elapsed))

        self._write_queue.put(None)
        self._writer_thread.join()
