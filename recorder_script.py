#!/usr/bin/env python3
import os
import time
import json
import queue
import random
import threading
from datetime import datetime, timezone, timedelta, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import HTTPError
import pandas as pd
from mykalshi import market, exchange

def get_seconds_until_close():
    now = datetime.now(timezone.utc)  # local time, adjust if your times are in UTC
    day = now.strftime("%A").lower()  # e.g. 'monday'

    schedule = exchange.get_exchange_schedule()
    standard_hours = schedule['schedule']['standard_hours'][0]
    today_sessions = standard_hours.get(day, [])

    # Parse sessions and find the latest close_time
    close_times = []
    for session in today_sessions:
        close_str = session["close_time"]
        close_hour, close_min = map(int, close_str.split(":"))
        close_dt = datetime.combine(now.date(), time(close_hour, close_min))

        # if close time is after midnight, roll to next day
        if close_dt <= now:
            close_dt += timedelta(days=1)

        close_times.append(close_dt)

    if not close_times:
        raise RuntimeError("No trading hours found for today.")

    latest_close = max(close_times)
    duration_secs = (latest_close - now).total_seconds()
    return max(0, duration_secs)

class MarketLOBRecorder:
    def __init__(self,
                 tickers,
                 interval_secs: float = 10.0,
                 max_workers: int = None,
                 max_retries: int = 5,
                 base_backoff: float = 0.1,
                 calls_per_sec: int = 30,
                 output_path: str = "lob_stream.jsonl"):
        self.tickers = tickers
        self.interval_secs = interval_secs
        self.max_retries = max_retries
        self.base_backoff = base_backoff

        self.max_workers = max_workers or min(32, len(tickers))
        self._executor = ThreadPoolExecutor(self.max_workers)

        # rate limiter
        self.min_interval = 1.0 / calls_per_sec
        self._lock = threading.Lock()
        self._last_call = 0.0

        # error tracking
        self.error_counts = {tk: 0 for tk in tickers}

        # disk‐writer
        self._write_q = queue.Queue(maxsize=10000)
        self._out_fh = open(output_path, "w")
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer_thread.start()

    def _writer_loop(self):
        while True:
            rec = self._write_q.get()
            if rec is None:
                break
            self._out_fh.write(json.dumps(rec) + "\n")
            if self._write_q.qsize() < 100:
                self._out_fh.flush()
        self._out_fh.flush()
        self._out_fh.close()

    def _wait_rate_limit(self):
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.time()

    def _fetch_one(self, ticker):
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._wait_rate_limit()
                resp = market.get_market_orderbook(ticker=ticker)
                book = resp.get("orderbook") or {}
                yes_list = book.get("yes") if isinstance(book.get("yes"), list) else []
                no_list  = book.get("no")  if isinstance(book.get("no"),  list) else []

                bids = {int(p): int(sz) for p, sz in yes_list if sz > 0}
                asks = {int(100 - p): int(sz) for p, sz in no_list  if sz > 0}

                if not (yes_list or no_list):
                    raise ValueError("Empty orderbook arrays")

                record = {
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "ticker": ticker,
                    "bids": bids,
                    "asks": asks
                }
                return record

            except HTTPError as http_err:
                code = getattr(http_err.response, "status_code", None)
                if code == 429 and attempt < self.max_retries:
                    delay = self.base_backoff * (2 ** (attempt - 1)) * random.uniform(0.8, 1.2)
                    time.sleep(delay)
                    last_exc = http_err
                    continue
                last_exc = http_err
                break

            except Exception as exc:
                delay = self.base_backoff * random.uniform(0.5, 1.5)
                time.sleep(delay)
                last_exc = exc
                continue

        # retries exhausted
        self.error_counts[ticker] += 1
        return {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "ticker": ticker,
            "bids": {},
            "asks": {},
            "error": repr(last_exc)
        }

    def _fetch_all(self):
        futures = {self._executor.submit(self._fetch_one, tk): tk for tk in self.tickers}
        records = [f.result() for f in as_completed(futures)]
        for rec in records:
            self._write_q.put(rec)
        return records

    def start(self, duration_secs: float):
        end = time.time() + duration_secs
        while time.time() < end:
            cycle_start = time.time()
            self._fetch_all()
            elapsed = time.time() - cycle_start
            time.sleep(max(0, self.interval_secs - elapsed))

        # shutdown writer
        self._write_q.put(None)
        self._writer_thread.join()

        print("Done streaming to disk.")
        print("Errors by ticker:", self.error_counts)


if __name__ == "__main__":
    import time
    from datetime import datetime, date, time as dtime, timezone, timedelta

    # ← your list of tickers
    tickers = [
        'KXRTSMURFS-0','KXRTSMURFS-5','KXRTSMURFS-10','KXRTSMURFS-15',
        'KXRTSMURFS-20','KXRTSMURFS-25','KXRTSMURFS-40','KXRTSMURFS-35',
        'KXRTSMURFS-30','KXRTSMURFS-90','KXRTSMURFS-75','KXRTSMURFS-60',
        'KXRTSMURFS-45'
    ]

    while True:
        # 1) compute seconds until close
        secs_to_close = get_seconds_until_close()
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y%m%d")

        # 2) pick a date-stamped file for this run
        output_file = f"lob_stream_{date_str}.jsonl"
        print(f"\n→ Starting recorder:  will write to {output_file}")
        print(f"  (will run for {secs_to_close/60:.1f} minutes until close)\n")

        # 3) instantiate & run
        rec = MarketLOBRecorder(
            tickers=tickers,
            interval_secs=10.0,                # or whatever you like
            max_workers=min(32, len(tickers)),
            calls_per_sec=30,
            output_path=output_file
        )
        rec.start(duration_secs=secs_to_close)

        # 4) figure out how long until next UTC-midnight
        now = datetime.now(timezone.utc)
        tomorrow = (now + timedelta(days=1)).date()
        next_run = datetime.combine(tomorrow, dtime(0,0), tzinfo=timezone.utc)
        sleep_secs = (next_run - now).total_seconds()
        h = sleep_secs / 3600
        print(f"\n→ Finished today’s run.  Sleeping ~{h:.2f}h until next run at UTC‐midnight…")
        time.sleep(sleep_secs)
