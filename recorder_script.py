#!/usr/bin/env python3
import os
import time                  # for sleep()
import json
import queue
import random
import threading
from datetime import datetime, timezone, timedelta, time as dttime
from datetime import time as dttime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import HTTPError
import pandas as pd
from mykalshi import market, exchange
import boto3
from botocore.exceptions import ClientError
from zoneinfo import ZoneInfo

def get_seconds_until_close():
    # 1) current instant in UTC
    now_utc = datetime.now(timezone.utc)

    # 2) what is "now" in New York?
    exch_tz   = ZoneInfo("America/New_York")
    now_local = now_utc.astimezone(exch_tz)

    # 3) fetch the schedule; assume it's keyed by weekday names
    day          = now_local.strftime("%A").lower()
    sched        = exchange.get_exchange_schedule()["schedule"]["standard_hours"][0]
    today_times  = sched.get(day, [])

    close_times = []
    for session in today_times:
        # parse the ET close_time (e.g. "03:00", "15:00")
        hr, mn = map(int, session["close_time"].split(":"))
        # build a localized datetime for today at that hour
        cand = datetime.combine(now_local.date(), dttime(hr, mn), tzinfo=exch_tz)

        # if that time has already passed in local, roll to tomorrow
        if cand <= now_local:
            cand += timedelta(days=1)

        # convert back to UTC
        close_times.append(cand.astimezone(timezone.utc))

    if not close_times:
        raise RuntimeError("No trading hours found for today in schedule.")

    latest_close_utc = max(close_times)
    return (latest_close_utc - now_utc).total_seconds()

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
    from datetime import datetime, timezone, timedelta
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3")

    # ← your list of tickers
    tickers = [
        'KXRTSMURFS-0','KXRTSMURFS-5','KXRTSMURFS-10','KXRTSMURFS-15',
        'KXRTSMURFS-20','KXRTSMURFS-25','KXRTSMURFS-40','KXRTSMURFS-35',
        'KXRTSMURFS-30','KXRTSMURFS-90','KXRTSMURFS-75','KXRTSMURFS-60',
        'KXRTSMURFS-45'
    ]
    
    # ──────────  PARAMETERS  ──────────
    # your actual production settings:
    INTERVAL_SECS  =   10.0           # seconds between snapshots
    MAX_WORKERS    =   min(32, len(tickers))
    CALLS_PER_SEC  =   10             # must match your API tier
    BUCKET         =   "mykalshi-lob-logs"
    S3_PREFIX      =   "daily/"
    # ───────────────────────────────────

    while True:
        # 1) compute seconds until close
        secs_to_close = get_seconds_until_close()
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y%m%d")

        # 2) name today’s file
        output_file = f"lob_stream_{date_str}.jsonl"
        print(f"▶ Recording until close (~{secs_to_close/3600:.2f}h), writing → {output_file}")

        # 3) run the recorder exactly once
        rec = MarketLOBRecorder(
            tickers=tickers,
            interval_secs=INTERVAL_SECS,
            max_workers=MAX_WORKERS,
            calls_per_sec=CALLS_PER_SEC,
            output_path=output_file
        )
        rec.start(duration_secs=secs_to_close)

        # 4) upload to S3 & delete locally
        key = f"{S3_PREFIX}{output_file}"
        try:
            print(f"Uploading {output_file} → s3://{BUCKET}/{key}")
            s3.upload_file(output_file, BUCKET, key)
            print("  ✔ upload succeeded, deleting local file")
            os.remove(output_file)
        except ClientError as e:
            print("  ! upload failed:", e)

        # 5) sleep until next UTC-midnight, then exit (systemd will restart it)
        now = datetime.now(timezone.utc)
        tomorrow = (now + timedelta(days=1)).date()
        next_run = datetime.combine(tomorrow, time(0,0), tzinfo=timezone.utc)
        sleep_secs = (next_run - now).total_seconds()
        print(f"Sleeping {sleep_secs/3600:.2f}h until tomorrow’s run…")
        time.sleep(sleep_secs)