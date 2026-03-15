#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from mykalshi.recorder import MarketLOBRecorder, seconds_until_exchange_close


def main():
    s3 = boto3.client("s3")

    tickers = [
        "KXRTSMURFS-0",
        "KXRTSMURFS-5",
        "KXRTSMURFS-10",
        "KXRTSMURFS-15",
        "KXRTSMURFS-20",
        "KXRTSMURFS-25",
        "KXRTSMURFS-40",
        "KXRTSMURFS-35",
        "KXRTSMURFS-30",
        "KXRTSMURFS-90",
        "KXRTSMURFS-75",
        "KXRTSMURFS-60",
        "KXRTSMURFS-45",
    ]

    interval_secs = 10.0
    max_workers = min(32, len(tickers))
    calls_per_sec = 10
    bucket = "mykalshi-lob-logs"
    s3_prefix = "daily/"

    while True:
        seconds_to_close = seconds_until_exchange_close()
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y%m%d")
        output_file = f"lob_stream_{date_str}.jsonl"

        print(
            f"Recording until close (~{seconds_to_close / 3600:.2f}h), "
            f"writing to {output_file}"
        )

        recorder = MarketLOBRecorder(
            tickers=tickers,
            interval_secs=interval_secs,
            max_workers=max_workers,
            calls_per_sec=calls_per_sec,
            output_path=output_file,
        )
        recorder.start(duration_secs=seconds_to_close)

        key = f"{s3_prefix}{output_file}"
        try:
            print(f"Uploading {output_file} to s3://{bucket}/{key}")
            s3.upload_file(output_file, bucket, key)
            os.remove(output_file)
        except ClientError as exc:
            print(f"Upload failed: {exc}")

        now = datetime.now(timezone.utc)
        tomorrow = (now + timedelta(days=1)).date()
        next_run = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
        sleep_seconds = (next_run - now).total_seconds()
        print(f"Sleeping {sleep_seconds / 3600:.2f}h until the next run")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
