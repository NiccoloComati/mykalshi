from __future__ import annotations

import threading
import time


DEFAULT_READ_LIMIT_PER_SECOND = 20.0
DEFAULT_WRITE_LIMIT_PER_SECOND = 10.0


class KalshiRateLimiter:
    """Simple deterministic limiter for Kalshi read/write request buckets."""

    def __init__(
        self,
        *,
        read_limit_per_second: float = DEFAULT_READ_LIMIT_PER_SECOND,
        write_limit_per_second: float = DEFAULT_WRITE_LIMIT_PER_SECOND,
        clock=None,
        sleeper=None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._lock = threading.Lock()
        self._next_allowed = {"read": 0.0, "write": 0.0}
        self.read_limit_per_second = DEFAULT_READ_LIMIT_PER_SECOND
        self.write_limit_per_second = DEFAULT_WRITE_LIMIT_PER_SECOND
        self.update_limits(
            read_limit_per_second=read_limit_per_second,
            write_limit_per_second=write_limit_per_second,
        )

    @staticmethod
    def _normalize_limit(limit: float) -> float:
        return max(float(limit), 0.001)

    def update_limits(
        self,
        *,
        read_limit_per_second: float | None = None,
        write_limit_per_second: float | None = None,
    ) -> None:
        with self._lock:
            if read_limit_per_second is not None:
                self.read_limit_per_second = self._normalize_limit(read_limit_per_second)
            if write_limit_per_second is not None:
                self.write_limit_per_second = self._normalize_limit(write_limit_per_second)

    def acquire(self, bucket: str, *, weight: float = 1.0) -> float:
        normalized_bucket = "write" if bucket == "write" else "read"
        normalized_weight = max(float(weight), 0.0)
        if normalized_weight == 0.0:
            return 0.0

        with self._lock:
            limit = self.write_limit_per_second if normalized_bucket == "write" else self.read_limit_per_second
            interval = normalized_weight / limit
            now = self._clock()
            ready_at = max(now, self._next_allowed[normalized_bucket])
            self._next_allowed[normalized_bucket] = ready_at + interval

        sleep_seconds = max(0.0, ready_at - now)
        if sleep_seconds > 0:
            self._sleeper(sleep_seconds)
        return sleep_seconds

    def snapshot(self) -> dict[str, float]:
        return {
            "read_limit_per_second": self.read_limit_per_second,
            "write_limit_per_second": self.write_limit_per_second,
        }
