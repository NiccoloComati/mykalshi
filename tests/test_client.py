from __future__ import annotations

import unittest

from mykalshi.client import KalshiClient
from mykalshi.config import KalshiConfig


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text="") -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text or ("" if payload is None else str(payload))
        self.content = b"" if payload is None else b"{}"

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def request(self, **kwargs):
        self.calls.append(kwargs)
        if not self._responses:
            raise AssertionError("No fake response queued for session.request")
        return self._responses.pop(0)


class FakeSigner:
    def sign_headers(self, method, path, base_url):
        return {"X-Signed": f"{method}:{path}:{base_url}"}


class KalshiClientTests(unittest.TestCase):
    def test_retries_429_using_retry_after_header(self):
        sleeps = []
        session = FakeSession(
            [
                FakeResponse(
                    status_code=429,
                    payload={"error": {"code": "too_many_requests"}},
                    headers={"Retry-After": "1.5"},
                    text="too many requests",
                ),
                FakeResponse(payload={"markets": []}),
            ]
        )
        client = KalshiClient(
            KalshiConfig(enable_rate_limiting=False, max_rate_limit_retries=1),
            session=session,
            sleeper=sleeps.append,
        )

        response = client.get("/markets")

        self.assertEqual(response, {"markets": []})
        self.assertEqual(sleeps, [1.5])
        self.assertEqual(len(session.calls), 2)

    def test_auto_detect_account_limits_updates_rate_limiter(self):
        session = FakeSession(
            [
                FakeResponse(payload={"read_limit": 30, "write_limit": 40}),
                FakeResponse(payload={"balance": {"balance": "100.00"}}),
            ]
        )
        client = KalshiClient(
            KalshiConfig(
                api_key_id="key-id",
                private_key_path="ignored.pem",
                enable_rate_limiting=True,
                auto_detect_account_limits=True,
                read_limit_per_second=20.0,
                write_limit_per_second=10.0,
            ),
            signer=FakeSigner(),
            session=session,
        )

        client.get("/portfolio/balance", authenticated=True)

        self.assertEqual(session.calls[0]["url"], "https://api.elections.kalshi.com/trade-api/v2/account/limits")
        self.assertEqual(session.calls[1]["url"], "https://api.elections.kalshi.com/trade-api/v2/portfolio/balance")
        self.assertEqual(
            client.rate_limiter.snapshot(),
            {"read_limit_per_second": 30.0, "write_limit_per_second": 40.0},
        )

    def test_weighted_batch_cancel_requests_are_paced_by_write_limit(self):
        current_time = [0.0]
        sleeps = []

        def clock():
            return current_time[0]

        def sleeper(seconds):
            sleeps.append(seconds)
            current_time[0] += seconds

        session = FakeSession([FakeResponse(), FakeResponse()])
        client = KalshiClient(
            KalshiConfig(
                api_key_id="key-id",
                private_key_path="ignored.pem",
                enable_rate_limiting=True,
                auto_detect_account_limits=False,
                write_limit_per_second=10.0,
            ),
            signer=FakeSigner(),
            session=session,
            clock=clock,
            sleeper=sleeper,
        )

        client.delete(
            "/portfolio/orders/batched",
            json_body={"ids": ["1", "2", "3", "4", "5"]},
            authenticated=True,
        )
        client.delete(
            "/portfolio/orders/batched",
            json_body={"ids": ["1", "2", "3", "4", "5"]},
            authenticated=True,
        )

        self.assertEqual(len(session.calls), 2)
        self.assertEqual(len(sleeps), 1)
        self.assertAlmostEqual(sleeps[0], 0.1, places=6)


if __name__ == "__main__":
    unittest.main()
