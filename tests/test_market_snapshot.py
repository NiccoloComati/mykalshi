from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from mykalshi import market


class GetAllMarketsTests(unittest.TestCase):
    def test_get_all_markets_passes_incremental_filters(self):
        captured: dict[str, object] = {}

        def fake_collect(fetch_page, item_key, max_items):
            captured["item_key"] = item_key
            captured["max_items"] = max_items
            fetch_page("cursor-123")
            return []

        with patch("mykalshi.market.collect_cursor_pages", side_effect=fake_collect), patch(
            "mykalshi.market.get_markets", return_value={"markets": [], "cursor": None}
        ) as mocked_get_markets:
            market.get_all_markets(
                status="open",
                batch_size=250,
                max_items=500,
                min_updated_ts="03/18/2026 10:00:00",
                series_ticker="PRES",
            )

        self.assertEqual(captured["item_key"], "markets")
        self.assertEqual(captured["max_items"], 500)
        mocked_get_markets.assert_called_once()
        self.assertEqual(mocked_get_markets.call_args.kwargs["limit"], 250)
        self.assertEqual(mocked_get_markets.call_args.kwargs["cursor"], "cursor-123")
        self.assertEqual(mocked_get_markets.call_args.kwargs["status"], "open")
        self.assertEqual(mocked_get_markets.call_args.kwargs["series_ticker"], "PRES")
        self.assertIsNotNone(mocked_get_markets.call_args.kwargs["min_updated_ts"])


class MarketSnapshotSyncTests(unittest.TestCase):
    def test_sync_market_snapshot_csv_full_refresh_creates_anchor(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "all_markets_2026-03-18-10-00-00.csv"

            def fake_get_markets(**kwargs):
                self.assertEqual(kwargs["limit"], 1000)
                self.assertIsNone(kwargs["cursor"])
                return {
                    "markets": [
                        {"ticker": "TICKER-1", "status": "open", "yes_bid": 45},
                        {"ticker": "TICKER-2", "status": "open", "yes_bid": 55},
                    ],
                    "cursor": None,
                }

            with patch("mykalshi.market.get_markets", side_effect=fake_get_markets), patch(
                "mykalshi.market._normalize_market_rows", side_effect=lambda rows: rows
            ):
                result = market.sync_market_snapshot_csv(snapshot_path)

            self.assertEqual(result["mode"], "full_refresh")
            self.assertEqual(result["market_count"], 2)
            self.assertTrue(snapshot_path.exists())
            self.assertTrue(result["anchor_path"].exists())
            frame = pd.read_csv(snapshot_path, low_memory=False)
            self.assertEqual(sorted(frame["ticker"].tolist()), ["TICKER-1", "TICKER-2"])
            anchor = json.loads(result["anchor_path"].read_text(encoding="utf-8"))
            self.assertEqual(anchor["market_count"], 2)

    def test_sync_market_snapshot_csv_incremental_refresh_merges_delta(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "all_markets_2026-03-18-10-00-00.csv"
            anchor_path = market.default_market_snapshot_anchor_path(snapshot_path)
            pd.DataFrame(
                [
                    {"ticker": "TICKER-1", "status": "open", "yes_bid": 10},
                    {"ticker": "TICKER-2", "status": "open", "yes_bid": 20},
                ]
            ).to_csv(snapshot_path, index=False)
            anchor_path.write_text(
                json.dumps(
                    {
                        "snapshot_path": str(snapshot_path),
                        "snapshot_created_at": datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc).isoformat(),
                        "snapshot_cursor_ts": int(datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc).timestamp()),
                        "market_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            def fake_get_markets(**kwargs):
                self.assertEqual(kwargs["cursor"], None)
                self.assertEqual(kwargs["min_updated_ts"], int(datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc).timestamp()))
                return {
                    "markets": [
                        {"ticker": "TICKER-2", "status": "open", "yes_bid": 25},
                        {"ticker": "TICKER-3", "status": "closed", "yes_bid": 30},
                    ],
                    "cursor": None,
                }

            with patch("mykalshi.market.get_markets", side_effect=fake_get_markets), patch(
                "mykalshi.market._normalize_market_rows", side_effect=lambda rows: rows
            ):
                result = market.sync_market_snapshot_csv(snapshot_path)

            self.assertEqual(result["mode"], "incremental_refresh")
            self.assertEqual(result["delta_count"], 2)
            frame = pd.read_csv(snapshot_path, low_memory=False).sort_values("ticker").reset_index(drop=True)
            self.assertEqual(frame["ticker"].tolist(), ["TICKER-1", "TICKER-2", "TICKER-3"])
            self.assertEqual(frame.loc[frame["ticker"] == "TICKER-2", "yes_bid"].iloc[0], 25)
            self.assertEqual(frame.loc[frame["ticker"] == "TICKER-3", "status"].iloc[0], "closed")

    def test_sync_market_snapshot_csv_bootstraps_anchor_from_filename(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "all_markets_2026-03-17-09-30-00.csv"
            pd.DataFrame([{"ticker": "TICKER-1", "status": "open", "yes_bid": 10}]).to_csv(snapshot_path, index=False)

            def fake_get_markets(**kwargs):
                self.assertEqual(kwargs["min_updated_ts"], int(datetime(2026, 3, 17, 9, 30, 0, tzinfo=timezone.utc).timestamp()))
                return {"markets": [], "cursor": None}

            with patch("mykalshi.market.get_markets", side_effect=fake_get_markets):
                result = market.sync_market_snapshot_csv(snapshot_path)

            self.assertEqual(result["mode"], "incremental_refresh_bootstrap_anchor")
            self.assertEqual(result["delta_count"], 0)
            self.assertTrue(result["anchor_path"].exists())


if __name__ == "__main__":
    unittest.main()
