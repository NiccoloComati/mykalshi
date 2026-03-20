from __future__ import annotations

import unittest

from mykalshi.formatting import parse_timestamp


class FormattingTests(unittest.TestCase):
    def test_parse_timestamp_accepts_iso8601_strings(self):
        parsed = parse_timestamp("2026-03-20T12:34:56+00:00")
        self.assertIsInstance(parsed, int)
        self.assertGreater(parsed, 0)


if __name__ == "__main__":
    unittest.main()
