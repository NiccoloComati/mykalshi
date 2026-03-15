from __future__ import annotations

import sys

from mykalshi import historical
from mykalshi.research import TradeBacktester, TradeSignal


def strategy(context, trade):
    if context.yes_position == 0:
        return TradeSignal("buy_yes", quantity=1)
    return None


def main() -> None:
    ticker = sys.argv[1] if len(sys.argv) > 1 else historical.get_historical_trades(limit=1)["trades"][0]["ticker"]
    result = TradeBacktester().run_on_historical_trades(
        ticker,
        strategy,
        initial_cash_cents=10000,
    )
    print("historical_ticker:", ticker)
    print(result.summary())


if __name__ == "__main__":
    main()
