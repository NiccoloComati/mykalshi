# Backtest Fee + Queue Finalization Pass

Date: 2026-03-15

## Summary

This completion slice finished the key remaining gaps from the execution-realism upgrade by wiring liquidity-role-aware fees and improving passive queue depletion signals from replayed market data.

## Main Changes

- updated fee plumbing in `EventDrivenBacktestEngine` so fee models can receive `liquidity_role` (`aggressive`/`passive`) while preserving backward-compatible fee-model call signatures
- added `KalshiMakerTakerFeeModel` in `mykalshi/research/backtest.py` and exported it via `mykalshi.research`
- improved queue-ahead depletion in `MarketState` using:
  - trade prints at the resting price
  - orderbook level-size reductions at the resting price
  - ticker top-of-book size reductions when price is unchanged
- tightened passive queue initialization so ticker-only replays can seed queue-ahead from top-of-book size when full depth is unavailable
- kept legacy wrapper behavior stable by treating legacy fills as aggressive and preserving old fee-model compatibility paths
- expanded tests for:
  - liquidity-role-aware fee differentiation
  - orderbook-size-drop queue depletion
  - ticker-size-drop queue depletion
  - maker/taker fee model compatibility on `TradeBacktester`

## Verification

- `python -m unittest tests.test_research_event_engine tests.test_research_backtest -v`
- `python -m unittest discover -s tests -v`

## Remaining Limits

- queue behavior is still an approximation and does not model full per-order matching priority or hidden liquidity
- ticker-based queue depletion only applies when price levels are unchanged and size fields are present
