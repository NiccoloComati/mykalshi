# Backtest Engine Architecture

Date: 2026-03-15

## Diagnosis Of The Old Engine

Before this refactor, the backtest path in `mykalshi/research/backtest.py` had a few real limits:

- market replay, strategy logic, order handling, execution, portfolio accounting, and reporting all lived in one procedural loop
- the strategy contract was tied directly to raw historical trade dictionaries
- the engine was trade-print-driven rather than event-driven, so there was no clean place for orderbook updates, cancellations, partial fills, settlement events, or end-of-step marks
- order state transitions were implicit instead of modeled as explicit events
- extending the engine toward realistic Kalshi microstructure would have required repeatedly editing the same monolithic file

## Target Architecture

The current target architecture is deliberately modeled around separate components:

- `mykalshi/research/engine/replay.py`
  - converts historical trades or stored market-data events into ordered engine events
- `mykalshi/research/engine/events.py`
  - defines explicit market, order, fill, cancellation, mark, and settlement events
  - owns `MarketState`
- `mykalshi/research/engine/strategy.py`
  - provides the user-facing strategy API and `StrategyContext`
- `mykalshi/research/engine/orders.py`
  - owns simulated order state and order lifecycle transitions
- `mykalshi/research/engine/execution.py`
  - owns the fill model and deterministic execution decisions
- `mykalshi/research/engine/portfolio.py`
  - owns positions, cash, realized PnL, and valuation
- `mykalshi/research/engine/reporting.py`
  - owns marks, logs, event logs, and final result packaging
- `mykalshi/research/engine/core.py`
  - owns the event loop and orchestrates all state transitions

## First Implementation Pass

This pass adds:

- a deterministic event loop through `EventDrivenBacktestEngine`
- historical-trade replay and generic market-data replay
- explicit order submission and cancellation requests
- explicit order events and fill events
- partial fill handling
- mark-to-market updates after each processed market event
- settlement event support in the state model
- a simple Backtrader-style strategy surface with methods like:
  - `buy_yes(...)`
  - `sell_yes(...)`
  - `buy_no(...)`
  - `sell_no(...)`
  - `cancel(order_id)`
  - `position(...)`
  - `market(...)`
  - `open_orders(...)`

## What Is Implemented vs Later

Implemented now:

- clean module boundaries
- deterministic event processing
- event logs
- trade-driven and quote/book-driven replay entry points
- order acceptance, cancellation, partial fills, and fill callbacks
- cash and inventory reservation for resting orders
- explicit cancel/replace semantics through deterministic request ordering
- centralized settlement handling that cancels open orders, waits on missing payouts, and realizes binary payouts once data arrives
- portfolio/accounting for Kalshi yes/no contracts
- migration of the legacy `TradeBacktester` surface onto the event-driven engine

Later work:

- queue-position and maker-style execution assumptions
- richer settlement sourcing from Kalshi metadata
- market-family and multi-market portfolio analytics
- performance reporting beyond the current summary metrics
- more orderbook-aware execution models beyond the current immediate compatibility shim
