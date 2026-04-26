# 2026-04-26: SPY-Like Market Notebook

## What changed

- Added `scripts/build_spy_like_markets_notebook.py` to generate a focused research notebook for SPY-like Kalshi markets.
- Added `notebooks/spy_like_markets.ipynb` as the generated notebook artifact.
- The notebook:
  - bootstraps the repo and `.venv` notebook environment like the current main notebook
  - discovers broad equity-index value markets using the current `ResearchSession` discovery surface
  - filters out unrelated S&P constituent add/remove contracts
  - pulls live market metadata, market-history candles, trade history, and optional authenticated order books
  - classifies included markets into distribution-relevant archetypes like terminal range bins, terminal tails, annual-max ladders, annual-min ladders, and standalone terminal threshold checkpoints
  - builds probability-distribution-ready tables for terminal-close partitions plus annual max/min threshold curves and bucketized approximations
  - exports per-market CSV/JSON artifacts under `data/spy_like_market_pull/`

## Why

- The user wanted a small current-code notebook that can pull all available data for prediction markets tied to SPY or closely related broad equity indexes.
- The existing main notebook is broader and exploratory; this new notebook gives a dedicated repeatable path for index-value market pulls.
