from __future__ import annotations

import json
import textwrap
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = ROOT / "notebooks" / "spy_like_markets.ipynb"


def src(text: str) -> list[str]:
    stripped = textwrap.dedent(text).strip("\n")
    return [line + "\n" for line in stripped.splitlines()] if stripped else []


def markdown_cell(text: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": src(text),
    }


def code_cell(text: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": src(text),
    }


def _cell_text(cell: dict) -> str:
    return "".join(cell.get("source", []))


def _marker_present(nb: dict, marker: str) -> bool:
    return any(marker in _cell_text(cell) for cell in nb.get("cells", []))


def _append_unique_cells(nb: dict, additions: list[tuple[str, dict]]) -> tuple[dict, int]:
    updated = deepcopy(nb)
    appended = 0
    for marker, cell in additions:
        if _marker_present(updated, marker):
            continue
        updated.setdefault("cells", []).append(cell)
        appended += 1
    return updated, appended


def _load_existing_notebook() -> dict:
    if NOTEBOOK_PATH.exists():
        return json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    raise FileNotFoundError(
        f"{NOTEBOOK_PATH} does not exist. "
        "This updater is append-only and will not create a replacement notebook from scratch."
    )


def planned_additions() -> list[tuple[str, dict]]:
    return [
        (
            "## Provenance Rules",
            markdown_cell(
                """
                ## Provenance Rules

                This notebook separates:

                - **Raw Kalshi contracts and fields**: contracts, strike bounds, event titles, bid/ask/last prices,
                  order book snapshots, trades, candles, open interest, and volume
                - **Notebook-derived views**: contract classification, midpoint probabilities, normalized partition
                  probabilities, monotonicity checks, and bucketized approximations
                """
            ),
        ),
        (
            "display(provenance_df)",
            code_cell(
                """
                display(provenance_df)
                """
            ),
        ),
        (
            "display(raw_contract_catalog_df[raw_contract_columns])",
            code_cell(
                """
                raw_contract_columns = [
                    "underlying",
                    "resolution_measure",
                    "kalshi_market_logic",
                    "series_ticker",
                    "event_ticker",
                    "market_ticker",
                    "strike_type",
                    "lower_bound",
                    "upper_bound",
                    "kalshi_bid_prob_proxy",
                    "kalshi_ask_prob_proxy",
                    "kalshi_price_prob_proxy",
                    "market_title",
                    "market_subtitle",
                    "raw_from_kalshi",
                    "derived_in_notebook",
                ]
                display(raw_contract_catalog_df[raw_contract_columns])
                """
            ),
        ),
        (
            'title="S&P 500 terminal-close distribution implied by Kalshi bins"',
            code_cell(
                """
                plot_partition_distribution(
                    spx_terminal_partition_df,
                    title="S&P 500 terminal-close distribution implied by Kalshi bins",
                )
                plot_partition_distribution(
                    ndx_terminal_partition_df,
                    title="Nasdaq-100 terminal-close distribution implied by Kalshi bins",
                )
                """
            ),
        ),
        (
            'title="S&P 500 annual max threshold curve"',
            code_cell(
                """
                plot_threshold_curve(
                    spx_max_curve_df,
                    title="S&P 500 annual max threshold curve",
                )
                plot_threshold_buckets(
                    spx_max_bucket_df,
                    title="S&P 500 annual max bucketized probability approximation",
                )
                plot_threshold_curve(
                    spx_min_curve_df,
                    title="S&P 500 annual min threshold curve",
                )
                plot_threshold_buckets(
                    spx_min_bucket_df,
                    title="S&P 500 annual min bucketized probability approximation",
                )
                """
            ),
        ),
        (
            'title="Nasdaq-100 annual max threshold curve"',
            code_cell(
                """
                plot_threshold_curve(
                    ndx_max_curve_df,
                    title="Nasdaq-100 annual max threshold curve",
                )
                plot_threshold_buckets(
                    ndx_max_bucket_df,
                    title="Nasdaq-100 annual max bucketized probability approximation",
                )
                plot_threshold_curve(
                    ndx_min_curve_df,
                    title="Nasdaq-100 annual min threshold curve",
                )
                plot_threshold_buckets(
                    ndx_min_bucket_df,
                    title="Nasdaq-100 annual min bucketized probability approximation",
                )
                """
            ),
        ),
    ]


def main() -> None:
    notebook = _load_existing_notebook()
    updated, appended = _append_unique_cells(notebook, planned_additions())
    NOTEBOOK_PATH.write_text(json.dumps(updated, indent=2) + "\n", encoding="utf-8")
    print(f"Updated {NOTEBOOK_PATH} by appending {appended} new cell(s)")


if __name__ == "__main__":
    main()
