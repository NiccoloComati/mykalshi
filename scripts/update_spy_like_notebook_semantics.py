from __future__ import annotations

import json
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = ROOT / "notebooks" / "spy_like_markets.ipynb"


def src(text: str) -> list[str]:
    stripped = textwrap.dedent(text).strip("\n")
    return [line + "\n" for line in stripped.splitlines()] if stripped else []


def replace_code_cell(cells: list[dict], marker: str, new_source: str) -> None:
    for cell in cells:
        if cell.get("cell_type") != "code":
            continue
        text = "".join(cell.get("source", []))
        if marker in text:
            cell["source"] = src(new_source)
            cell["outputs"] = []
            cell["execution_count"] = None
            return
    raise LookupError(f"Could not find code cell containing marker: {marker}")


def insert_code_cell_after(cells: list[dict], marker: str, new_source: str) -> None:
    for index, cell in enumerate(cells):
        text = "".join(cell.get("source", []))
        if marker in text:
            cells.insert(
                index + 1,
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": src(new_source),
                },
            )
            return
    raise LookupError(f"Could not find insertion marker: {marker}")


def main() -> None:
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    cells = notebook["cells"]

    replace_code_cell(
        cells,
        "def plot_partition_distribution(partition_df, *, title):",
        """
        def plot_partition_distribution(partition_df, *, title):
            if partition_df.empty:
                print(f"No partition distribution available for {title}.")
                return
            plot_df = partition_df.copy()
            values = pd.to_numeric(plot_df["mid_prob_normalized"], errors="coerce")
            if values.isna().all():
                values = pd.to_numeric(plot_df["mid_prob"], errors="coerce")
            fig, ax = plt.subplots(figsize=(14, 4.5))
            ax.bar(plot_df["bin_label"], values.fillna(0.0), color="#2f6db3", edgecolor="white")
            ax.set_title(title)
            ax.set_ylabel("Probability mass")
            ax.set_xlabel("Settlement bin")
            ax.tick_params(axis="x", rotation=60)
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            plt.show()

        def plot_curve_with_reference(curve_df, *, title, reference_df=None, reference_label=None):
            if curve_df.empty:
                print(f"No curve available for {title}.")
                return
            plot_df = curve_df.copy()
            x = pd.to_numeric(plot_df["threshold"], errors="coerce")
            y = pd.to_numeric(plot_df["mid_prob"], errors="coerce")
            fig, ax = plt.subplots(figsize=(10, 4.5))
            ax.plot(x, y, marker="o", linewidth=2, color="#1f4e79", label="Kalshi contract curve")
            if reference_df is not None and not reference_df.empty:
                ref = reference_df.copy()
                ref_x = pd.to_numeric(ref["threshold"], errors="coerce")
                ref_y = pd.to_numeric(ref["probability"], errors="coerce")
                ax.plot(ref_x, ref_y, marker="o", linewidth=2, linestyle="--", color="#d17a22", label=reference_label or "Reference")
            ax.set_title(title)
            ax.set_xlabel("Threshold")
            ax.set_ylabel(plot_df["curve_label"].iloc[0])
            ax.grid(True, alpha=0.3)
            ax.legend(loc="best")
            fig.tight_layout()
            plt.show()

        def plot_bucket_probabilities(bucket_df, *, title):
            if bucket_df.empty:
                print(f"No bucketized view available for {title}.")
                return
            plot_df = bucket_df.copy()
            labels = []
            for _, row in plot_df.iterrows():
                low = row.get("bucket_low")
                high = row.get("bucket_high")
                if pd.notna(low) and pd.notna(high):
                    labels.append(f"{float(low):,.0f} to {float(high):,.0f}")
                elif pd.notna(low):
                    labels.append(f">= {float(low):,.0f}")
                elif pd.notna(high):
                    labels.append(f"<= {float(high):,.0f}")
                else:
                    labels.append(str(row.get("market_ticker")))
            values = pd.to_numeric(plot_df["bucket_prob_mid"], errors="coerce").fillna(0.0)
            fig, ax = plt.subplots(figsize=(14, 4.5))
            ax.bar(labels, values, color="#4c9a2a", edgecolor="white")
            ax.set_title(title)
            ax.set_ylabel("Probability mass")
            ax.set_xlabel("Bucket")
            ax.tick_params(axis="x", rotation=60)
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            plt.show()
        """,
    )

    replace_code_cell(
        cells,
        'spx_max_bucket_df = build_threshold_bucket_view(spx_max_curve_df)',
        """
        pulled = pull_equity_index_market_data(matches)

        markets_df = pulled["markets_df"]
        history_summary_df = pulled["history_summary_df"]
        trade_summary_df = pulled["trade_summary_df"]
        orderbook_summary_df = pulled["orderbook_summary_df"]
        errors_df = pulled["errors_df"]
        classified_markets_df = classify_market_table(markets_df)
        provenance_df = build_provenance_catalog()
        raw_contract_catalog_df = build_raw_contract_catalog(classified_markets_df)
        family_summary_df = build_family_summary(classified_markets_df)

        spx_terminal_partition_df = build_partition_distribution(classified_markets_df, underlying="S&P 500")
        ndx_terminal_partition_df = build_partition_distribution(classified_markets_df, underlying="Nasdaq-100")

        spx_max_curve_df = build_threshold_curve(
            classified_markets_df,
            underlying="S&P 500",
            distribution_family="annual_max",
        )
        ndx_max_curve_df = build_threshold_curve(
            classified_markets_df,
            underlying="Nasdaq-100",
            distribution_family="annual_max",
        )
        spx_min_curve_df = build_threshold_curve(
            classified_markets_df,
            underlying="S&P 500",
            distribution_family="annual_min",
        )
        ndx_min_curve_df = build_threshold_curve(
            classified_markets_df,
            underlying="Nasdaq-100",
            distribution_family="annual_min",
        )

        spx_max_bucket_df = build_threshold_bucket_view(spx_max_curve_df)
        ndx_max_bucket_df = build_threshold_bucket_view(ndx_max_curve_df)
        spx_min_bucket_df = build_threshold_bucket_view(spx_min_curve_df)
        ndx_min_bucket_df = build_threshold_bucket_view(ndx_min_curve_df)

        terminal_reference_df = classified_markets_df[
            classified_markets_df["market_archetype"] == "terminal_reference_upper_tail"
        ].copy()

        def build_terminal_reference_curves(partition_df):
            if partition_df.empty:
                return {
                    "upper_tail_curve": pd.DataFrame(),
                    "lower_tail_curve": pd.DataFrame(),
                    "bucket_mass": pd.DataFrame(),
                }

            ordered = partition_df.copy().sort_values(["bin_low", "bin_high"], na_position="first").reset_index(drop=True)
            probs = pd.to_numeric(ordered["mid_prob_normalized"], errors="coerce").fillna(0.0)
            labels = ordered["bin_label"].astype(str)
            lower = pd.to_numeric(ordered["bin_low"], errors="coerce")
            upper = pd.to_numeric(ordered["bin_high"], errors="coerce")

            upper_tail = []
            lower_tail = []
            for i in range(len(ordered)):
                upper_tail.append(
                    {
                        "threshold": lower.iloc[i] if pd.notna(lower.iloc[i]) else upper.iloc[i],
                        "probability": float(probs.iloc[i:].sum()),
                        "bin_label": labels.iloc[i],
                    }
                )
                lower_tail.append(
                    {
                        "threshold": upper.iloc[i] if pd.notna(upper.iloc[i]) else lower.iloc[i],
                        "probability": float(probs.iloc[: i + 1].sum()),
                        "bin_label": labels.iloc[i],
                    }
                )

            bucket_mass = ordered[["market_ticker", "bin_label", "bin_low", "bin_high", "mid_prob_normalized"]].copy()
            bucket_mass = bucket_mass.rename(columns={"mid_prob_normalized": "bucket_prob_mid"})

            return {
                "upper_tail_curve": pd.DataFrame(upper_tail).sort_values("threshold", na_position="last").reset_index(drop=True),
                "lower_tail_curve": pd.DataFrame(lower_tail).sort_values("threshold", na_position="last").reset_index(drop=True),
                "bucket_mass": bucket_mass,
            }

        spx_terminal_reference = build_terminal_reference_curves(spx_terminal_partition_df)
        ndx_terminal_reference = build_terminal_reference_curves(ndx_terminal_partition_df)

        spx_close_upper_tail_curve_df = spx_terminal_reference["upper_tail_curve"]
        spx_close_lower_tail_curve_df = spx_terminal_reference["lower_tail_curve"]
        ndx_close_upper_tail_curve_df = ndx_terminal_reference["upper_tail_curve"]
        ndx_close_lower_tail_curve_df = ndx_terminal_reference["lower_tail_curve"]

        print("Market rows:", len(markets_df))
        print("History rows:", len(history_summary_df))
        print("Trade summary rows:", len(trade_summary_df))
        print("Orderbook summary rows:", len(orderbook_summary_df))
        print("Errors:", len(errors_df))
        """,
    )

    replace_code_cell(
        cells,
        'title="S&P 500 annual max threshold curve"',
        """
        plot_curve_with_reference(
            spx_max_curve_df,
            title="S&P 500 annual max threshold curve vs terminal-close upper tail",
            reference_df=spx_close_upper_tail_curve_df,
            reference_label="Terminal-close upper tail",
        )
        plot_bucket_probabilities(
            spx_max_bucket_df,
            title="S&P 500 annual max implied probability mass",
        )
        plot_curve_with_reference(
            spx_min_curve_df,
            title="S&P 500 annual min threshold curve vs terminal-close lower tail",
            reference_df=spx_close_lower_tail_curve_df,
            reference_label="Terminal-close lower tail",
        )
        plot_bucket_probabilities(
            spx_min_bucket_df,
            title="S&P 500 annual min implied probability mass",
        )
        """,
    )

    replace_code_cell(
        cells,
        'title="Nasdaq-100 annual max threshold curve"',
        """
        plot_curve_with_reference(
            ndx_max_curve_df,
            title="Nasdaq-100 annual max threshold curve vs terminal-close upper tail",
            reference_df=ndx_close_upper_tail_curve_df,
            reference_label="Terminal-close upper tail",
        )
        plot_bucket_probabilities(
            ndx_max_bucket_df,
            title="Nasdaq-100 annual max implied probability mass",
        )
        plot_curve_with_reference(
            ndx_min_curve_df,
            title="Nasdaq-100 annual min threshold curve vs terminal-close lower tail",
            reference_df=ndx_close_lower_tail_curve_df,
            reference_label="Terminal-close lower tail",
        )
        plot_bucket_probabilities(
            ndx_min_bucket_df,
            title="Nasdaq-100 annual min implied probability mass",
        )
        """,
    )

    insert_code_cell_after(
        cells,
        'plot_partition_distribution(\n    ndx_terminal_partition_df,',
        """
        print("S&P 500 terminal-close upper tail derived from Kalshi close bins")
        display(spx_close_upper_tail_curve_df)
        print("S&P 500 terminal-close lower tail derived from Kalshi close bins")
        display(spx_close_lower_tail_curve_df)

        print("Nasdaq-100 terminal-close upper tail derived from Kalshi close bins")
        display(ndx_close_upper_tail_curve_df)
        print("Nasdaq-100 terminal-close lower tail derived from Kalshi close bins")
        display(ndx_close_lower_tail_curve_df)
        """,
    )

    NOTEBOOK_PATH.write_text(json.dumps(notebook, indent=2) + "\n", encoding="utf-8")
    print(f"Updated {NOTEBOOK_PATH} in place")


if __name__ == "__main__":
    main()
