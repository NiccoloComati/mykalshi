from __future__ import annotations

import json
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OLD_NOTEBOOK = ROOT / "notebooks" / "main.ipynb"
NEW_NOTEBOOK = ROOT / "notebooks" / "main_current.ipynb"


def src(text: str) -> list[str]:
    stripped = textwrap.dedent(text).strip("\n")
    return [line + "\n" for line in stripped.splitlines()] if stripped else []


def set_cell(nb: dict, index: int, text: str) -> None:
    nb["cells"][index]["source"] = src(text)
    if nb["cells"][index]["cell_type"] == "code":
        nb["cells"][index]["outputs"] = []
        nb["cells"][index]["execution_count"] = None


nb = json.loads(OLD_NOTEBOOK.read_text(encoding="utf-8"))
for cell in nb["cells"]:
    if cell.get("cell_type") == "code":
        cell["outputs"] = []
        cell["execution_count"] = None

nb["metadata"]["kernelspec"] = {
    "display_name": "mykalshi (.venv)",
    "language": "python",
    "name": "mykalshi-venv",
}
nb["metadata"]["language_info"]["version"] = "3.11.9"

set_cell(
    nb,
    0,
    r'''
    import glob
    import importlib.util
    import os
    import site
    import tempfile
    import time
    import warnings
    from datetime import datetime, timezone
    from pathlib import Path

    PROJECT_ROOT = Path.cwd()
    if not (PROJECT_ROOT / "pyproject.toml").exists() and (PROJECT_ROOT.parent / "pyproject.toml").exists():
        PROJECT_ROOT = PROJECT_ROOT.parent
    if str(PROJECT_ROOT) not in os.sys.path:
        os.sys.path.insert(0, str(PROJECT_ROOT))

    def bootstrap_repo_venv_site_packages(project_root: Path):
        version_tag = f"python{os.sys.version_info.major}.{os.sys.version_info.minor}"
        candidates = [
            project_root / ".venv" / "Lib" / "site-packages",
            project_root / ".venv" / "lib" / version_tag / "site-packages",
        ]
        for candidate in candidates:
            if candidate.exists():
                site.addsitedir(str(candidate))
                return candidate
        return None

    required_packages = ("matplotlib", "mplfinance", "numpy", "pandas", "seaborn", "ipywidgets")
    missing = [package_name for package_name in required_packages if importlib.util.find_spec(package_name) is None]
    venv_site_packages = None
    if missing:
        venv_site_packages = bootstrap_repo_venv_site_packages(PROJECT_ROOT)
        missing = [package_name for package_name in required_packages if importlib.util.find_spec(package_name) is None]
    if missing:
        install_target = f'{PROJECT_ROOT}[analysis,storage,websocket]'
        raise ModuleNotFoundError(
            "Notebook dependencies are missing in the current kernel "
            f'({os.sys.executable}). Missing: {", ".join(missing)}. '
            f'Run `%pip install -e \"{install_target}\"` in this notebook, '
            'or switch VS Code/Jupyter to the "mykalshi (.venv)" kernel.'
        )
    if venv_site_packages is not None and ".venv" not in os.sys.executable:
        print(
            "Notebook is running outside the repo venv; "
            f"using packages from {venv_site_packages}"
        )

    from mykalshi import events, formatting, market, routing, trading, communications, exchange
    from mykalshi.config import KalshiConfig
    from mykalshi.exceptions import KalshiHTTPError
    from mykalshi.recorder import MarketLOBRecorder
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    import mplfinance as mpf
    import matplotlib.dates as mdates
    import matplotlib.ticker as mticker
    from IPython.display import clear_output, display

    try:
        from ipywidgets import interact, IntSlider, fixed
        IPYWIDGETS_AVAILABLE = True
    except ModuleNotFoundError:
        IPYWIDGETS_AVAILABLE = False

    sns.set_theme(style="darkgrid")
    %config InlineBackend.figure_format = 'retina'

    CONFIG = KalshiConfig.from_env()
    AUTH_AVAILABLE = bool(CONFIG.api_key_id and CONFIG.private_key_path)
    PRESIDENTIAL_EVENT_TICKER = "PRES-2024"
    WEATHER_SERIES = {
        'KXHIGHUS': 'High temp in United States',
        'KXHIGHAUS': 'Highest temperature in Austin',
        'KXHIGHCHI': 'Highest temperature in Chicago',
        'KXHIGHDEN': 'Highest temperature in Denver',
        'KXHIGHHOU': 'Highest temperature in Houston',
        'KXHIGHLAX': 'Highest temperature in Los Angeles',
        'KXHIGHMIA': 'Highest temperature in Miami',
        'KXHIGHNY': 'Highest temperature in NYC',
        'KXHIGHPHIL': 'Highest temperature in Philadelphia',
    }

    def latest_market_snapshot_path() -> Path | None:
        files = sorted(PROJECT_ROOT.glob("all_markets_*.csv"), key=lambda path: path.stat().st_mtime)
        return files[-1] if files else None

    MARKET_HISTORY_CACHE = {}

    def retry_on_429(label: str, func, *, max_retries: int = 4, base_backoff_seconds: float = 1.0):
        for attempt in range(1, max_retries + 1):
            try:
                return func()
            except KalshiHTTPError as exc:
                if exc.status_code != 429 or attempt >= max_retries:
                    raise
                sleep_seconds = base_backoff_seconds * attempt
                print(f"Rate limited while loading {label}; retrying in {sleep_seconds:.1f}s...")
                time.sleep(sleep_seconds)

    def normalize_market_frame(frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.copy()
        if frame.empty:
            return frame
        for target, source in {
            "last_price": "last_price_dollars",
            "yes_bid": "yes_bid_dollars",
            "yes_ask": "yes_ask_dollars",
            "no_bid": "no_bid_dollars",
            "no_ask": "no_ask_dollars",
        }.items():
            if target not in frame.columns and source in frame.columns:
                frame[target] = (pd.to_numeric(frame[source], errors="coerce") * 100).round()
        for target, source in {
            "volume": "volume_fp",
            "volume_24h": "volume_24h_fp",
            "open_interest": "open_interest_fp",
            "liquidity": "liquidity_dollars",
        }.items():
            if target not in frame.columns and source in frame.columns:
                frame[target] = pd.to_numeric(frame[source], errors="coerce")
        return frame

    def load_market_snapshot() -> pd.DataFrame:
        snapshot_path = latest_market_snapshot_path()
        if snapshot_path is not None:
            print(f"Loaded market snapshot from {snapshot_path.name}")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=pd.errors.DtypeWarning)
                return normalize_market_frame(pd.read_csv(snapshot_path, low_memory=False))
        print("No cached all-market CSV found; falling back to a sampled live pull.")
        sampled = pd.json_normalize(market.get_all_markets(batch_size=100, max_items=500))
        return normalize_market_frame(sampled)

    def load_open_markets(limit: int = 300) -> pd.DataFrame:
        payload = pd.json_normalize(market.get_all_markets(status="open", batch_size=100, max_items=limit))
        return normalize_market_frame(payload)

    def pick_lob_market(open_markets: pd.DataFrame, probe_limit: int = 20) -> str:
        explicit = os.getenv("MYKALSHI_NOTEBOOK_LOB_TICKER")
        if explicit:
            return explicit
        sortable = open_markets.copy()
        if "volume" in sortable.columns:
            sortable = sortable.sort_values(["volume", "ticker"], ascending=[False, True])
        fallback = str(sortable.iloc[0]["ticker"])
        quoted = sortable
        if {"yes_bid", "yes_ask"}.issubset(sortable.columns):
            quoted = sortable[sortable["yes_bid"].notna() & sortable["yes_ask"].notna()]
            if not AUTH_AVAILABLE and not quoted.empty:
                return str(quoted.iloc[0]["ticker"])
        if not AUTH_AVAILABLE:
            return fallback

        best_one_sided = None
        candidates = list(quoted["ticker"].head(probe_limit)) if not quoted.empty else []
        seen = set(str(ticker) for ticker in candidates)
        candidates.extend(
            str(ticker) for ticker in sortable["ticker"].head(probe_limit * 2)
            if str(ticker) not in seen
        )
        for ticker in candidates:
            try:
                orderbook = market.get_market_orderbook(str(ticker)).get("orderbook", {})
            except Exception:
                continue
            yes_levels = orderbook.get("yes", [])
            no_levels = orderbook.get("no", [])
            if yes_levels and no_levels:
                return str(ticker)
            if (yes_levels or no_levels) and best_one_sided is None:
                best_one_sided = str(ticker)

        return best_one_sided or fallback

    def load_full_market_cached(
        series_ticker: str,
        ticker: str,
        period_interval: str,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
        max_retries: int = 4,
        base_backoff_seconds: float = 1.0,
    ):
        cache_key = (series_ticker, ticker, period_interval, start_ts, end_ts)
        if cache_key in MARKET_HISTORY_CACHE:
            return MARKET_HISTORY_CACHE[cache_key]

        result = retry_on_429(
            ticker,
            lambda: market.get_full_market(
                series_ticker=series_ticker,
                ticker=ticker,
                period_interval=period_interval,
                start_ts=start_ts,
                end_ts=end_ts,
            ),
            max_retries=max_retries,
            base_backoff_seconds=base_backoff_seconds,
        )
        MARKET_HISTORY_CACHE[cache_key] = result
        return result

    def extract_city_from_text(series: pd.Series) -> pd.Series:
        return series.astype(str).str.extract(
            r'in\s+(.+?)(?=\s+(?:on|today|tomorrow|yesterday)\b|[?]|$)',
            expand=False,
        ).str.strip()
    ''',
)

set_cell(nb, 2, '''
markets_df = load_market_snapshot()
display(markets_df.head(10))
''')

set_cell(nb, 6, '''
open_mkts = load_open_markets(limit=300)
LOB_MARKET_TICKER = pick_lob_market(open_mkts)
display(open_mkts.sort_values(by='volume', ascending=False).head(25))
print(f"Using LOB market: {LOB_MARKET_TICKER}")
''')

set_cell(nb, 8, '''
out = events.event_info(PRESIDENTIAL_EVENT_TICKER)
display(pd.DataFrame([out["event_info"]]))
display(out["markets"].sort_values("volume", ascending=False))
''')
set_cell(nb, 9, '''
results = {}

for mkt in out["markets"]["market_ticker"].values:
    cs_df = market.candlesticks_to_df(
        load_full_market_cached(series_ticker="PRES", ticker=mkt, period_interval='h', end_ts='11/10/2024')
    )[['end_period', 'yes_ask_close', 'yes_bid_close', 'volume']]

    cs_df['end_period'] = pd.to_datetime(cs_df['end_period'])
    cs_df['date'] = cs_df['end_period']
    cs_df['midval'] = (cs_df['yes_ask_close'] + cs_df['yes_bid_close']) / 2
    cs_df['ask'] = cs_df['yes_ask_close']
    cs_df['bid'] = cs_df['yes_bid_close']
    cs_df = cs_df.drop(columns=['yes_ask_close', 'yes_bid_close', 'end_period'])
    cs_df.set_index('date', inplace=True)
    title = out["markets"].loc[out["markets"]["market_ticker"] == mkt, "yes_sub_title"].values[0]
    results[title] = cs_df

all_indices = sorted(set().union(*[df.index for df in results.values()]))
global_index = pd.DatetimeIndex(all_indices)
results_df = pd.DataFrame(index=global_index)

for title, df in results.items():
    df = df.groupby(df.index).mean()
    renamed_df = df.rename(columns={
        'midval': f'{title}_midval',
        'ask': f'{title}_ask',
        'bid': f'{title}_bid',
        'volume': f'{title}_volume'
    })
    renamed_df = renamed_df.reindex(global_index)
    results_df = pd.concat([results_df, renamed_df], axis=1)

for col in results_df.columns:
    if '_midval' in col:
        vol_col = col.replace('_midval', '_volume')
        filled_series = results_df[col].copy()
        volume_series = results_df[vol_col]
        for i in range(1, len(filled_series)):
            if pd.isna(filled_series.iloc[i]) and pd.notna(volume_series.iloc[i-1]) and volume_series.iloc[i-1] > 200:
                filled_series.iloc[i] = filled_series.iloc[i - 1]
        results_df[col] = filled_series

results_df['Vol'] = results_df[[col for col in results_df.columns if '_volume' in col]].sum(axis=1)
results_df['Tot'] = results_df[[col for col in results_df.columns if '_midval' in col]].sum(axis=1)
results_df.index.name = 'date'
results_df
''')

set_cell(nb, 11, '''
pres_djt_data = load_full_market_cached(
    series_ticker="PRES",
    ticker="PRES-2024-DJT",
    end_ts="11/10/2024",
    period_interval='d'
)

pres_kh_data = load_full_market_cached(
    series_ticker="PRES",
    ticker="PRES-2024-KH",
    end_ts="11/10/2024",
    period_interval='d'
)
djt_df = market.candlesticks_to_df(pres_djt_data)
kh_df = market.candlesticks_to_df(pres_kh_data)

djt_candlestick_df = market.build_candlestick(pres_djt_data)
kh_candlestick_df = market.build_candlestick(pres_kh_data)

fig, axes = mpf.plot(
    kh_candlestick_df,
    volume=True,
    figratio=(20, 10),
    figscale=1.8,
    show_nontrading=False,
    mav=3 * 24,
    returnfig=True,
    type='candle',
)
_ = axes[0].axhline(y=50, color='red', linestyle='--', linewidth=1)
_ = axes[0].axvline(x=datetime.strptime('Feb 3 1970 00:00', '%b %d %Y %H:%M'), color='blue', linestyle='--', linewidth=1)
plt.show()

fig, axes = mpf.plot(
    djt_candlestick_df,
    volume=True,
    figratio=(20, 10),
    figscale=1.8,
    show_nontrading=False,
    mav=3 * 24,
    returnfig=True,
    type='candle',
)
_ = axes[0].axhline(y=50, color='red', linestyle='--', linewidth=1)
_ = axes[0].axvline(x=datetime.strptime('Feb 3 1970 00:00', '%b %d %Y %H:%M'), color='blue', linestyle='--', linewidth=1)
plt.show()
''')

set_cell(nb, 13, '''
# very liquid tested markets {'market ticker': 'market question}
tested_markets = {'PRES-2024-KH': 'Will Kamala Harris or another Democrat win the Presidency?',
'PRES-2024-DJT': 'Will Donald Trump or another Republican win the Presidency?',
'KXNBA-25-IND': 'Will the Indiana Pacers win the NBA Finals?',
'KXNBA-25-OKC': 'Will the Oklahoma City Thunder win the NBA Finals?',
'POPVOTE-24-D': 'Will the Democratic party win the popular vote?',
'POPVOTE-24-R': 'Will the Republican party win the popular vote?'}

market_vals = {}
trades_record = {}

def ensure_tested_market_history(ticker, period_interval='h'):
    if ticker not in market_vals:
        market_vals[ticker] = load_full_market_cached(
            series_ticker=ticker.split('-')[0],
            ticker=ticker,
            period_interval=period_interval,
        )['candlesticks']
    return market_vals[ticker]

for ticker in tested_markets.keys():
    trades_record[ticker] = routing.get_trades_preview_dataframe_auto(ticker, limit=25)

ensure_tested_market_history('KXNBA-25-IND')
''')

set_cell(nb, 14, '''
routing.get_trades_preview_dataframe_auto(ticker='PRES-2024-DJT', limit=25)
''')

set_cell(nb, 16, '''
benchmark_results = []
for r in [5, 10, 20]:
    try:
        result = market.get_all_trades(ticker='PRES-2024-DJT', calls_per_sec=r, batch_size=100)
        ok = result['total_count'] >= 0
    except Exception:
        ok = False
    benchmark_results.append({'calls_per_sec': r, 'trades_success': ok})

pd.DataFrame(benchmark_results)
''')

set_cell(nb, 19, '''
orderbook = market.get_market_orderbook(ticker=LOB_MARKET_TICKER)["orderbook"]
yes_bids = sorted(orderbook["yes"], key=lambda x: x[0])
yes_asks = sorted([[100 - price, size] for price, size in orderbook["no"]], key=lambda x: x[0])

print()
print(f"Ticker: {LOB_MARKET_TICKER}")
if not yes_bids and not yes_asks:
    print("No visible YES-side depth in the current book snapshot.")
print("Bids:")
for price, qty in yes_bids:
    print(f"  YES @ {price}c x {qty:,.2f} contracts")

print("Asks:")
for price, qty in yes_asks:
    print(f"  YES @ {price}c x {qty:,.2f} contracts")
''')

set_cell(nb, 21, 'get_market_lob(LOB_MARKET_TICKER)')
set_cell(nb, 22, 'plot_market_lob(LOB_MARKET_TICKER)')
set_cell(nb, 26, 'routing.get_trades_preview_dataframe_auto(LOB_MARKET_TICKER, limit=25)')
set_cell(nb, 20, '''
def get_market_lob(ticker):
    orderbook = market.get_market_orderbook(ticker=ticker)["orderbook"]
    yes_bids = sorted(orderbook["yes"], key=lambda x: x[0])
    yes_asks = sorted([[100 - price, size] for price, size in orderbook["no"]], key=lambda x: x[0])

    print()
    print(f"Ticker: {ticker}")
    if not yes_bids and not yes_asks:
        print("No visible YES-side depth in the current book snapshot.")
        return {"yes_bids": yes_bids, "yes_asks": yes_asks}

    print("Bids:")
    for price, qty in yes_bids:
        print(f"  YES @ {price}c x {qty:,.2f} contracts")

    print("Asks:")
    for price, qty in yes_asks:
        print(f"  YES @ {price}c x {qty:,.2f} contracts")

    return {"yes_bids": yes_bids, "yes_asks": yes_asks}

def plot_market_lob(ticker):
    orderbook = market.get_market_orderbook(ticker=ticker)["orderbook"]
    yes_bids = sorted(orderbook["yes"], key=lambda x: x[0])
    yes_asks = sorted([[100 - price, size] for price, size in orderbook["no"]], key=lambda x: x[0])

    ask_prices = [p for p, _ in yes_asks]
    ask_sizes = [q for _, q in yes_asks]
    ask_cum = list(np.cumsum(ask_sizes)) if ask_sizes else []

    bid_prices = [p for p, _ in yes_bids]
    bid_sizes = [q for _, q in yes_bids]
    bid_cum = list(np.cumsum(bid_sizes[::-1]))[::-1] if bid_sizes else []

    plt.figure(figsize=(10, 6))
    if not bid_prices and not ask_prices:
        plt.text(0.5, 0.5, f"No visible YES-side depth for {ticker}", ha="center", va="center", transform=plt.gca().transAxes)
        plt.xlim(0, 100)
        plt.ylim(0, 1)
        plt.xlabel("Price (c)")
        plt.ylabel("Cumulative Size")
        plt.title("YES Order Book Depth")
        plt.grid(True)
        plt.tight_layout()
        plt.show()
        return
    if bid_prices:
        bid_prices_ext = bid_prices + [bid_prices[-1]]
        bid_cum_ext = bid_cum + [0]
        plt.step(bid_prices_ext, bid_cum_ext, label="Bids", color="green", where="post")
        plt.fill_between(bid_prices_ext, bid_cum_ext, step="post", color="green", alpha=0.3, hatch='//')
    if ask_prices:
        ask_prices_ext = [ask_prices[0]] + ask_prices
        ask_cum_ext = [0] + ask_cum
        plt.step(ask_prices_ext, ask_cum_ext, label="Asks", color="red", where="post")
        plt.fill_between(ask_prices_ext, ask_cum_ext, step="post", color="red", alpha=0.3, hatch='\\\\')

    plt.xlabel("Price (c)")
    plt.ylabel("Cumulative Size")
    plt.title("YES Order Book Depth")
    plt.legend(loc="upper center")
    plt.xlim(0, 100)
    plt.grid(True)

    max_val = max(max(bid_cum, default=0), max(ask_cum, default=0))
    if max_val >= 1_000_000:
        divisor = 1_000_000
        suffix = "M"
    elif max_val >= 1_000:
        divisor = 1_000
        suffix = "K"
    else:
        divisor = 1
        suffix = ""

    plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x / divisor:.1f}{suffix}"))
    plt.tight_layout()
    plt.show()
''')

set_cell(nb, 23, '''
import time

def live_market_lob(ticker, refresh_interval=1, iterations=5):
    try:
        for _ in range(iterations):
            loop_start = time.time()
            os.system('cls' if os.name == 'nt' else 'clear')
            get_market_lob(ticker)
            elapsed = time.time() - loop_start
            time.sleep(max(0, refresh_interval - elapsed))
    except KeyboardInterrupt:
        print()
        print("Stopped.")
''')

set_cell(nb, 24, '''
import time
from IPython.display import clear_output

def live_market_lob_notebook(ticker, refresh_interval=1, iterations=5):
    try:
        for _ in range(iterations):
            loop_start = time.time()
            clear_output(wait=True)
            get_market_lob(ticker)
            elapsed = time.time() - loop_start
            time.sleep(max(0, refresh_interval - elapsed))
    except KeyboardInterrupt:
        print()
        print("Stopped.")
''')

set_cell(nb, 25, '''
import time
from IPython.display import clear_output

def live_plot_market_lob(ticker, refresh_interval=1, iterations=5):
    try:
        for _ in range(iterations):
            loop_start = time.time()
            clear_output(wait=True)
            plot_market_lob(ticker)
            elapsed = time.time() - loop_start
            time.sleep(max(0, refresh_interval - elapsed))
    except KeyboardInterrupt:
        clear_output(wait=True)
        print("Live plot stopped.")
''')

set_cell(nb, 27, 'live_market_lob_notebook(LOB_MARKET_TICKER, refresh_interval=2, iterations=3)')
set_cell(nb, 28, 'live_plot_market_lob(LOB_MARKET_TICKER, refresh_interval=2, iterations=3)')

set_cell(nb, 37, '''
import time
from datetime import datetime
import numpy as np
import pandas as pd

capture_path = Path(tempfile.gettempdir()) / f"{LOB_MARKET_TICKER.replace('-', '_')}_lob_snapshots.jsonl"
rec = MarketLOBRecorder(
    tickers=[LOB_MARKET_TICKER],
    interval_secs=2,
    max_workers=1,
    calls_per_sec=5,
    output_path=str(capture_path),
)

t0 = time.time()
rec.start(duration_secs=12)
total_duration = time.time() - t0

df = pd.read_json(capture_path, lines=True)
df['ts_dt'] = pd.to_datetime(df['timestamp'])
''')

set_cell(nb, 38, '''
total_records = len(df)
error_count = df['error'].notnull().sum() if 'error' in df.columns else 0
error_rate = error_count / total_records if total_records else float('nan')

mse_list = []
all_deltas = []

for tk, group in df.groupby('ticker'):
    times = group.sort_values('ts_dt')['ts_dt']
    deltas = times.diff().dt.total_seconds().dropna()
    if not deltas.empty:
        mse_list.append(((deltas - 2.0)**2).mean())
        all_deltas.extend(deltas.values)

precision_mse = float(np.mean(mse_list)) if mse_list else float('nan')
mean_cycle = float(np.mean(all_deltas)) if all_deltas else float('nan')
std_cycle = float(np.std(all_deltas)) if all_deltas else float('nan')

print(f"Total duration:         {total_duration:.2f} s")
print(f"Total records:          {total_records}")
print(f"Error count:            {error_count}")
print(f"Error rate:             {error_rate:.2%}")
print(f"Precision MSE (2s):     {precision_mse:.6f}")
print(f"Mean cycle length:      {mean_cycle:.3f} s")
print(f"Cycle length std dev:   {std_cycle:.3f} s")
''')

set_cell(nb, 43, '''
import pandas as pd

def snapshot_list_to_dfs(snapshots):
    price_levels = list(range(1, 100))
    bids_data = []
    asks_data = []
    timestamps = []

    for snap in snapshots:
        if snap.get('error'):
            continue
        timestamps.append(pd.to_datetime(snap["timestamp"], utc=True))
        bids_data.append([snap["bids"].get(str(p), snap["bids"].get(p, 0)) for p in price_levels])
        asks_data.append([snap["asks"].get(str(p), snap["asks"].get(p, 0)) for p in price_levels])

    bids_ts = pd.DataFrame(bids_data, index=timestamps, columns=price_levels).astype(float)
    asks_ts = pd.DataFrame(asks_data, index=timestamps, columns=price_levels).astype(float)
    return bids_ts, asks_ts

filtered_snapshots = df[df['ticker'] == LOB_MARKET_TICKER].to_dict(orient="records")
bids_df, asks_df = snapshot_list_to_dfs(filtered_snapshots)
asks_df
''')

set_cell(nb, 44, '''
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.ticker as mticker

def plot_lob_at_index(idx, bids_ts, asks_ts):
    snapshot_time = bids_ts.index[idx]
    bids = bids_ts.iloc[idx]
    asks = asks_ts.iloc[idx]

    yes_bids = sorted([[p, bids[p]] for p in bids.index if bids[p] > 0], key=lambda x: x[0])
    yes_asks = sorted([[p, asks[p]] for p in asks.index if asks[p] > 0], key=lambda x: x[0])

    bid_prices = [p for p, _ in yes_bids]
    bid_sizes = [q for _, q in yes_bids]
    bid_cum = list(np.cumsum(bid_sizes[::-1]))[::-1] if bid_sizes else []

    ask_prices = [p for p, _ in yes_asks]
    ask_sizes = [q for _, q in yes_asks]
    ask_cum = list(np.cumsum(ask_sizes)) if ask_sizes else []

    plt.figure(figsize=(11, 5))

    if yes_bids:
        bid_prices_ext = bid_prices + [bid_prices[-1]]
        bid_cum_ext = bid_cum + [0]
        plt.step(bid_prices_ext, bid_cum_ext, label="Bids", color="green", where="post")
        plt.fill_between(bid_prices_ext, bid_cum_ext, step="post", color="green", alpha=0.3, hatch='//')

    if yes_asks:
        ask_prices_ext = [ask_prices[0]] + ask_prices
        ask_cum_ext = [0] + ask_cum
        plt.step(ask_prices_ext, ask_cum_ext, label="Asks", color="red", where="post")
        plt.fill_between(ask_prices_ext, ask_cum_ext, step="post", color="red", alpha=0.3, hatch='\\\\')

    plt.title(f"LOB at {snapshot_time}")
    plt.xlabel("Price (c)")
    plt.ylabel("Cumulative Size")
    plt.xlim(0, 100)
    plt.grid(True)
    plt.legend(loc="upper center")

    max_val = max(max(bid_cum, default=0), max(ask_cum, default=0))
    if max_val >= 1_000_000:
        divisor, suffix = 1_000_000, "M"
    elif max_val >= 1_000:
        divisor, suffix = 1_000, "K"
    else:
        divisor, suffix = 1, ""

    plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x / divisor:.1f}{suffix}"))
    plt.tight_layout()
    plt.show()

if IPYWIDGETS_AVAILABLE and len(bids_df) > 0:
    interact(
        plot_lob_at_index,
        idx=IntSlider(min=0, max=len(bids_df) - 1, step=1, value=0),
        bids_ts=fixed(bids_df),
        asks_ts=fixed(asks_df)
    )
elif len(bids_df) > 0:
    plot_lob_at_index(0, bids_df=bids_df, asks_ts=asks_df)
else:
    print("No clean order book snapshots were captured.")
''')

set_cell(nb, 45, '''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

order_size = 10
inventory = 0
cash = 0
inventory_path = []
cash_path = []
midprice_path = []
timestamp_path = []

timestamps = bids_df.index.to_list()

for i in range(1, len(timestamps)):
    t_prev = timestamps[i - 1]
    t_curr = timestamps[i]
    bid_prev = bids_df.loc[t_prev]
    ask_prev = asks_df.loc[t_prev]
    bid_curr = bids_df.loc[t_curr]
    ask_curr = asks_df.loc[t_curr]

    bid_prices = [p for p in bid_prev.index if bid_prev[p] > 0]
    ask_prices = [p for p in ask_prev.index if ask_prev[p] > 0]
    if not bid_prices or not ask_prices:
        continue

    best_bid = max(bid_prices)
    best_ask = min(ask_prices)
    mid = 0.5 * (best_bid + best_ask)

    bid_filled = best_bid in bid_curr and (bid_prev[best_bid] - bid_curr[best_bid]) >= order_size
    ask_filled = best_ask in ask_curr and (ask_prev[best_ask] - ask_curr[best_ask]) >= order_size

    if bid_filled:
        inventory += order_size
        cash -= order_size * best_bid
    if ask_filled:
        inventory -= order_size
        cash += order_size * best_ask

    inventory_path.append(inventory)
    cash_path.append(cash)
    midprice_path.append(mid)
    timestamp_path.append(t_curr)

pnl = pd.Series(cash_path) + pd.Series(inventory_path) * pd.Series(midprice_path)

plt.figure(figsize=(12, 6))
palette = sns.color_palette("tab10")
plt.plot(timestamp_path, pnl, label="PnL", color=palette[0])
plt.ylabel("PnL")
plt.legend(loc="upper left")
plt.twinx().plot(timestamp_path, inventory_path, label="Inventory", color=palette[1])
plt.ylabel("Inventory (contracts)")
plt.xticks(rotation=45)
plt.title("FIFO-Fill Simulated Passive MM")
plt.xlabel("Time")
plt.tight_layout()
plt.show()
''')
set_cell(nb, 48, '''
family_markets = normalize_market_frame(
    pd.json_normalize(market.get_markets(series_ticker='KXHIGHMIA', status='open', limit=10)['markets'])
).sort_values('volume', ascending=False)
family_tickers = family_markets['ticker'].head(4).tolist()
family_capture_path = Path(tempfile.gettempdir()) / 'mykalshi_weather_family_lob.jsonl'
family_rec = MarketLOBRecorder(
    tickers=family_tickers,
    interval_secs=4,
    max_workers=min(4, len(family_tickers)),
    calls_per_sec=8,
    output_path=str(family_capture_path),
)
family_rec.start(duration_secs=12)
family_df = pd.read_json(family_capture_path, lines=True)
family_df
''')

set_cell(nb, 49, '''
results = {}
for ticker in family_tickers:
    snapshots = family_df[family_df['ticker'] == ticker].to_dict(orient="records")
    if len(snapshots) < 2:
        continue
    bids_df, asks_df = snapshot_list_to_dfs(snapshots)

    order_size = 10
    inventory = 0
    cash = 0
    inventory_path = []
    cash_path = []
    midprice_path = []
    timestamp_path = []

    timestamps = bids_df.index.to_list()
    for i in range(1, len(timestamps)):
        t_prev = timestamps[i - 1]
        t_curr = timestamps[i]
        bid_prev = bids_df.loc[t_prev]
        ask_prev = asks_df.loc[t_prev]
        bid_curr = bids_df.loc[t_curr]
        ask_curr = asks_df.loc[t_curr]
        bid_prices = [p for p in bid_prev.index if bid_prev[p] > 0]
        ask_prices = [p for p in ask_prev.index if ask_prev[p] > 0]
        if not bid_prices or not ask_prices:
            continue
        best_bid = max(bid_prices)
        best_ask = min(ask_prices)
        mid = 0.5 * (best_bid + best_ask)
        bid_filled = best_bid in bid_curr and (bid_prev[best_bid] - bid_curr[best_bid]) >= order_size
        ask_filled = best_ask in ask_curr and (ask_prev[best_ask] - ask_curr[best_ask]) >= order_size
        if bid_filled:
            inventory += order_size
            cash -= order_size * best_bid
        if ask_filled:
            inventory -= order_size
            cash += order_size * best_ask
        inventory_path.append(inventory)
        cash_path.append(cash)
        midprice_path.append(mid)
        timestamp_path.append(t_curr)

    if timestamp_path:
        pnl = pd.Series(cash_path) + pd.Series(inventory_path) * pd.Series(midprice_path)
        results[ticker] = {"timestamp": timestamp_path, "pnl": pnl, "inventory": inventory_path}

if results:
    n = len(results)
    cols = 2
    rows = (n + cols - 1) // cols
    fig, axs = plt.subplots(rows, cols, figsize=(14, 3.5 * rows), sharex=False)
    axs = axs.flatten()
    for i, (ticker, data) in enumerate(results.items()):
        ax = axs[i]
        ax2 = ax.twinx()
        ax.plot(data["timestamp"], data["pnl"], label="PnL", color="tab:blue")
        ax2.plot(data["timestamp"], data["inventory"], label="Inventory", color="tab:orange")
        ax.set_title(ticker)
        ax.set_ylabel("PnL")
        ax2.set_ylabel("Inventory")
        ax.tick_params(axis='x', rotation=45)
    for j in range(i + 1, len(axs)):
        fig.delaxes(axs[j])
    fig.suptitle("Simulated FIFO-Fill MM Performance Across Markets", fontsize=14)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()
else:
    print("The short family capture did not collect enough movement for the multi-market simulation grid.")
''')

set_cell(nb, 50, 'pd.DataFrame({"ticker": family_tickers})')
set_cell(nb, 52, 'pd.DataFrame({"ticker": list(results.keys())}) if results else pd.DataFrame()')
set_cell(nb, 53, 'for tkr, r in results.items():\n    print(f"{tkr:20s} Last simulated PnL: {r[\'pnl\'].iloc[-1]:.2f}")')

set_cell(nb, 61, '''
weather_series = pd.json_normalize(events.get_series_list(category='Climate and Weather')['series'])
weather_titles = pd.Series(weather_series['title'].unique())
weather_titles.sort_values()
weather_series
''')

set_cell(nb, 62, 'weather_series_dict = WEATHER_SERIES.copy()\nweather_series_dict')
set_cell(nb, 64, '''
weather_events = pd.DataFrame()
for series_ticker in weather_series_dict.keys():
    events_temp = pd.json_normalize(
        retry_on_429(
            f"weather events {series_ticker}",
            lambda st=series_ticker: events.get_events(series_ticker=st, limit=25),
        )['events']
    )
    weather_events = pd.concat([weather_events, events_temp], ignore_index=True)
weather_events['City'] = extract_city_from_text(weather_events['title'])
weather_events
''')

set_cell(nb, 65, '''
weather_markets = pd.DataFrame()
for series_ticker in weather_series_dict.keys():
    events_temp = pd.json_normalize(
        retry_on_429(
            f"weather markets {series_ticker}",
            lambda st=series_ticker: market.get_markets(series_ticker=st, status='open', limit=25),
        )['markets']
    )
    weather_markets = pd.concat([weather_markets, events_temp], ignore_index=True)
weather_markets = normalize_market_frame(weather_markets)
weather_markets['City'] = extract_city_from_text(weather_markets['yes_sub_title'])
weather_markets
''')

set_cell(nb, 69, '''
try:
    import requests
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    print("Install beautifulsoup4 to run the legacy IMDB appendix.")
''')

set_cell(nb, 70, '''
if 'BeautifulSoup' in globals():
    def get_rating_distribution(title_id):
        url = f"https://www.imdb.com/title/{title_id}/ratings"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        dist = {}
        for row in soup.select('table.imdbRatingTable tr'):
            cols = row.find_all('td')
            if len(cols) == 3:
                rating = int(cols[0].text.strip())
                count = int(cols[2].text.strip().replace(',', ''))
                dist[rating] = count
        return dist
''')

set_cell(nb, 71, '''
if 'get_rating_distribution' in globals():
    dist = get_rating_distribution('tt0133093')
    dist
''')

set_cell(nb, 72, '''
if 'BeautifulSoup' in globals():
    url = f"https://www.imdb.com/title/tt0133093/ratings"
    headers = {'User-Agent': 'Mozilla/5.0'}
    res = requests.get(url, headers=headers, timeout=15)
    soup = BeautifulSoup(res.text, 'html.parser')
    soup
''')

NEW_NOTEBOOK.write_text(json.dumps(nb, indent=1) + "\n", encoding="utf-8")
