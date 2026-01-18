"""
Robust CLI runner for ETL with improved function signature detection
and clearer HTTP error reporting.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
from importlib import import_module
from pathlib import Path
import sys
import traceback
import inspect

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]

def parse_date(s: str) -> datetime.date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def slice_df_by_date(df: pd.DataFrame, start_date, end_date):
    if df is None or df.empty:
        return df
    for col in ("trade_date", "date", "Date", "tradeDate"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date
            return df[(df[col] >= start_date) & (df[col] <= end_date)].copy()
    return df

def choose_and_call(func, ticker, s_date, e_date):
    """
    Inspect func signature and attempt the most logical call patterns.
    Returns the result or raises the original exception.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.keys())
    tried = []

    # Build candidate arglists in order of likely usefulness
    candidates = []

    # try keyword-based calls if params named intuitively
    name_map = {p.lower(): p for p in params}
    if 'ticker' in name_map or 'symbol' in name_map or 'stock' in name_map:
        # build keyword dicts
        kwargs = {}
        if 'ticker' in name_map:
            kwargs[name_map['ticker']] = ticker
        elif 'symbol' in name_map:
            kwargs[name_map['symbol']] = ticker
        elif 'stock' in name_map:
            kwargs[name_map['stock']] = ticker
        if 'start' in name_map or 'start_date' in name_map:
            if 'start' in name_map:
                kwargs[name_map['start']] = s_date
            else:
                kwargs[name_map['start_date']] = s_date
        if 'end' in name_map or 'end_date' in name_map:
            if 'end' in name_map:
                kwargs[name_map['end']] = e_date
            else:
                kwargs[name_map['end_date']] = e_date
        if kwargs:
            candidates.append(('kwargs', kwargs))

    # positional attempts: (ticker, start, end), (ticker, start), (ticker,)
    candidates.append(('pos', (ticker, s_date, e_date)))
    candidates.append(('pos', (ticker, s_date)))
    candidates.append(('pos', (ticker,)))
    candidates.append(('pos', (s_date, e_date)))
    candidates.append(('pos', (s_date,)))

    last_exc = None
    for typ, args_or_kwargs in candidates:
        try:
            tried.append((typ, args_or_kwargs))
            if typ == 'kwargs':
                print(f"Trying {func.__module__}.{func.__name__} with keywords: {args_or_kwargs}")
                return func(**args_or_kwargs)
            else:
                print(f"Trying {func.__module__}.{func.__name__} with positional args: {args_or_kwargs}")
                return func(*args_or_kwargs)
        except TypeError as te:
            # signature mismatch — remember and continue
            last_exc = te
            continue
        except requests.exceptions.HTTPError as http_e:
            # Provide richer debug info
            resp = getattr(http_e, "response", None)
            print("HTTPError during download call.")
            if resp is not None:
                try:
                    print(f"Response status: {resp.status_code}")
                    text = resp.text
                    if text:
                        print("Response body (first 1000 chars):")
                        print(text[:1000])
                except Exception:
                    print("Could not read response body.")
            raise
        except Exception as ex:
            # Some other runtime error — raise after printing trace
            print("Exception while calling function:")
            traceback.print_exc()
            raise
    # If we reach here, nothing matched
    print("Tried candidate call patterns (in order):")
    for t in tried:
        print("  ", t)
    if last_exc:
        raise last_exc
    raise RuntimeError("No suitable call pattern found for function " + func.__name__)

def run_single_chunk(ticker, s_date, e_date, dry_run=False):
    dl_mod = import_module("src.etl.download")
    nz_mod = import_module("src.etl.normalize")
    ld_mod = import_module("src.etl.load")

    # find a callable in the download module
    def pick_callable(mod):
        # common names
        candidates = ["fetch_cse_chart", "fetch_chart", "fetch_prices", "download_prices", "get_prices", "fetch"]
        for c in candidates:
            if hasattr(mod, c) and callable(getattr(mod, c)):
                return getattr(mod, c)
        # fallback to first public callable
        for attr in dir(mod):
            if attr.startswith("_"):
                continue
            obj = getattr(mod, attr)
            if callable(obj):
                return obj
        raise RuntimeError(f"No callable found in {mod.__name__}")

    dl_func = pick_callable(dl_mod)
    nz_func = None
    try:
        nz_func = pick_callable(nz_mod)
    except Exception:
        pass
    ld_func = None
    try:
        ld_func = pick_callable(ld_mod)
    except Exception:
        pass

    print(f"Downloading {ticker} {s_date} → {e_date} using {dl_func.__name__}()")
    df = choose_and_call(dl_func, ticker, s_date, e_date)

    df = slice_df_by_date(df, s_date, e_date)
    if df is None or df.empty:
        print("No rows returned for this chunk.")
        return

    print(f"Downloaded {len(df)} rows. Columns: {list(df.columns)}")
    if nz_func:
        try:
            print(f"Normalizing using {nz_func.__name__}()")
            # prefer normalize(df), but choose_and_call will inspect signature
            df = choose_and_call(nz_func, df, s_date, e_date)
        except Exception:
            print("Normalization failed or not applicable — continuing with raw df.")
            traceback.print_exc()

    df = slice_df_by_date(df, s_date, e_date)

    if dry_run:
        print("DRY RUN — sample rows:")
        try:
            print(df.head(10).to_string(index=False))
        except Exception:
            print(df.head(10))
        outdir = PROJECT_ROOT / "data" / "sample"
        outdir.mkdir(parents=True, exist_ok=True)
        csv_path = outdir / f"{ticker}_{s_date}_{e_date}.csv"
        df.to_csv(csv_path, index=False)
        print(f"Saved sample CSV to {csv_path}")
        return

    if ld_func:
        print(f"Loading {len(df)} rows using {ld_func.__name__}()")
        try:
            choose_and_call(ld_func, df, s_date, e_date)
            print("Load complete.")
        except Exception:
            print("Load failed:")
            traceback.print_exc()
            raise
    else:
        print("No load function available; skipping DB write.")

def main():
    p = argparse.ArgumentParser(description="ETL runner (download->normalize->load).")
    p.add_argument("--ticker", "-t", required=True, help="Ticker symbol")
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    p.add_argument("--batch-years", type=int, default=1, help="Chunk size in years")
    p.add_argument("--dry-run", action="store_true", help="Don't write to DB; save CSV")
    args = p.parse_args()

    s_date = parse_date(args.start)
    e_date = parse_date(args.end)
    if s_date > e_date:
        raise SystemExit("start must be <= end")

    batch_years = max(1, args.batch_years)
    cur_start = s_date
    while cur_start <= e_date:
        try:
            tentative = datetime(cur_start.year + batch_years, cur_start.month, cur_start.day).date()
            cur_end = tentative - timedelta(days=1)
            if cur_end > e_date:
                cur_end = e_date
        except Exception:
            cur_end = min(e_date, cur_start + timedelta(days=365 * batch_years) - timedelta(days=1))

        print("="*60)
        print(f"Processing chunk: {cur_start} -> {cur_end}")
        run_single_chunk(args.ticker, cur_start, cur_end, dry_run=args.dry_run)
        cur_start = cur_end + timedelta(days=1)

if __name__ == "__main__":
    main()