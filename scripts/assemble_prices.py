from pathlib import Path
import pandas as pd

ROOT = Path.cwd()
NORMAL = ROOT / "data"/"normalized"
TICKERS_CSV = ROOT / "data"/"tickers_used.csv"
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "prices_long.parquet"

# get a list of tickers to include
def load_selected_tickers():
    df = pd.read_csv(TICKERS_CSV)
    tickers = df.loc[df['include'].str.lower() == 'yes', 'ticker'].tolist()
    return tickers

def assemble(tickers):
    parts = []
    for ticker in tickers:
        path = NORMAL / f"{ticker}.csv"
        if not path.exists():
            print(f"Warning: file for ticker {ticker} not found at {path}, skipping.")
            continue
        df = pd.read_csv(path, parse_dates=["trade_date"])
        #new ticker column
        df["ticker"] = ticker
        cols = ["ticker", "trade_date", "open", "high", "low", "close", "turnover", "share_volume", "trade_volume"]
        existing_cols = []
        for c in cols:
            if c in df.columns:
                existing_cols.append(c)
        df = df[existing_cols]
        parts.append(df)
    if not parts:
        raise SystemExit("No data assembled")
    all_df = pd.concat(parts, ignore_index=True)
    all_df["trade_date"] = pd.to_datetime(all_df["trade_date"]).dt.date
    all_df = all_df.sort_values(["ticker","trade_date"])
    all_df.to_parquet(OUT_FILE, index=False)
    print(f"Wrote {OUT_FILE} (rows={len(all_df)})")

if __name__ == "__main__":
    tickers = load_selected_tickers()
    print(f"Selected tickers: {len(tickers)}")
    assemble(tickers)