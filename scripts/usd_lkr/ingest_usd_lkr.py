from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path.cwd()
RAW_FILE = ROOT / "data" / "usd_lkr" / "raw" / "usd_lkr_raw.csv"
OUT_FILE = ROOT / "data" / "usd_lkr" / "normalized" / "usd_lkr.csv"
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

def normalize_raw(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    # parse date
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')

    df['usd_lkr_spot'] = (
        df['Price']
        .astype(str)
        .str.replace(',', '')
        .str.replace('"', '')
        .str.strip()
    )
    df['usd_lkr_spot'] = pd.to_numeric(df['usd_lkr_spot'], errors='coerce')

    # drop rows without date or price
    df = df.dropna(subset=['date', 'usd_lkr_spot']).copy()

    # canonicalize date to ISO string (YYYY-MM-DD)
    df['date'] = df['date'].dt.date.astype(str)
    df = df.sort_values('date').drop_duplicates('date', keep='last')
    out = df[['date', 'usd_lkr_spot', 'source']].reset_index(drop=True)
    out = out.sort_values('date')
    return out

def main():
    if not RAW_FILE.exists():
        raise SystemExit(f"Raw file not found: {RAW_FILE}")
    out = normalize_raw(RAW_FILE)
    out.to_csv(OUT_FILE, index=False)
    print(f"Wrote normalized USD/LKR to {OUT_FILE} (rows={len(out)})")
    print("date range:", out['date'].min(), "to", out['date'].max())

if __name__ == "__main__":
    main()