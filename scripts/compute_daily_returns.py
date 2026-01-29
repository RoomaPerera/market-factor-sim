from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path.cwd()
IN_FILE = ROOT / "data" / "processed" / "prices_long.parquet"
OUT_FILE = ROOT / "data" / "processed" / "prices_with_returns.parquet"

def main():
    df = pd.read_parquet(IN_FILE)
    df['trade_date']=pd.to_datetime(df['trade_date'])
    def make_returns(g):
        g = g.sort_values('trade_date').copy()
        g['close_pct_return'] = g['close'].pct_change()
        previouse_close = g['close'].shift(1)
        ratio = g['close'] / previouse_close
        log_return = np.log(ratio)
        safe_log_return = log_return.replace([np.inf, -np.inf], np.nan)
        g['close_log_return'] = safe_log_return
        return g
    out = df.groupby('ticker', group_keys=False).apply(make_returns)
    out.to_parquet(OUT_FILE)
    print(f"Wrote {OUT_FILE} (rows = {len(out)})")

if __name__ == "__main__":
    main()