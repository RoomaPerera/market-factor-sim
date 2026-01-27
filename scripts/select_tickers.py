#!/usr/bin/env python3
"""
scripts/select_tickers.py
Create data/tickers_used.csv from data/raw/manifest.csv by filtering rules.
Usage: python scripts/select_tickers.py --min-rows 100 --min-start "2018-01-01"
explanation: checks if there is enough data in raw files to consider for normalization.
checked if there are at least 100 rows to be considered, and that data starts on or before 2018-01-01.
"""

import csv
import argparse
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path.cwd()
RAW = ROOT / "data" / "raw"
MANIFEST = RAW / "manifest.csv"
OUT = ROOT / "data" / "tickers_used.csv"

def parse_date(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None

def main(min_rows=50, min_start=None, max_missing_days=None):
    if not MANIFEST.exists():
        raise SystemExit(f"Manifest not found: {MANIFEST}")
    rows = []
    with MANIFEST.open("r", encoding="utf8") as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            if r.get("status") != "has_data":
                continue
            ticker = r["ticker"]
            rows_count = int(r.get("rows") or 0)
            sd = parse_date(r.get("start_date"))
            ed = parse_date(r.get("end_date"))
            rel = r.get("relative_path")
            include = True
            if rows_count < min_rows:
                include = False
            if min_start and (sd is None or sd > min_start):
                include = False
            # optional coarse missingness filter via span
            if max_missing_days and sd and ed:
                span = (ed - sd).days
                if span < max_missing_days:
                    include = False
            rows.append({
                "ticker": ticker,
                "relative_path": rel,
                "rows": rows_count,
                "start_date": sd.isoformat() if sd else "",
                "end_date": ed.isoformat() if ed else "",
                "include": "yes" if include else "no"
            })
    # write output
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf8") as fh:
        w = csv.DictWriter(fh, fieldnames=["ticker","relative_path","rows","start_date","end_date","include"])
        w.writeheader()
        for r in sorted(rows, key=lambda x: (-int(x["rows"]), x["ticker"])):
            w.writerow(r)
    print(f"Wrote {OUT} (tickers: {len(rows)})")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--min-rows", type=int, default=50, help="Minimum number of records in raw file")
    p.add_argument("--min-start", type=str, default=None, help="Earliest allowed start date (YYYY-MM-DD)")
    p.add_argument("--max-missing-days", type=int, default=None, help="Minimum span in days between start and end (coarse filter)")
    args = p.parse_args()
    min_start = datetime.fromisoformat(args.min_start).date() if args.min_start else None
    main(min_rows=args.min_rows, min_start=min_start, max_missing_days=args.max_missing_days)