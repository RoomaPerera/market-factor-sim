"""
    -recursively scan data/raw for JSON files
    -classifies them into subfolders (has_data, empty_file, empty_array, invalid_json)
    - writes data/raw/manifest.csv (tikcer, relative_path, status, rows,start_date, end_date, filesize)
    - for each has_data file: normalize into data/normalized/<ticker>.csv with canonical schema
    - basic validation and row filtering applied
    safe: it DOES NOT delete raw files; it writes normalized CSVs and manifest
"""
import os, json, csv, shutil
from datetime import datetime
import pandas as pd

ROOT = os.getcwd()
RAW = os.path.join(ROOT, "data", "cse", "raw")
NORMAL = os.path.join(ROOT, "data", "cse", "normalized")
MANIFEST = os.path.join(RAW, "manifest.csv")

os.makedirs(NORMAL, exist_ok=True)
os.makedirs(RAW, exist_ok=True)

def _ms_to_date_iso(ms):
    try:
        return datetime.utcfromtimestamp(int(ms)/1000.0).date().isoformat()
    except Exception:
        return ""

def classify_and_summarize():
    rows = []
    for dirpath, _, files in os.walk(RAW):
        for fn in sorted(files):
            if not fn.lower().endswith((".json", ".txt")):
                continue
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, RAW).replace("\\","/")
            size = os.path.getsize(full)
            status = "unknown"
            recs = 0
            start_date = ""
            end_date = ""
            try:
                if size == 0:
                    status = "empty_file"
                else:
                    with open(full, "r", encoding="utf8", errors="ignore") as f:
                        txt = f.read().strip()
                    if txt == "":
                        status = "empty_file"
                    else:
                        # try JSON
                        try:
                            j = json.loads(txt)
                        except Exception:
                            # try to salvage, sometimes pasted HTML contains JSON array inside — extract first '['..']'
                            if "[" in txt and "]" in txt:
                                start = txt.find("[")
                                end = txt.rfind("]")+1
                                try:
                                    j = json.loads(txt[start:end])
                                except Exception:
                                    j = None
                            else:
                                j = None
                        if j is None:
                            status = "invalid_json"
                        else:
                            if isinstance(j, list):
                                recs = len(j)
                                if recs == 0:
                                    status = "empty_array"
                                else:
                                    status = "has_data"
                                    dates=[]
                                    for it in j:
                                        if not isinstance(it, dict):
                                            continue
                                        td = it.get("tradeDate") or it.get("d") or it.get("date")
                                        if td is None:
                                            continue
                                        try:
                                            dates.append(datetime.utcfromtimestamp(int(td)/1000.0).date())
                                        except:
                                            pass
                                    if dates:
                                        start_date = min(dates).isoformat()
                                        end_date = max(dates).isoformat()
                            elif isinstance(j, dict):
                                # try to locate first list inside dict
                                found=False
                                for v in j.values():
                                    if isinstance(v, list):
                                        recs = len(v)
                                        if recs == 0:
                                            status="empty_array"
                                        else:
                                            status="has_data"
                                            dates=[]
                                            for it in v:
                                                if not isinstance(it, dict): continue
                                                td = it.get("tradeDate") or it.get("d") or it.get("date")
                                                if td is None: continue
                                                try:
                                                    dates.append(datetime.utcfromtimestamp(int(td)/1000.0).date())
                                                except:
                                                    pass
                                            if dates:
                                                start_date=min(dates).isoformat()
                                                end_date=max(dates).isoformat()
                                        found=True
                                        break
                                if not found:
                                    status="invalid_json"
                            else:
                                status="invalid_json"
            except Exception as e:
                status="error"
            ticker = os.path.splitext(fn)[0]
            rows.append((ticker, rel, status, recs, start_date, end_date, size))
    # write manifest
    with open(MANIFEST, "w", newline="", encoding="utf8") as fh:
        w = csv.writer(fh)
        w.writerow(["ticker","relative_path","status","rows","start_date","end_date","filesize_bytes"])
        w.writerows(rows)
    print(f"Wrote manifest: {MANIFEST} (files: {len(rows)})")
    return rows

def normalize_has_data():
    # iterate manifest, process has_data rows
    with open(MANIFEST, "r", encoding="utf8") as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            if r["status"] != "has_data":
                continue
            rel = r["relative_path"].replace("/", os.sep)
            raw_path = os.path.join(RAW, rel)
            ticker = r["ticker"]
            try:
                with open(raw_path, "r", encoding="utf8", errors="ignore") as fh:
                    content = fh.read()
                j = json.loads(content)
            except Exception:
                # try to extract array substring
                try:
                    s = content.find("[")
                    e = content.rfind("]")+1
                    j = json.loads(content[s:e])
                except Exception:
                    print(f"[SKIP] Could not parse {raw_path}")
                    continue
            # find list of records
            records = None
            if isinstance(j, list):
                records = j
            elif isinstance(j, dict):
                for v in j.values():
                    if isinstance(v, list):
                        records = v
                        break
            if records is None:
                print(f"[SKIP] no record list in {raw_path}")
                continue
            rows = []
            for it in records:
                if not isinstance(it, dict):
                    continue
                td = it.get("tradeDate") or it.get("d") or it.get("date")
                try:
                    trade_date = datetime.utcfromtimestamp(int(td)/1000.0).date().isoformat() if td else ""
                except Exception:
                    trade_date = ""
                # canonical columns
                open_ = it.get("open") if it.get("open") is not None else None
                high_ = it.get("high") if it.get("high") is not None else None
                low_ = it.get("low") if it.get("low") is not None else None
                close_ = it.get("close") if it.get("close") is not None else (it.get("v") or None)
                turnover = it.get("turnover")
                share_vol = it.get("shareVolume") or it.get("shareVolume")
                trade_vol = it.get("tradeVolume") or it.get("tradeVolume")
                rows.append({
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "open": open_,
                    "high": high_,
                    "low": low_,
                    "close": close_,
                    "turnover": turnover,
                    "share_volume": share_vol,
                    "trade_volume": trade_vol
                })
            if not rows:
                print(f"[NO ROWS] {ticker}")
                continue
            df = pd.DataFrame(rows)
            # basic validation- drop rows without trade_date or close
            df = df[df["trade_date"].astype(str) != ""]
            df = df[~df["close"].isnull()]
            # coerce numeric types
            for c in ["open","high","low","close","turnover"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            for c in ["share_volume","trade_volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
            df = df.sort_values("trade_date")
            outpath = os.path.join(NORMAL, f"{ticker}.csv")
            df.to_csv(outpath, index=False)
            print(f"WROTE normalized: {outpath} (rows={len(df)})")

if __name__ == "__main__":
    classify_and_summarize()
    normalize_has_data()
    print("Done. Check data/cse/raw/manifest.csv and data/cse/normalized/*.csv")