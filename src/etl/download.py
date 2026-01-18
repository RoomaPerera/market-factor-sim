# src/etl/download.py

import requests
import os
import json
from datetime import datetime, timezone
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _ms_to_date(ms):
    try:
        return datetime.utcfromtimestamp(int(ms) / 1000.0).date()
    except Exception:
        return None


def _to_epoch_ms(d):
    """Convert date / datetime / string to epoch milliseconds (UTC)."""
    if d is None:
        return None

    if isinstance(d, (int, float)):
        return int(d)

    try:
        if isinstance(d, datetime):
            dt = d
        else:
            dt = pd.to_datetime(d).to_pydatetime()

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return int(dt.timestamp() * 1000)
    except Exception:
        return None


# -------------------------------------------------------------------
# Base URL normalization
# -------------------------------------------------------------------

_raw_base = os.getenv("CSE_API_BASE", "https://www.cse.lk").rstrip("/")
if _raw_base.endswith("/api"):
    CSE_BASE = _raw_base[:-4]
else:
    CSE_BASE = _raw_base


# -------------------------------------------------------------------
# Main downloader
# -------------------------------------------------------------------

def fetch_cse_chart(symbol: str, start=None, end=None) -> pd.DataFrame:
    """
    Fetch daily OHLC chart data from CSE.

    Required POST params (verified from browser):
      - symbol
      - chartId
      - period
      - fromDate (epoch ms)
      - toDate   (epoch ms)

    Returns:
      DataFrame with columns:
      trade_date, open, high, low, close, turnover, shareVolume, tradeVolume
    """

    endpoint = f"{CSE_BASE}/api/charts"

    from_ms = _to_epoch_ms(start)
    to_ms = _to_epoch_ms(end)

    payload = {
        "symbol": symbol,
        "chartId": 1,
        "period": 1,
        "fromDate": str(from_ms),
        "toDate": str(to_ms),
    }

    headers = {
        "Referer": f"https://www.cse.lk/company-profile?symbol={symbol}",
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    token = os.getenv("CSE_ACCESS_TOKEN")
    if token:
        headers["Cookie"] = f"accessToken={token}"

    # ---------------------------
    # Request
    # ---------------------------
    r = requests.post(endpoint, data=payload, headers=headers, timeout=60)

    # Save raw response for debugging
    outdir = os.path.join(os.getcwd(), "data", "raw")
    os.makedirs(outdir, exist_ok=True)
    fname = os.path.join(
        outdir, f"charts_{symbol}_{int(datetime.utcnow().timestamp())}.json"
    )

    try:
        raw_json = r.json()
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(raw_json, f, indent=2, ensure_ascii=False)
    except Exception:
        with open(fname, "w", encoding="utf-8") as f:
            f.write(r.text[:200000])
        raise RuntimeError("CSE returned non-JSON response")

    r.raise_for_status()

    # ---------------------------
    # Parse response
    # ---------------------------
    if not raw_json:
        return pd.DataFrame([])

    if isinstance(raw_json, dict):
        records = None
        for key in ("data", "chartData", "series", "items", "rows"):
            if key in raw_json and isinstance(raw_json[key], list):
                records = raw_json[key]
                break
        if records is None:
            return pd.DataFrame([])
    elif isinstance(raw_json, list):
        records = raw_json
    else:
        return pd.DataFrame([])

    rows = []
    for it in records:
        trade_date = _ms_to_date(
            it.get("tradeDate") or it.get("d") or it.get("date")
        )

        rows.append({
            "trade_date": trade_date,
            "open": _safe_float(it.get("open")),
            "high": _safe_float(it.get("high")),
            "low": _safe_float(it.get("low")),
            "close": _safe_float(it.get("close")),
            "turnover": _safe_float(it.get("turnover")),
            "shareVolume": _safe_int(it.get("shareVolume")),
            "tradeVolume": _safe_int(it.get("tradeVolume")),
        })

    df = pd.DataFrame(rows).dropna(subset=["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


def _safe_float(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _safe_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None