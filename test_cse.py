import os, requests, json
from datetime import date

CSE_BASE = os.getenv("CSE_API_BASE", "https://www.cse.lk/api").rstrip('/')
symbols = ["ABAN.N0000", "ABAN", "ABAN.N", "ABAN.N000"]  # variants to try
endpoint = f"{CSE_BASE}/chartData"

print("Testing endpoint:", endpoint)
for sym in symbols:
    try:
        payload = {"symbol": sym, "chartId": 1, "period": 1}
        print("\n=== Trying symbol:", sym)
        r = requests.post(endpoint, data=payload, timeout=20)
        print("Status:", r.status_code)
        text = r.text
        print("Response length:", len(text))
        preview = text[:2000]
        print("Preview (first 2000 chars):")
        print(preview)
        # Try parsing JSON summary
        try:
            j = r.json()
            print("Type:", type(j))
            if isinstance(j, dict):
                print("Top-level keys:", list(j.keys()))
                if "chartData" in j:
                    cd = j["chartData"]
                    try:
                        print("chartData length:", len(cd))
                        if len(cd) > 0:
                            print("sample item keys:", list(cd[0].keys()))
                    except Exception:
                        print("chartData is present but not a list; repr:", repr(cd)[:400])
            elif isinstance(j, list):
                print("Top-level is list. length:", len(j))
            else:
                print("Top-level JSON type not dict/list:", type(j))
        except Exception as je:
            print("Could not parse JSON:", je)
    except Exception as ex:
        print("Request failed:", type(ex), ex)