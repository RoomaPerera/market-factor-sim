from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

FACTORS = {
    "cse": {
        "root": DATA / "cse",
        "raw": DATA / "cse" / "raw",
        "normalized": DATA / "cse" / "normalized",
        "processed": DATA / "cse" / "processed",
    },
    "usd_lkr": {
        "root": DATA / "usd_lkr",
        "raw": DATA / "usd_lkr" / "raw",
        "normalized": DATA / "usd_lkr" / "normalized",
        "processed": DATA / "usd_lkr" / "processed",
    }
}