# src/env.py
from pathlib import Path
from dotenv import load_dotenv
import os
from typing import Optional

# Locate project root (assumes this file is inside src/)
ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

# Load .env into process environment
load_dotenv(dotenv_path=ENV_PATH, override=False)

# Convenience getters with optional defaults
def get_env(name: str, default: Optional[str]=None) -> Optional[str]:
    val = os.getenv(name)
    if val is None:
        return default
    return val

# Examples of environment variables
POSTGRES_USER = get_env("POSTGRES_USER")
POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD")
POSTGRES_DB = get_env("POSTGRES_DB")
POSTGRES_HOST = get_env("POSTGRES_HOST", "localhost")
POSTGRES_PORT = get_env("POSTGRES_PORT", "5432")
CSE_API_BASE = get_env("CSE_API_BASE")