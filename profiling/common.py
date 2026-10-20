"""Shared loader for profiling scripts."""
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "include" / "data" / "nammamart"

FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "increment": "orders_2026-10-01_increment.csv",
}

# Text values that mean "missing" but pandas would NOT treat as NaN when keep_default_na=False
PLACEHOLDERS = {"", "NULL", "NONE", "NAN", "N/A", "NA", "NIL", "-"}


def load(name: str) -> pd.DataFrame:
    """Read a CSV with every column as text and no automatic NaN conversion,
    so we see the data exactly as the client sent it."""
    return pd.read_csv(DATA_DIR / FILES[name], dtype=str, keep_default_na=False)


def is_missing(series: pd.Series) -> pd.Series:
    """True where the value is blank or a placeholder like 'NULL' / 'N/A'."""
    return series.str.strip().str.upper().isin(PLACEHOLDERS)
