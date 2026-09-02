"""Lightweight result validation -- presence + dtype family. Not pandera.

Column spec values:
    "date"      -> calendar date; COERCED to tz-naive midnight datetime64 in validate()
                   so frames from different sources merge on `date` without offset bugs
    "datetime"  -> datetime64[ns], any time-of-day (kept as-is; use for intraday stamps)
    "float"     -> numeric (coerced)
    "int"       -> integer-ish
    "string"    -> object/string
    "*"         -> any remaining columns must match this (wildcard)
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from .errors import SchemaError

_FAMILIES = {
    "date": lambda s: pd.api.types.is_datetime64_any_dtype(s),  # also normalised in validate()
    "datetime": lambda s: pd.api.types.is_datetime64_any_dtype(s),
    "float": lambda s: pd.api.types.is_numeric_dtype(s),
    "int": lambda s: pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s),
    "string": lambda s: pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s),
    "*": lambda s: True,
}


def _naive_midnight(s: pd.Series) -> pd.Series:
    """Coerce to datetime, drop any tz (in UTC), floor to midnight."""
    s = pd.to_datetime(s, errors="coerce")
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return s.dt.normalize()


@dataclass
class Schema:
    columns: dict[str, str] = field(default_factory=dict)  # name -> family; "*" allowed as a key

    def validate(self, df: pd.DataFrame, *, where: str = "") -> pd.DataFrame:
        tag = f" [{where}]" if where else ""
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise SchemaError(f"empty / non-frame result{tag}")
        named = {k: v for k, v in self.columns.items() if k != "*"}
        wild = self.columns.get("*")

        # normalise every declared `date` column to tz-naive midnight (one copy)
        date_cols = [c for c, f in named.items() if f == "date" and c in df.columns]
        if date_cols:
            df = df.copy()
            for c in date_cols:
                df[c] = _naive_midnight(df[c])

        for col, fam in named.items():
            if col not in df.columns:
                raise SchemaError(f"missing column '{col}'{tag}; got {list(df.columns)}")
            if not _FAMILIES[fam](df[col]):
                raise SchemaError(f"column '{col}' expected {fam}, got {df[col].dtype}{tag}")
        if wild:
            for col in df.columns:
                if col in named:
                    continue
                if not _FAMILIES[wild](df[col]):
                    raise SchemaError(f"column '{col}' expected {wild}, got {df[col].dtype}{tag}")
        return df


# reusable dataset schemas
VOL_INDEX_WIDE = Schema({"date": "date", "*": "float"})
MACRO_WIDE = Schema({"date": "date", "*": "float"})
BARS_LONG = Schema(
    {
        "date": "date",
        "ticker": "string",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "float",
        "vwap": "float",
        "trades": "float",
    }
)

# canonical option-chain slice -- one row per contract; every provider maps to this.
# iv / greeks / open_interest are optional (present only if the vendor supplies them).
OPTION_CHAIN = Schema(
    {
        "symbol": "string",
        "date": "date",
        "expiration": "date",
        "strike": "float",
        "right": "string",  # 'C' | 'P'
        "bid": "float",
        "ask": "float",
        "*": "float",
    }
)
