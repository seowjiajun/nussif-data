"""Lightweight result validation -- presence + dtype family. Not pandera.

Column spec values:
    "datetime"  -> datetime64[ns]
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
    "datetime": lambda s: pd.api.types.is_datetime64_any_dtype(s),
    "float": lambda s: pd.api.types.is_numeric_dtype(s),
    "int": lambda s: pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s),
    "string": lambda s: pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s),
    "*": lambda s: True,
}


@dataclass
class Schema:
    columns: dict[str, str] = field(default_factory=dict)   # name -> family; "*" allowed as a key

    def validate(self, df: pd.DataFrame, *, where: str = "") -> pd.DataFrame:
        tag = f" [{where}]" if where else ""
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise SchemaError(f"empty / non-frame result{tag}")
        named = {k: v for k, v in self.columns.items() if k != "*"}
        wild = self.columns.get("*")
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
VOL_INDEX_WIDE = Schema({"date": "datetime", "*": "float"})
MACRO_WIDE = Schema({"date": "datetime", "*": "float"})
BARS_LONG = Schema({
    "date": "datetime", "ticker": "string",
    "open": "float", "high": "float", "low": "float", "close": "float",
    "volume": "float", "vwap": "float", "trades": "float",
})
