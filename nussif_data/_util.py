from __future__ import annotations

import os
from collections.abc import Sequence

import pandas as pd

from .cache import out_dir, write_parquet


def flatten_symbols(args: Sequence) -> list[str]:
    """Accept either varargs ("A", "B") or a single list/tuple/set passed as the
    sole positional argument (["A", "B"]) -- never explode a lone string into
    its characters."""
    if len(args) == 1 and isinstance(args[0], (list, tuple, set, frozenset)):
        args = args[0]
    return [str(s) for s in args]


def to_ts(x) -> pd.Timestamp | None:
    """Loose date parse: None -> None; '2010' -> 2010-01-01; also YYYY-MM,
    YYYY-MM-DD, datetime/date/Timestamp."""
    if x is None:
        return None
    return pd.Timestamp(x)


def slice_dates(df: pd.DataFrame, start=None, end=None, col: str = "date") -> pd.DataFrame:
    s, e = to_ts(start), to_ts(end)
    out = df
    if s is not None:
        out = out[out[col] >= s]
    if e is not None:
        out = out[out[col] <= e]
    return out.reset_index(drop=True)


def outer_merge_on_date(frames: list[pd.DataFrame]) -> pd.DataFrame:
    it = iter(frames)
    df = next(it)
    for f in it:
        df = df.merge(f, on="date", how="outer")
    return df.sort_values("date").reset_index(drop=True)


_WRITERS = {
    "parquet": lambda df, p: df.to_parquet(p, index=False),
    "pq": lambda df, p: df.to_parquet(p, index=False),
    "csv": lambda df, p: df.to_csv(p, index=False),
    "json": lambda df, p: df.to_json(p, orient="records", date_format="iso"),
    "feather": lambda df, p: df.to_feather(p),
    "ft": lambda df, p: df.to_feather(p),
    "xlsx": lambda df, p: df.to_excel(p, index=False),
}


def write_frame(df: pd.DataFrame, path, *, metadata: dict[str, str] | None = None) -> str:
    """Write `df` to `path`; format from the extension (.parquet/.csv/.json/.feather/.xlsx).

    A relative `path` resolves against `cache.out_dir()` ($NUSSIF_DATA_OUT, else
    the cwd) -- never hardcode a folder in a script/notebook and expect it to
    exist on someone else's machine; each user points $NUSSIF_DATA_OUT at
    wherever they want their own output to land. An absolute path is used as-is.

    `metadata` -- string key/value pairs embedded as real Parquet file-level
    metadata (readable without loading the data: `pyarrow.parquet.read_schema
    (path).metadata`), not just a row/column in the data itself. Parquet-only
    (.parquet/.pq); ignored for other formats, which have no equivalent
    concept. Meant for provenance that should travel with the file forever --
    e.g. which columns a connector added/renamed vs. which are untouched
    vendor fields -- see connectors/massive/options.py's own use of this."""
    p = str(path)
    if not os.path.isabs(p):
        p = os.path.join(out_dir(), p)
    ext = os.path.splitext(p)[1].lower().lstrip(".")
    if ext not in _WRITERS:
        raise ValueError(
            f"unsupported output extension {ext!r}; use one of "
            f"{sorted(set(_WRITERS) - {'pq', 'ft'})}"
        )
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)
    if metadata and ext in ("parquet", "pq"):
        write_parquet(df, p, metadata)
    else:
        _WRITERS[ext](df, p)
    return p
