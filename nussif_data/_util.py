from __future__ import annotations

import os

import pandas as pd


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


def write_frame(df: pd.DataFrame, path) -> str:
    """Write `df` to `path`; format from the extension (.parquet/.csv/.json/.feather/.xlsx)."""
    p = str(path)
    ext = os.path.splitext(p)[1].lower().lstrip(".")
    if ext not in _WRITERS:
        raise ValueError(
            f"unsupported output extension {ext!r}; use one of "
            f"{sorted(set(_WRITERS) - {'pq', 'ft'})}"
        )
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)
    _WRITERS[ext](df, p)
    return p
