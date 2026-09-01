from __future__ import annotations

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
