"""FRED series (free CSV, no key). Each series fetched and cached by its FRED id.
Callers may pass catalog aliases (e.g. 'baa10y') or any raw FRED id (e.g. 'UNRATE').
"""
from __future__ import annotations

import io

import pandas as pd

from ._catalog import section
from ._util import outer_merge_on_date, slice_dates
from .cache import cached, http_get


def _fetch_one(fred_id: str, colname: str, url_template: str, history_start: str) -> pd.DataFrame:
    raw = http_get(url_template.format(id=fred_id, history_start=history_start))
    df = pd.read_csv(io.BytesIO(raw))
    df.columns = ["date", colname]                 # 1st column is the date, whatever the header
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df[colname] = pd.to_numeric(df[colname], errors="coerce")   # missing marked '.'
    return df.dropna().sort_values("date").reset_index(drop=True)


def series(*ids: str, start=None, end=None, refresh: bool = False) -> pd.DataFrame:
    """Wide frame: `date` + one column per requested series.

    No args -> the catalog default bundle. Args may be aliases ('baa10y', 'nfci')
    or raw FRED ids ('T10Y2Y', 'UNRATE'); columns are named by the alias when one
    exists, else the lowercased id.
    """
    cfg = section("fred")
    alias2id: dict = cfg.get("series", {})
    id2alias = {v.lower(): k for k, v in alias2id.items()}
    reqs = list(ids) or list(alias2id.keys())

    frames = []
    for r in reqs:
        fid = alias2id.get(r, r)                          # alias -> id, or treat as id
        col = r if r in alias2id else id2alias.get(fid.lower(), fid.lower())
        frames.append(cached(
            f"fred/{fid}",
            lambda fid=fid, col=col: _fetch_one(fid, col, cfg["url_template"], cfg["history_start"]),
            refresh=refresh,
        ))
    return slice_dates(outer_merge_on_date(frames), start, end)
