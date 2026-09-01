"""CBOE volatility indices (daily EOD). One <SYM>_History.csv per symbol; each is
fetched and cached individually so you only pull what you ask for.
"""
from __future__ import annotations

import io

import pandas as pd

from ._catalog import endpoint
from ._util import outer_merge_on_date, slice_dates
from .cache import cached, http_get


def _parse_history(sym: str, raw: bytes) -> pd.DataFrame:
    # CBOE history CSVs sometimes carry a title line before the real header.
    lines = raw.decode("utf-8", "replace").splitlines()
    hdr = next(i for i, ln in enumerate(lines) if ln.upper().lstrip().startswith("DATE"))
    df = pd.read_csv(io.StringIO("\n".join(lines[hdr:])))
    df.columns = [c.strip().upper() for c in df.columns]
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df = df.dropna(subset=["DATE"])
    val = "CLOSE" if "CLOSE" in df.columns else [c for c in df.columns if c != "DATE"][-1]
    out = df[["DATE", val]].rename(columns={"DATE": "date", val: sym})
    out[sym] = pd.to_numeric(out[sym], errors="coerce")
    return out.dropna().sort_values("date").reset_index(drop=True)


def _fetch_one(sym: str, base_url: str, suffix: str) -> pd.DataFrame:
    return _parse_history(sym, http_get(base_url + sym + suffix))


def history(*symbols: str, start=None, end=None, refresh: bool = False) -> pd.DataFrame:
    """Wide frame: `date` + one column per requested symbol (index close level).

    No args -> the catalog default bundle. Any CBOE index that publishes
    <SYM>_History.csv works if named explicitly, e.g. cboe("VXD", "EVZ").
    """
    ep = endpoint("cboe", "daily_prices")
    syms = [s.upper() for s in symbols] or list(ep["symbols"])
    frames = [
        cached(f"cboe/{s}", lambda s=s: _fetch_one(s, ep["base_url"], ep["suffix"]), refresh=refresh)
        for s in syms
    ]
    return slice_dates(outer_merge_on_date(frames), start, end)
