"""Massive market data (Polygon.io-compatible REST). Currently: split/div-adjusted
daily bars, one ticker fetched & cached at a time. Needs an API key (see
nussif_data.set_key). Rate-limited on cold fetch; cached thereafter.
"""
from __future__ import annotations

import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

import pandas as pd

from ._catalog import section
from ._config import get_key
from ._util import slice_dates
from .cache import cached

_CFG = section("massive")


def get(path: str, params: dict | None = None, retries: int = 4) -> dict:
    params = dict(params or {})
    params["apiKey"] = get_key("massive")
    url = f"{_CFG['base_url']}{path}?{urllib.parse.urlencode(params)}"
    for a in range(retries):
        try:
            req = urllib.request.Request(url, headers={"Connection": "close"})
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(5 * (a + 1))
                continue
            if e.code in (500, 502, 503, 504) and a < retries - 1:
                time.sleep(2 * (a + 1))
                continue
            raise RuntimeError(f"HTTP {e.code} {path}: {e.read()[:200]!r}")
        except (urllib.error.URLError, http.client.HTTPException, OSError):
            if a < retries - 1:
                time.sleep(2 * (a + 1))
                continue
            raise
    raise RuntimeError(f"gave up on {path}")


def _fetch_bars_one(ticker: str) -> pd.DataFrame:
    spec = _CFG["daily_bars"]
    path = spec["endpoint"].format(ticker=ticker, start=spec["history_start"], end=date.today())
    j = get(path, spec.get("params", {}))
    rows = j.get("results") or []
    if not rows:
        raise RuntimeError(f"no bars returned for {ticker!r}")
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["t"], unit="ms").dt.normalize()   # 05:00 UTC -> date
    df["ticker"] = ticker
    df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close",
                            "v": "volume", "vw": "vwap", "n": "trades"})
    time.sleep(60.0 / _CFG.get("rate_limit_rpm", 40))
    return df[["date", "ticker", "open", "high", "low", "close", "volume", "vwap", "trades"]]


def _trim_leading_gaps(df: pd.DataFrame, max_gap_days: int) -> pd.DataFrame:
    out = []
    for t, g in df.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        big = g["date"].diff().dt.days.gt(max_gap_days)
        if big.any():
            g = g.loc[big[big].index[-1]:].reset_index(drop=True)
        out.append(g)
    return pd.concat(out, ignore_index=True)


def bars(*tickers: str, start=None, end=None, refresh: bool = False) -> pd.DataFrame:
    """Long tidy frame: date, ticker, open, high, low, close, volume, vwap, trades.

    No args -> the catalog default bundle. Multi-year vendor history holes are
    trimmed to the contiguous recent block.
    """
    tk = [t.upper() for t in tickers] or list(_CFG["daily_bars"]["default_tickers"])
    frames = [cached(f"massive/bars/{t}", lambda t=t: _fetch_bars_one(t), refresh=refresh) for t in tk]
    df = pd.concat(frames, ignore_index=True)
    df = _trim_leading_gaps(df, _CFG["daily_bars"].get("trim_gap_days", 15))
    return slice_dates(df.sort_values(["ticker", "date"]).reset_index(drop=True), start, end)
