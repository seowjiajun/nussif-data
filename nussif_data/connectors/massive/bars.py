"""Massive daily & intraday bars -- plain per-ticker REST fetches over the
shared HttpClient. No concurrency needed here (unlike options.py): one ticker,
one call (daily) or one call per chunk_months window (intraday).

Both ride the same vendor endpoint -- Massive's own "Custom Bars (OHLC)"
(cfg["endpoints"]["custom_bars"]) -- daily_bars and intraday_bars
(cfg["composites"]["daily_bars"/"intraday_bars"]) just parameterize it
differently (fixed multiplier=1/timespan=day vs. caller-supplied), so there's
one endpoint template, not two.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from ...core.errors import UpstreamError

BAR_FIELDS = ("open", "high", "low", "close", "volume", "vwap", "trades")

_RENAME = {
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "vw": "vwap",
    "n": "trades",
}
_COLS = ["date", "ticker", "open", "high", "low", "close", "volume", "vwap", "trades"]


def fetch_daily(http, cfg: dict, symbol: str, *, raw: bool = False) -> pd.DataFrame:
    ep = cfg["endpoints"]["custom_bars"]
    d = cfg["composites"]["daily_bars"]
    tk = symbol.upper()
    path = ep["endpoint"].format(
        ticker=tk,
        multiplier=d["multiplier"],
        timespan=d["timespan"],
        start=d["history_start"],
        end=date.today(),
    )
    j = http.get_json(path, ep.get("params", {}))
    rows = j.get("results") or []
    if not rows:
        raise UpstreamError(f"massive: no bars for {tk!r}")
    df = pd.DataFrame(rows)
    if raw:
        return df  # t, o, h, l, c, v, vw, n verbatim
    df = df.rename(columns=_RENAME)
    df["date"] = pd.to_datetime(df["t"], unit="ms").dt.normalize()  # 05:00 UTC -> date
    df["ticker"] = tk
    return df[_COLS]


def combine_daily(frames: list[pd.DataFrame], max_gap_days: int) -> pd.DataFrame:
    df = pd.concat(frames, ignore_index=True)
    return _trim_leading_gaps(df, max_gap_days)


def _trim_leading_gaps(df: pd.DataFrame, max_gap_days: int) -> pd.DataFrame:
    out = []
    for _, g in df.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        big = g["date"].diff().dt.days.gt(max_gap_days)
        if big.any():
            g = g.loc[big[big].index[-1] :].reset_index(drop=True)
        out.append(g)
    return pd.concat(out, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)


def fetch_intraday(http, cfg: dict, ticker: str, multiplier: int, timespan: str) -> pd.DataFrame:
    ep = cfg["endpoints"]["custom_bars"]
    d = cfg["composites"]["intraday_bars"]
    tk = ticker.upper()
    step = pd.DateOffset(months=int(d.get("chunk_months", 6)))
    today = pd.Timestamp(date.today())
    rows: list[dict] = []
    lo = pd.Timestamp(d["history_start"])
    while lo < today:
        hi = min(lo + step, today)
        path = ep["endpoint"].format(
            ticker=tk, multiplier=multiplier, timespan=timespan, start=lo.date(), end=hi.date()
        )
        j = http.get_json(path, ep.get("params", {}))
        page = j.get("results") or []
        nxt = j.get("next_url")
        while nxt:  # vendor split the window
            j = http.get_json(nxt)
            page += j.get("results") or []
            nxt = j.get("next_url")
        rows.extend(page)
        lo = hi + pd.Timedelta(days=1)
    if not rows:
        raise UpstreamError(f"massive: no intraday bars for {tk!r}")
    df = pd.DataFrame(rows).rename(columns=_RENAME)
    df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert("America/New_York")
    df["ticker"] = tk
    keep = ["timestamp", "ticker", *[c for c in BAR_FIELDS if c in df.columns]]
    return df[keep].drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
