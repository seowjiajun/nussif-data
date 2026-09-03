"""Massive connector (Polygon.io-compatible). Split/div-adjusted daily bars,
one ticker per REST call. Needs an API key ($MASSIVE_API_KEY or ~/.config/nussif-data/keys.env).
Rate limiting + retry are handled by the shared HttpClient.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from .. import _util
from .._config import get_key
from ..cache import cached
from ..core import Connector, Dataset, HttpClient, QueryKeyAuth
from ..core.errors import UpstreamError
from ..core.schema import BARS_LONG

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


class MassiveConnector(Connector):
    name = "massive"
    primary_method = "bars"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        auth_cfg = cfg.get("auth", {})
        vendor = auth_cfg.get("vendor", "massive")
        self.http = HttpClient(
            base_url=cfg["base_url"],
            rate_limit_rpm=cfg.get("rate_limit_rpm", 40),
            auth=QueryKeyAuth(auth_cfg.get("param", "apiKey"), lambda: get_key(vendor)),
            name="massive",
        )

    def datasets(self) -> list[Dataset]:
        return [
            Dataset(
                "daily_bars",
                BARS_LONG,
                needs_key=True,
                description="split/div-adjusted daily OHLCV",
            )
        ]

    # -- dataset accessor --
    def bars(self, *tickers, start=None, end=None, refresh=False, out=None, field=None, raw=False):
        """Adjusted daily OHLCV.

        raw=True    -> {ticker: Massive payload verbatim (t, o, h, l, c, v, vw, n)}.
        field=None  -> long tidy frame (date, ticker, open, high, low, close,
          volume, vwap, trades).
        field="close" (etc.) -> WIDE: date + one column per ticker (matches
          nd.cboe / nd.fred shape).
        """
        if raw:
            if field:
                raise ValueError("field= is not compatible with raw=True")
            return self.fetch(
                "daily_bars", tickers, start=start, end=end, refresh=refresh, raw=True
            )
        if field is not None and field not in BAR_FIELDS:
            raise ValueError(f"field must be one of {BAR_FIELDS}")
        df = self.fetch(
            "daily_bars", tickers, start=start, end=end, refresh=refresh, out=None if field else out
        )
        if field is None:
            return df
        wide = df.pivot(index="date", columns="ticker", values=field).reset_index()
        wide.columns.name = None
        if out:
            _util.write_frame(wide, out)
        return wide

    # -- intraday (Polygon aggregates at an arbitrary multiplier/timespan) --
    def bars_intraday(
        self,
        *tickers,
        multiplier: int = 5,
        timespan: str = "minute",
        start=None,
        end=None,
        rth: bool = True,
        refresh: bool = False,
        out=None,
    ):
        """Intraday OHLCV aggregates. One tidy long frame:
        (timestamp [tz-aware, America/New_York], ticker, open, high, low, close,
        volume, vwap, trades).

        The full history for each (ticker, multiplier, timespan) is fetched once
        and cached; `start` / `end` then slice the cached frame. `rth=True` keeps
        only the 09:30-16:00 ET regular session (drop for overnight studies).
        """
        d = self.cfg["datasets"]["intraday_bars"]
        lo = pd.Timestamp(start) if start else pd.Timestamp(d["history_start"])
        hi = pd.Timestamp(end) if end else pd.Timestamp(date.today())
        frames = []
        for tk in _util.flatten_symbols(tickers):
            key = f"{self.name}/intraday_bars/{tk.upper()}_{multiplier}{timespan[:3]}"
            frames.append(
                cached(
                    key,
                    lambda tk=tk: self._fetch_intraday(tk, multiplier, timespan),
                    refresh=refresh,
                )
            )
        df = pd.concat(frames, ignore_index=True)
        df = df[
            (df["timestamp"] >= lo.tz_localize("America/New_York"))
            & (df["timestamp"] < (hi + pd.Timedelta(days=1)).tz_localize("America/New_York"))
        ]
        if rth:
            m = df["timestamp"].dt.hour * 60 + df["timestamp"].dt.minute
            df = df[(m >= 570) & (m < 960)]
        df = df.sort_values(["ticker", "timestamp"]).reset_index(drop=True)
        if out:
            _util.write_frame(df, out)
        return df

    def _fetch_intraday(self, ticker: str, multiplier: int, timespan: str) -> pd.DataFrame:
        d = self.cfg["datasets"]["intraday_bars"]
        tk = ticker.upper()
        step = pd.DateOffset(months=int(d.get("chunk_months", 6)))
        today = pd.Timestamp(date.today())
        rows: list[dict] = []
        lo = pd.Timestamp(d["history_start"])
        while lo < today:
            hi = min(lo + step, today)
            path = d["endpoint"].format(
                ticker=tk, multiplier=multiplier, timespan=timespan, start=lo.date(), end=hi.date()
            )
            j = self.http.get_json(path, d.get("params", {}))
            page = j.get("results") or []
            nxt = j.get("next_url")
            while nxt:  # vendor split the window
                j = self.http.get_json(nxt)
                page += j.get("results") or []
                nxt = j.get("next_url")
            rows.extend(page)
            lo = hi + pd.Timedelta(days=1)
        if not rows:
            raise UpstreamError(f"massive: no intraday bars for {tk!r}")
        df = pd.DataFrame(rows).rename(columns=_RENAME)
        df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(
            "America/New_York"
        )
        df["ticker"] = tk
        keep = ["timestamp", "ticker", *[c for c in BAR_FIELDS if c in df.columns]]
        return df[keep].drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)

    def _cache_symbol(self, dataset: str, symbol: str) -> str:
        return symbol.upper()

    def _fetch_symbol(self, dataset: str, symbol: str, raw: bool = False) -> pd.DataFrame:
        d = self.cfg["datasets"][dataset]
        tk = symbol.upper()
        path = d["endpoint"].format(ticker=tk, start=d["history_start"], end=date.today())
        j = self.http.get_json(path, d.get("params", {}))
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

    def _combine(self, dataset: str, frames: list[pd.DataFrame]) -> pd.DataFrame:
        df = pd.concat(frames, ignore_index=True)
        return _trim_leading_gaps(df, self.cfg["datasets"][dataset].get("trim_gap_days", 15))

    def health(self) -> bool:
        try:
            get_key("massive")  # cheap: is a key configured at all?
            return True
        except Exception:
            return False


def _trim_leading_gaps(df: pd.DataFrame, max_gap_days: int) -> pd.DataFrame:
    out = []
    for _, g in df.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        big = g["date"].diff().dt.days.gt(max_gap_days)
        if big.any():
            g = g.loc[big[big].index[-1] :].reset_index(drop=True)
        out.append(g)
    return pd.concat(out, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)
