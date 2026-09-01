"""Massive connector (Polygon.io-compatible). Split/div-adjusted daily bars,
one ticker per REST call. Needs an API key (nussif_data.set_key / $MASSIVE_API_KEY).
Rate limiting + retry are handled by the shared HttpClient.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from .. import _util
from .._config import get_key
from ..core import Connector, Dataset, HttpClient, QueryKeyAuth
from ..core.errors import UpstreamError
from ..core.schema import BARS_LONG

_RENAME = {"o": "open", "h": "high", "l": "low", "c": "close",
           "v": "volume", "vw": "vwap", "n": "trades"}
_COLS = ["date", "ticker", "open", "high", "low", "close", "volume", "vwap", "trades"]


class MassiveConnector(Connector):
    name = "massive"

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
        d = self.cfg["datasets"]["daily_bars"]
        return [Dataset("daily_bars", BARS_LONG, needs_key=True,
                        description="split/div-adjusted daily OHLCV",
                        default_symbols=d["default_symbols"])]

    def _cache_symbol(self, dataset: str, symbol: str) -> str:
        return symbol.upper()

    def _fetch_symbol(self, dataset: str, symbol: str) -> pd.DataFrame:
        d = self.cfg["datasets"][dataset]
        tk = symbol.upper()
        path = d["endpoint"].format(ticker=tk, start=d["history_start"], end=date.today())
        j = self.http.get_json(path, d.get("params", {}))
        rows = j.get("results") or []
        if not rows:
            raise UpstreamError(f"massive: no bars for {tk!r}")
        df = pd.DataFrame(rows).rename(columns=_RENAME)
        df["date"] = pd.to_datetime(df["t"], unit="ms").dt.normalize()   # 05:00 UTC -> date
        df["ticker"] = tk
        return df[_COLS]

    def _combine(self, dataset: str, frames: list[pd.DataFrame]) -> pd.DataFrame:
        df = pd.concat(frames, ignore_index=True)
        return _trim_leading_gaps(df, self.cfg["datasets"][dataset].get("trim_gap_days", 15))

    def health(self) -> bool:
        try:
            get_key("massive")   # cheap: is a key configured at all?
            return True
        except Exception:  # noqa: BLE001
            return False


def _trim_leading_gaps(df: pd.DataFrame, max_gap_days: int) -> pd.DataFrame:
    out = []
    for _, g in df.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        big = g["date"].diff().dt.days.gt(max_gap_days)
        if big.any():
            g = g.loc[big[big].index[-1]:].reset_index(drop=True)
        out.append(g)
    return pd.concat(out, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)
