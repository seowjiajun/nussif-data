"""FRED connector -- one series per FRED id. Callers pass catalog aliases
('baa10y') or any raw id ('UNRATE'); columns are named by alias when one exists."""
from __future__ import annotations

import io

import pandas as pd

from ..core import Connector, Dataset, HttpClient, NoAuth
from ..core.schema import MACRO_WIDE


class FredConnector(Connector):
    name = "fred"
    primary_method = "series"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.http = HttpClient(auth=NoAuth(), name="fred",
                               rate_limit_rpm=cfg.get("rate_limit_rpm"))
        d = cfg["datasets"]["macro_series"]
        self._alias2id: dict = d.get("aliases", {})
        self._id2alias = {v.lower(): k for k, v in self._alias2id.items()}

    def datasets(self) -> list[Dataset]:
        return [Dataset("macro_series", MACRO_WIDE,
                        description="FRED macro/funding series (any id)")]

    # -- dataset accessor --
    def series(self, *ids, start=None, end=None, refresh=False, out=None, raw=False):
        """FRED series. Args are catalog aliases or raw FRED ids.
        raw=False -> wide frame (date + one aliased col per series).
        raw=True  -> {id: CSV verbatim (observation_date, <ID>; '.' for missing)}."""
        return self.fetch("macro_series", ids, start=start, end=end,
                          refresh=refresh, out=out, raw=raw)

    def _resolve(self, symbol: str) -> tuple[str, str]:
        """(fred_id, column_name)."""
        fid = self._alias2id.get(symbol, symbol)
        col = symbol if symbol in self._alias2id else self._id2alias.get(fid.lower(), fid.lower())
        return fid, col

    def _cache_symbol(self, dataset: str, symbol: str) -> str:
        return self._resolve(symbol)[0]                     # cache by FRED id, not by alias

    def _fetch_symbol(self, dataset: str, symbol: str, raw: bool = False) -> pd.DataFrame:
        d = self.cfg["datasets"][dataset]
        fid, col = self._resolve(symbol)
        payload = self.http.get_bytes(
            d["url_template"].format(id=fid, history_start=d["history_start"]))
        df = pd.read_csv(io.BytesIO(payload))
        if raw:
            return df                                       # observation_date, <ID>; '.' kept
        df.columns = ["date", col]                          # 1st col is the date whatever the header
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df[col] = pd.to_numeric(df[col], errors="coerce")   # missing marked '.'
        return df.dropna().sort_values("date").reset_index(drop=True)
