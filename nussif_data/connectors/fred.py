"""FRED connector -- one series per FRED id, FRED's own symbology verbatim
(BAA10Y, DGS10, NFCI, ...). No invented aliases: look an id up at
https://fred.stlouisfed.org, or see README.md for the ones this project uses.
"""

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
        self.http = HttpClient(auth=NoAuth(), name="fred", rate_limit_rpm=cfg.get("rate_limit_rpm"))

    def datasets(self) -> list[Dataset]:
        return [
            Dataset(
                "macro_series",
                MACRO_WIDE,
                description="any FRED series, by its own id (e.g. BAA10Y, DGS10, NFCI)",
            )
        ]

    # -- dataset accessor --
    def series(self, *ids, start=None, end=None, refresh=False, out=None, raw=False):
        """FRED series, by FRED's own id (case-insensitive; columns come back
        uppercased, matching FRED).
        raw=False -> wide frame (date + one col per id).
        raw=True  -> {id: CSV verbatim (observation_date, <ID>; '.' for missing)}."""
        return self.fetch(
            "macro_series", ids, start=start, end=end, refresh=refresh, out=out, raw=raw
        )

    def _cache_symbol(self, dataset: str, symbol: str) -> str:
        return symbol.upper()

    def _fetch_symbol(self, dataset: str, symbol: str, raw: bool = False) -> pd.DataFrame:
        d = self.cfg["datasets"][dataset]
        fid = symbol.upper()
        payload = self.http.get_bytes(
            d["url_template"].format(id=fid, history_start=d["history_start"])
        )
        df = pd.read_csv(io.BytesIO(payload))
        if raw:
            return df  # observation_date, <ID>; '.' kept
        df.columns = ["date", fid]  # 1st col is the date whatever the header
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df[fid] = pd.to_numeric(df[fid], errors="coerce")  # missing marked '.'
        return df.dropna().sort_values("date").reset_index(drop=True)
