"""CBOE connector -- volatility index EOD levels, one <SYM>_History.csv per symbol."""
from __future__ import annotations

import io

import pandas as pd

from ..core import Connector, Dataset, HttpClient, NoAuth
from ..core.schema import VOL_INDEX_WIDE


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


class CboeConnector(Connector):
    name = "cboe"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.http = HttpClient(auth=NoAuth(), name="cboe",
                               rate_limit_rpm=cfg.get("rate_limit_rpm"))

    def datasets(self) -> list[Dataset]:
        d = self.cfg["datasets"]["vol_index"]
        return [Dataset("vol_index", VOL_INDEX_WIDE,
                        description="CBOE volatility index EOD close levels",
                        default_symbols=d["default_symbols"])]

    def _fetch_symbol(self, dataset: str, symbol: str) -> pd.DataFrame:
        d = self.cfg["datasets"][dataset]
        sym = symbol.upper()
        return _parse_history(sym, self.http.get_bytes(d["base_url"] + sym + d["suffix"]))

    def _cache_symbol(self, dataset: str, symbol: str) -> str:
        return symbol.upper()

    def ping(self) -> bool:
        """Explicit liveness probe (network)."""
        self.http.get_bytes(self.cfg["datasets"]["vol_index"]["base_url"] + "VIX_History.csv")
        return True
