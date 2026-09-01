"""CBOE connector -- volatility index EOD levels, one <SYM>_History.csv per symbol."""
from __future__ import annotations

import io

import pandas as pd

from ..core import Connector, Dataset, HttpClient, NoAuth
from ..core.schema import VOL_INDEX_WIDE


def _read_history(raw: bytes) -> pd.DataFrame:
    """The CSV as CBOE ships it (title line skipped, DATE parsed) -- original
    column names: DATE, OPEN, HIGH, LOW, CLOSE (or DATE, <SYM>)."""
    lines = raw.decode("utf-8", "replace").splitlines()
    hdr = next(i for i, ln in enumerate(lines) if ln.upper().lstrip().startswith("DATE"))
    df = pd.read_csv(io.StringIO("\n".join(lines[hdr:])))
    df.columns = [c.strip() for c in df.columns]
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df.dropna(subset=[date_col]).reset_index(drop=True)


def _parse_history(sym: str, raw: bytes) -> pd.DataFrame:
    """Tidy: date + one numeric column named `sym` (the close level)."""
    df = _read_history(raw)
    df.columns = [c.upper() for c in df.columns]
    val = "CLOSE" if "CLOSE" in df.columns else [c for c in df.columns if c != "DATE"][-1]
    out = df[["DATE", val]].rename(columns={"DATE": "date", val: sym})
    out[sym] = pd.to_numeric(out[sym], errors="coerce")
    return out.dropna().sort_values("date").reset_index(drop=True)


class CboeConnector(Connector):
    name = "cboe"
    primary_method = "vol_index"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.http = HttpClient(auth=NoAuth(), name="cboe",
                               rate_limit_rpm=cfg.get("rate_limit_rpm"))

    def datasets(self) -> list[Dataset]:
        return [Dataset("vol_index", VOL_INDEX_WIDE,
                        description="CBOE volatility index EOD close levels "
                                    "(VIX, VIX1D/9D/3M/6M, VVIX, VXN, RVX, VXTLT, GVZ, OVX, SKEW, …)")]

    # -- dataset accessor --
    def vol_index(self, *symbols, start=None, end=None, refresh=False, out=None, raw=False):
        """CBOE vol-index EOD levels.
        raw=False -> wide frame (date + one close col per symbol).
        raw=True  -> {symbol: History CSV verbatim (DATE, OPEN, HIGH, LOW, CLOSE)}."""
        return self.fetch("vol_index", symbols, start=start, end=end,
                          refresh=refresh, out=out, raw=raw)

    def _fetch_symbol(self, dataset: str, symbol: str, raw: bool = False) -> pd.DataFrame:
        d = self.cfg["datasets"][dataset]
        sym = symbol.upper()
        b = self.http.get_bytes(d["url_template"].format(symbol=sym))
        return _read_history(b) if raw else _parse_history(sym, b)

    def _cache_symbol(self, dataset: str, symbol: str) -> str:
        return symbol.upper()

    def ping(self) -> bool:
        """Explicit liveness probe (network)."""
        self.http.get_bytes(self.cfg["datasets"]["vol_index"]["url_template"].format(symbol="VIX"))
        return True
