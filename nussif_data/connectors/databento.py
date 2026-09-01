"""Databento connector -- historical OPRA option chains.

Two-stage per (symbol, date), because pulling a whole parent symbol drags in the
full ~1.7M-contract universe (~$10/name/month):

  1. `definition`  -> every listed contract for <SYM>.OPT on the date
     (strike / expiry / call-put), ~$0.01/day
  2. filter to a moneyness / DTE band  -> ~1-2k contracts
  3. `cbbo-1m`     -> 1-minute consolidated NBBO for just those, in the minute
     before the 16:00 ET close

That's ~cents for the full weekly 2013->now, 4-name P2 backfill. Real OPRA quotes;
no IV/greeks/OI (compute IV yourself; OI is a separate `statistics` pull).

Uses the `databento` client (its own protocol), not the shared HttpClient.
`pip install "nussif-data[databento]"`.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from .._config import get_key
from ..cache import cached
from ..core import Connector, Dataset
from ..core.errors import UpstreamError
from ..core.schema import OPTION_CHAIN

_UTC = ZoneInfo("UTC")


def _close_utc(date_str: str, tz: str, hhmm: str) -> datetime:
    """The market close for `date_str` as an aware UTC datetime (handles DST)."""
    h, m = (int(x) for x in hhmm.split(":"))
    local = datetime.combine(datetime.fromisoformat(date_str).date(), time(h, m), ZoneInfo(tz))
    return local.astimezone(_UTC)


def _guess_spot(defn: pd.DataFrame) -> float:
    """Rough spot for the moneyness filter: median strike of the nearest expiry
    (ATM strikes are the densest, so this lands close enough for a wide band)."""
    nx = defn.loc[defn["expiration"] == defn["expiration"].min(), "strike"]
    return float(nx.median() if len(nx) else defn["strike"].median())


class DatabentoConnector(Connector):
    name = "databento"
    primary_method = "option_chain"
    DATASET = "OPRA.PILLAR"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._cli = None

    def _client(self):
        if self._cli is None:
            try:
                import databento as db
            except ImportError as e:  # pragma: no cover
                raise RuntimeError(
                    "databento not installed -- pip install 'nussif-data[databento]'"
                ) from e
            vendor = self.cfg.get("auth", {}).get("vendor", "databento")
            self._cli = db.Historical(get_key(vendor))
        return self._cli

    def datasets(self) -> list[Dataset]:
        return [
            Dataset(
                "option_chain",
                OPTION_CHAIN,
                needs_key=True,
                description="historical OPRA EOD option chain (definition + cbbo-1m), from 2013-04",
            )
        ]

    def _fetch_symbol(self, dataset, symbol, raw=False):
        raise NotImplementedError("use nd.databento.option_chain(symbol, date=...)")

    # -- the accessor --
    def option_chain(
        self,
        *symbols,
        date,
        spot=None,
        moneyness=None,
        min_dte=None,
        max_dte=None,
        refresh: bool = False,
    ):
        """EOD option chain for each `symbol` on `date` (YYYY-MM-DD), filtered to
        strikes within +-`moneyness` of spot and DTE in [`min_dte`, `max_dte`].

        `spot` -- pass the underlier close for an accurate moneyness filter; if
        omitted, a rough nearest-expiry-median proxy is used (fine for a wide band).
        `min_dte` -- raise it (e.g. 15) to drop the daily / weekly expiries; on
        SPY/QQQ these otherwise blow past Databento's 2,000-symbol quote cap.
        Returns the canonical option-chain frame; `right` in {'C','P'}. No IV/greeks/OI.
        """
        spec = self.cfg["datasets"]["option_chain"]
        mny = spec.get("default_moneyness", 0.25) if moneyness is None else moneyness
        ndte = spec.get("default_min_dte", 0) if min_dte is None else min_dte
        mdte = spec.get("default_max_dte", 150) if max_dte is None else max_dte
        syms = [
            str(s).upper()
            for s in (
                symbols[0]
                if len(symbols) == 1 and isinstance(symbols[0], (list, tuple))
                else symbols
            )
        ]
        if not syms:
            raise ValueError("nd.databento.option_chain(...) needs at least one symbol")

        frames = [
            cached(
                f"databento/option_chain/{s}/{date}/m{mny}_d{ndte}-{mdte}",
                lambda s=s: self._one(s, str(date), spot, mny, ndte, mdte),
                refresh=refresh,
            )
            for s in syms
        ]
        out = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return OPTION_CHAIN.validate(out, where="databento.option_chain")

    # -- stages --
    def _one(self, sym: str, date: str, spot, mny: float, ndte: int, mdte: int) -> pd.DataFrame:
        spec = self.cfg["datasets"]["option_chain"]
        defn = self._get_definitions(sym, date)
        defn = self._filter(defn, date, spot, mny, ndte, mdte)
        if defn.empty:
            raise UpstreamError(f"databento: no contracts in band for {sym} {date}")
        quotes = self._get_quotes(defn["raw_symbol"].tolist(), date, spec)
        return self._assemble(sym, date, defn, quotes, spec)

    # -- network (isolated so tests can monkeypatch) --
    def _get_definitions(self, sym: str, date: str) -> pd.DataFrame:
        end = (pd.Timestamp(date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        data = self._client().timeseries.get_range(
            dataset=self.DATASET,
            schema="definition",
            symbols=[f"{sym}.OPT"],
            stype_in="parent",
            start=date,
            end=end,
        )
        df = data.to_df()
        return df[["raw_symbol", "instrument_id", "strike_price", "expiration", "instrument_class"]]

    def _get_quotes(self, raw_symbols: list[str], date: str, spec: dict) -> pd.DataFrame:
        c = _close_utc(
            date, spec.get("close_tz", "America/New_York"), spec.get("close_time", "16:00")
        )
        start = (c - timedelta(minutes=3)).isoformat()
        end = (c + timedelta(minutes=1)).isoformat()
        cap = int(spec.get("max_quote_symbols", 2000))  # databento get_range hard limit
        frames = []
        for i in range(0, len(raw_symbols), cap):
            data = self._client().timeseries.get_range(
                dataset=self.DATASET,
                schema="cbbo-1m",
                symbols=raw_symbols[i : i + cap],
                stype_in="raw_symbol",
                start=start,
                end=end,
            )
            frames.append(data.to_df())  # databento >=0.80: float prices + pretty ts
        if len(frames) == 1:
            return frames[0]
        keep_index = all(f.index.name for f in frames)  # e.g. ts_event index
        return pd.concat(frames) if keep_index else pd.concat(frames, ignore_index=True)

    # -- pure transforms --
    @staticmethod
    def _filter(
        defn: pd.DataFrame, date: str, spot, mny: float, ndte: int, mdte: int
    ) -> pd.DataFrame:
        d = defn.copy()
        d["expiration"] = pd.to_datetime(d["expiration"]).dt.tz_localize(None)
        d["strike"] = pd.to_numeric(d["strike_price"], errors="coerce")
        d["dte"] = (d["expiration"] - pd.Timestamp(date)).dt.days
        s = float(spot) if spot is not None else _guess_spot(d)
        m = (d["strike"] >= s * (1 - mny)) & (d["strike"] <= s * (1 + mny))
        return d[m & d["dte"].between(ndte, mdte)].reset_index(drop=True)

    @staticmethod
    def _assemble(sym, date, defn, quotes, spec) -> pd.DataFrame:
        close = _close_utc(
            date, spec.get("close_tz", "America/New_York"), spec.get("close_time", "16:00")
        )
        q = quotes.copy()
        ts = pd.to_datetime(q.get("ts_event", q.index), utc=True)
        q = q.assign(_ts=ts)
        q = q[q["_ts"] <= pd.Timestamp(close)]
        last = q.sort_values("_ts").groupby("instrument_id").tail(1)

        cols = {
            "bid_px_00": "bid",
            "ask_px_00": "ask",
            "bid_sz_00": "bid_size",
            "ask_sz_00": "ask_size",
        }
        last = last.rename(columns=cols)
        out = defn.merge(
            last[["instrument_id", *[c for c in cols.values() if c in last.columns]]],
            on="instrument_id",
            how="inner",
        )
        out["symbol"] = sym
        out["date"] = pd.Timestamp(date)
        out["right"] = (
            out["instrument_class"].map({"C": "C", "P": "P"}).fillna(out["instrument_class"])
        )
        keep = [
            "symbol",
            "date",
            "expiration",
            "strike",
            "right",
            "bid",
            "ask",
            "bid_size",
            "ask_size",
        ]
        out = out[[c for c in keep if c in out.columns]]
        return out.sort_values(["expiration", "strike", "right"]).reset_index(drop=True)
