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

Open interest -- `open_interest()` -- is that separate `statistics` pull:
one `stat_type=OPEN_INTEREST` snapshot for the WHOLE parent symbol (no
definition/moneyness stage needed, and no per-request symbol cap the way
`cbbo-1m` quotes have), disseminated once near the 9:31 ET open. Per OPRA/OCC
convention (and what every public GEX calculator assumes), that AM print is
the open interest *in effect for that trading day's session*, not the prior
day's closing snapshot repeated -- so pulling `date=d` here lines up with a
same-day `option_chain(date=d)` EOD quote, even though the two are stamped
~6.5 hours apart. Contracts are identified straight from the OSI-format
`symbol` string (root + YYMMDD + C/P + strike*1000, 21 chars fixed-width) --
cheaper and simpler than a second `definition` round-trip for the same thing
`option_chain`'s own definition stage already gives you.

Uses the `databento` client (its own protocol), not the shared HttpClient.
`pip install "nussif-data[databento]"`.
"""

from __future__ import annotations

import os
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from .._config import get_key
from ..cache import cache_dir, cached
from ..core import Connector, Dataset
from ..core.errors import UpstreamError
from ..core.schema import OPEN_INTEREST, OPTION_CHAIN

_STAT_TYPE_OPEN_INTEREST = 9  # databento_dbn.StatType.OPEN_INTEREST


def _save_raw(schema: str, sym: str, date: str, df: pd.DataFrame) -> None:
    """Persist Databento's response for this call exactly as returned by
    `.to_df()` -- every field, every dtype, untouched -- before any of this
    module's filtering/renaming/joining. This is the audit source of truth:
    if `option_chain()`/`open_interest()`'s derived, convenience-shaped
    output is ever questioned, this is what you reconcile against, not a
    re-derivation from the already-narrowed frame. Only called from inside
    `_get_definitions`/`_get_quotes`/`_get_oi`, which only run on a real
    network call (a genuine cache miss or explicit `refresh=True` on the
    outer `option_chain`/`open_interest` cache) -- so this never writes
    without a real, paid Databento response behind it, and a `refresh=True`
    re-pull overwrites with the newer raw response rather than silently
    keeping the stale one."""
    path = os.path.join(cache_dir(), "databento", "raw", schema, sym, f"{date}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)


def _parse_osi(symbol: str) -> dict | None:
    """OSI-format option symbol -> {root, expiration, right, strike}. Fixed
    21-char layout: 6-char root (space-padded), YYMMDD, 'C'|'P', strike*1000
    zero-padded to 8 digits. `None` if `symbol` doesn't match (defensive --
    every OPRA options print should)."""
    if len(symbol) != 21 or symbol[12] not in "CP":
        return None
    root = symbol[:6].strip()
    yy, mm, dd = symbol[6:8], symbol[8:10], symbol[10:12]
    try:
        expiration = pd.Timestamp(f"20{yy}-{mm}-{dd}")
        strike = int(symbol[13:21]) / 1000.0
    except ValueError:
        return None
    return {"root": root, "expiration": expiration, "right": symbol[12], "strike": strike}


_UTC = ZoneInfo("UTC")


def _close_utc(date_str: str, tz: str, hhmm: str) -> datetime:
    """The market close for `date_str` as an aware UTC datetime (handles DST)."""
    h, m = (int(x) for x in hhmm.split(":"))
    local = datetime.combine(datetime.fromisoformat(date_str).date(), time(h, m), ZoneInfo(tz))
    return local.astimezone(_UTC)


def _nyse_early_close(date_str: str) -> bool:
    """NYSE's scheduled 1pm closes: July 3 and December 24 when they fall
    Monday-Thursday (on a Friday the holiday is observed that day and the
    market is shut), and the day after Thanksgiving (4th Thursday of November).
    Matches every early close 2013 onward, the span OPRA.PILLAR covers."""
    d = pd.Timestamp(date_str)
    if (d.month, d.day) in ((7, 3), (12, 24)):
        return d.dayofweek <= 3
    return d.month == 11 and d.dayofweek == 4 and 22 <= d.day - 1 <= 28


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
            ),
            Dataset(
                "open_interest",
                OPEN_INTEREST,
                needs_key=True,
                description="daily OPRA open interest per contract (statistics, AM print), from 2013-04",
            ),
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
        date_str = pd.Timestamp(date).strftime("%Y-%m-%d")  # a bare `str(pd.Timestamp(...))`
        # includes " 00:00:00" -- Databento's `start`/`end` reject that, only clean ISO dates

        frames = [
            cached(
                f"databento/option_chain/{s}/{date_str}/m{mny}_d{ndte}-{mdte}",
                lambda s=s: self._one(s, date_str, spot, mny, ndte, mdte),
                refresh=refresh,
            )
            for s in syms
        ]
        out = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return OPTION_CHAIN.validate(out, where="databento.option_chain")

    def open_interest(self, *symbols, date, refresh: bool = False):
        """Daily open interest per contract for each `symbol` on `date`
        (YYYY-MM-DD), from Databento's `statistics` schema. One row per listed
        contract with nonzero OI that day -- no moneyness/DTE band (the whole
        parent-symbol pull is one lightweight `statistics` request, not the
        quote-cap-constrained `cbbo-1m` stage `option_chain` needs), so filter
        the result yourself, or join it to a same-day `option_chain(date=date)`
        frame on (expiration, strike, right). See the module docstring for the
        AM-print/same-day convention this assumes.
        """
        syms = [
            str(s).upper()
            for s in (
                symbols[0]
                if len(symbols) == 1 and isinstance(symbols[0], (list, tuple))
                else symbols
            )
        ]
        if not syms:
            raise ValueError("nd.databento.open_interest(...) needs at least one symbol")
        date_str = pd.Timestamp(date).strftime("%Y-%m-%d")  # a bare `str(pd.Timestamp(...))`
        # includes " 00:00:00" -- Databento's `start`/`end` reject that, only clean ISO dates

        frames = [
            cached(
                f"databento/open_interest/{s}/{date_str}",
                lambda s=s: self._one_oi(s, date_str),
                refresh=refresh,
            )
            for s in syms
        ]
        out = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return OPEN_INTEREST.validate(out, where="databento.open_interest")

    # -- stages --
    def _one(self, sym: str, date: str, spot, mny: float, ndte: int, mdte: int) -> pd.DataFrame:
        spec = self.cfg["datasets"]["option_chain"]
        defn = self._get_definitions(sym, date)
        defn = self._filter(defn, date, spot, mny, ndte, mdte)
        if defn.empty:
            raise UpstreamError(f"databento: no contracts in band for {sym} {date}")
        quotes = self._get_quotes(sym, defn["raw_symbol"].tolist(), date, spec)
        return self._assemble(sym, date, defn, quotes, spec)

    def _one_oi(self, sym: str, date: str) -> pd.DataFrame:
        raw = self._get_oi(sym, date)
        out = self._assemble_oi(sym, date, raw)
        if out.empty:
            raise UpstreamError(f"databento: no open interest for {sym} {date}")
        return out

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
        _save_raw("definition", sym, date, df)
        return df[["raw_symbol", "instrument_id", "strike_price", "expiration", "instrument_class"]]

    def _get_quotes(self, sym: str, raw_symbols: list[str], date: str, spec: dict) -> pd.DataFrame:
        close = (
            spec.get("early_close_time", "13:00")
            if _nyse_early_close(date)
            else spec.get("close_time", "16:00")
        )  # a 16:00 snapshot on a half-day finds no quotes -- the market shut at 13:00
        c = _close_utc(date, spec.get("close_tz", "America/New_York"), close)
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
        quotes = frames[0]
        if len(frames) > 1:
            keep_index = all(f.index.name for f in frames)  # e.g. ts_event index
            quotes = pd.concat(frames) if keep_index else pd.concat(frames, ignore_index=True)
        _save_raw("cbbo-1m", sym, date, quotes)
        return quotes

    def _get_oi(self, sym: str, date: str) -> pd.DataFrame:
        end = (pd.Timestamp(date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        data = self._client().timeseries.get_range(
            dataset=self.DATASET,
            schema="statistics",
            symbols=[f"{sym}.OPT"],
            stype_in="parent",
            start=date,
            end=end,
        )
        df = data.to_df()
        _save_raw(
            "statistics", sym, date, df
        )  # every stat_type, not just OPEN_INTEREST -- see filter below
        # `statistics` carries every stat type (settlement price, highest bid,
        # volatility, ...), not just OI -- on a typical day only ~20% of rows
        # are `stat_type == OPEN_INTEREST`; the rest's `quantity` field is
        # unused/meaningless for that row (seen in practice as a raw int32
        # sentinel, 2147483647) and must not be read as open interest. The
        # raw save above keeps every stat_type regardless -- this filter only
        # narrows what `open_interest()`'s derived output returns.
        return df[df["stat_type"] == _STAT_TYPE_OPEN_INTEREST]

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

    @staticmethod
    def _assemble_oi(sym: str, date: str, raw: pd.DataFrame) -> pd.DataFrame:
        if raw.empty:
            return raw
        # the raw feed repeats the same (instrument_id, ts_event) print several
        # times (a multicast/publisher artifact, not distinct updates) -- keep
        # one row per instrument, the latest by ts_event, same convention
        # `_assemble` already uses for quotes.
        r = raw.assign(_ts=pd.to_datetime(raw["ts_event"], utc=True))
        r = r.sort_values("_ts").groupby("instrument_id").tail(1)

        parsed = r["symbol"].map(_parse_osi)
        rows = []
        for p, qty in zip(parsed, r["quantity"], strict=True):
            if p is None or p["root"] != sym:
                continue
            rows.append(
                {
                    "symbol": sym,
                    "date": pd.Timestamp(date),
                    "expiration": p["expiration"],
                    "strike": p["strike"],
                    "right": p["right"],
                    "open_interest": float(qty),
                }
            )
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        return out.sort_values(["expiration", "strike", "right"]).reset_index(drop=True)
