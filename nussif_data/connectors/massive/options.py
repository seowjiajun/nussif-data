"""Massive option_chain(): contract reference lookup (paginated, over the
shared HttpClient) + concurrent per-contract EOD quotes (its own pooled
requests.Session -- see _pooled_session's docstring for why that's not
optional plumbing). This is the largest and most self-contained piece of the
massive connector, and the only one that needs its own connection pool
instead of the shared HttpClient -- split out of __init__.py/bars.py for that
reason.
"""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from datetime import time as dt_time
from zoneinfo import ZoneInfo

import pandas as pd

from ... import _util
from ..._config import get_key
from ...cache import cache_dir, cached, is_cached
from ...core.errors import RateLimited, UpstreamError
from ...core.schema import OPTION_CHAIN
from . import bars

log = logging.getLogger("nussif_data.massive")

_UNSET = object()  # option_chain()'s moneyness/min_dte/max_dte: distinguishes "use the
# catalog default" (argument omitted) from an explicit `None` ("drop this filter
# entirely"). Deliberately different from databento.option_chain()'s convention,
# where None means "use the default" -- see option_chain()'s own docstring for why.
# option_chain is a composite (cfg["composites"]["option_chain"]) built from two
# pure endpoints -- cfg["endpoints"]["option_contracts"] and ["quotes"] -- named
# in its own catalog `uses:` list, not re-declared here.


def _close_window_utc(
    day: pd.Timestamp, tz: str, hhmm: str, lookback_min: int, lookahead_min: int = 1
) -> tuple[str, str]:
    """The [close - lookback_min, close + lookahead_min] window for `day`, as
    aware-UTC ISO strings -- proper EDT/EST handling via zoneinfo, not a fixed
    UTC offset. Same convention as connectors/databento.py::_close_utc, widened
    into a window here (Databento's own quote pull uses a fixed 3-minute window
    and no fallback; Massive's needs the fallback ladder below because a narrow
    window alone produces a false "no quote" for a contract that stopped trading
    earlier in the day -- validated in notebooks/data_exploration/massive.ipynb)."""
    h, m = (int(x) for x in hhmm.split(":"))
    local_close = datetime.combine(day.date(), dt_time(h, m), ZoneInfo(tz))
    start = (local_close - pd.Timedelta(minutes=lookback_min)).astimezone(ZoneInfo("UTC"))
    end = (local_close + pd.Timedelta(minutes=lookahead_min)).astimezone(ZoneInfo("UTC"))
    return start.isoformat(), end.isoformat()


def _save_raw(kind: str, sym: str, date_str: str, df: pd.DataFrame) -> None:
    """Persist Massive's response for this call exactly as returned, before any
    of this module's filtering/renaming/joining -- same audit-trail convention
    as connectors/databento.py::_save_raw (added after an explicit instruction
    not to drop vendor fields silently, however unused they look). `kind` is
    "contracts" (Endpoint 5, band membership for a date) or "quotes" (Endpoint
    3, one row per contract actually fetched for that date). No-ops on an empty
    frame -- nothing paid-for to preserve."""
    if df.empty:
        return
    path = os.path.join(cache_dir(), "massive", "raw", kind, sym, f"{date_str}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)


_ADDED_COLUMNS = (
    "symbol (derived from underlying_ticker), right (derived from contract_type), "
    "expiration/strike/bid/ask (renamed from expiration_date/strike_price/bid_price/"
    "ask_price), timestamp (derived from sip_timestamp, tz-aware America/New_York), "
    "ticker (quotes side -- Massive's own quote response doesn't self-identify which "
    "contract it's for), date (which day was queried), lookback_min_used (fetch "
    "provenance: which fallback window found a quote)"
)


class OptionChainRequest:
    """A built, validated option_chain request -- symbols/days/moneyness-DTE
    band/worker count are all already resolved; nothing here does network I/O
    until you call `.fetch()` or `.estimate()`. Building this once and calling
    either (or both) off the same object means the two can never see a
    different day-list/band than each other -- there's exactly one place
    that logic runs, not two copies that could drift.

    Returned by `nd.massive.option_chain(...)` (== `OptionChainFetcher.__call__`).
    """

    def __init__(
        self, fetcher: OptionChainFetcher, syms, date_strs, spot, mny, ndte, mdte, workers, raw
    ):
        self._fetcher = fetcher
        self.syms = syms
        self.date_strs = date_strs
        self.spot = spot
        self.mny = mny
        self.ndte = ndte
        self.mdte = mdte
        self.workers = workers
        self.raw = raw
        m_tag = "all" if mny is None else str(mny)
        lo_tag = "0" if ndte is None else str(ndte)
        hi_tag = "inf" if mdte is None else str(mdte)
        self._band_tag = f"m{m_tag}_dte{lo_tag}-{hi_tag}"

    def _cache_key(self, sym: str, date_str: str) -> str:
        # no raw/ prefix -- raw=True and raw=False produce identical content
        # now (both return nd's canonical shape; raw= only changes .fetch()'s
        # return packaging), so caching them separately would just be the
        # same data written to disk twice under two different paths.
        return f"massive/option_chain/{sym}/{date_str}/{self._band_tag}"

    def _metadata_for(self, sym: str, date_str: str) -> dict[str, str]:
        # nussif_data.__version__ imported lazily -- options.py loads DURING
        # nussif_data/__init__.py's own execution (massive -> connectors ->
        # top-level), so __version__ isn't set yet at module-import time; by
        # the time this runs, the package is fully loaded.
        from ... import __version__ as nd_version

        return {
            "nd_vendor": "massive",
            "nd_dataset": "option_chain",
            "nd_version": nd_version,
            "nd_symbol": sym,
            "nd_date": date_str,
            "nd_added_columns": _ADDED_COLUMNS,
            "nd_fetched_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        }

    def fetch(self, *, refresh: bool = False, out=None) -> pd.DataFrame | dict:
        """One EOD chain per (symbol, day), fetched and cached per (symbol,
        day) -- so re-running an overlapping request only pulls the new days.

        nd's canonical silver shape -- symbol/date/expiration/strike/right/
        bid/ask (matches core/schema.py's OPTION_CHAIN, same shape
        alphavantage's/databento's own option_chain()s return) plus ticker/
        bid_size/ask_size/lookback_min_used/timestamp. See _assemble's own
        docstring for exactly what's renamed/derived/dropped from Massive's
        response -- nothing is actually lost, `_save_raw` already archived
        the untouched vendor response upstream of this (contracts + quotes,
        under `$NUSSIF_DATA_CACHE/massive/raw/`).

        raw=True (set when this request was built) only changes the return
        shape -- {symbol: frame} instead of one combined frame -- the column
        content is identical either way now.

        `out` -- write the result to `.parquet`/`.csv`/`.json`/`.feather`/`.xlsx`
        (format from the extension, same nd._util.write_frame every other
        accessor's `out=` uses) and still return it. Not supported with
        raw=True -- that returns a dict of per-symbol frames, not one table."""
        if self.raw and out:
            raise ValueError("out= is not supported with raw=True (per-symbol payloads differ)")

        fetcher = self._fetcher
        frames_by_symbol: dict[str, list[pd.DataFrame]] = {s: [] for s in self.syms}
        for date_str in self.date_strs:
            for s in self.syms:
                frames_by_symbol[s].append(
                    cached(
                        self._cache_key(s, date_str),
                        lambda s=s, date_str=date_str: fetcher._one_chain(
                            s,
                            date_str,
                            self.spot,
                            self.mny,
                            self.ndte,
                            self.mdte,
                            self.workers,
                        ),
                        refresh=refresh,
                        metadata=self._metadata_for(s, date_str),
                    )
                )

        if self.raw:
            return {
                s: pd.concat(fs, ignore_index=True) if len(fs) > 1 else fs[0]
                for s, fs in frames_by_symbol.items()
            }
        all_frames = [f for fs in frames_by_symbol.values() for f in fs]
        result = pd.concat(all_frames, ignore_index=True) if len(all_frames) > 1 else all_frames[0]
        if out:
            from ... import __version__ as nd_version  # see _metadata_for's own comment

            metadata = {
                "nd_vendor": "massive",
                "nd_dataset": "option_chain",
                "nd_version": nd_version,
                "nd_symbols": ",".join(self.syms),
                "nd_added_columns": _ADDED_COLUMNS,
                "nd_fetched_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
            }
            _util.write_frame(result, out, metadata=metadata)
        return result

    def download(self, *, refresh: bool = False, on_progress=None) -> dict:
        """Like `.fetch()`, but never holds the requested range in memory:
        each (symbol, day) is fetched and written straight to
        $NUSSIF_DATA_CACHE by `cached()`'s own side effect, then discarded --
        peak memory is one day's contracts, not the whole range. `.fetch()`
        instead accumulates every day's frame and only concatenates (and
        writes `out=`) once, after the entire loop finishes -- fine for a
        modest range, but for years of history that's both a multi-GB
        in-memory frame and an all-or-nothing `out=` write that produces
        nothing at all if a later day aborts the run.

        Also more resilient for exactly that reason: a (symbol, day) that
        fails (RateLimited after its own retries, or anything else) is
        caught and skipped, not left to abort the remaining range the way
        `.fetch()` does -- appropriate for a job meant to run unattended for
        hours, where "3,180 succeeded, 5 failed, here's which ones" is a far
        better outcome than silently stopping on day 2,847. Re-running the
        same request afterward only retries the failed days -- everything
        else already has a file at its cache key, so `cached()`'s own
        existence check skips it for free, same overlap-skipping `.fetch()`
        always had.

        Returns nothing in-memory as data -- read it back from the cache (or
        call `.fetch()` on a narrower, now-warmed sub-range) once you have
        it. Returns a summary instead:
        `{"succeeded": [{"symbol", "date"}, ...], "failed": [{"symbol",
        "date", "error"}, ...]}`.

        `on_progress(done, total, symbol, date_str, ok)` -- optional, called
        after every (symbol, day) attempt (`done`/`total` counts, `ok` =
        whether it succeeded). Lets a caller show live progress on a job
        that's expected to run for hours -- see repl.py's background
        `option_chain --mode download` job, which polls this to answer
        `jobs` without blocking the shell on the download itself.
        """
        fetcher = self._fetcher
        succeeded: list[dict] = []
        failed: list[dict] = []
        total = len(self.date_strs) * len(self.syms)
        done = 0
        for date_str in self.date_strs:
            for s in self.syms:
                try:
                    cached(
                        self._cache_key(s, date_str),
                        lambda s=s, date_str=date_str: fetcher._one_chain(
                            s,
                            date_str,
                            self.spot,
                            self.mny,
                            self.ndte,
                            self.mdte,
                            self.workers,
                        ),
                        refresh=refresh,
                        metadata=self._metadata_for(s, date_str),
                    )
                    succeeded.append({"symbol": s, "date": date_str})
                    ok = True
                except Exception as e:
                    log.warning("massive.option_chain.download: %s %s failed: %s", s, date_str, e)
                    failed.append({"symbol": s, "date": date_str, "error": str(e)})
                    ok = False
                done += 1
                if on_progress is not None:
                    on_progress(done, total, s, date_str, ok)
        return {"succeeded": succeeded, "failed": failed}

    def estimate(self, *, refresh: bool = False) -> dict:
        """Cheap pre-flight estimate: for each (symbol, day) not already
        cached (skipped for free -- refresh=True to force re-probing those
        too), fetches ONLY the contracts listing (fast reference data, no
        per-contract quote fetch) to get a real contract count, then projects
        quote-fetch time from a calibrated throughput constant (catalog
        `composites.option_chain.contracts_per_worker_second`).

        This is a real measurement of contract counts, not a guess -- but the
        throughput projection IS a single calibrated constant, so treat
        `est_seconds` as order-of-magnitude: 429s, retries, and illiquid-
        contract fallback windows aren't modeled. Doesn't touch the quote
        stage at all, so it's cheap even for a wide date range."""
        fetcher = self._fetcher
        spec = fetcher.cfg["composites"]["option_chain"]
        rate = spec.get("contracts_per_worker_second", 1.0)
        overhead = spec.get("contracts_fetch_overhead_s", 1.5)

        per_day = []
        days_cached = 0
        est_seconds = 0.0
        for date_str in self.date_strs:
            for s in self.syms:
                if not refresh and is_cached(self._cache_key(s, date_str)):
                    days_cached += 1
                    continue
                spot = float(self.spot) if self.spot is not None else fetcher._spot_on(s, date_str)
                contracts_df = fetcher._get_contracts(
                    s, date_str, spot, self.mny, self.ndte, self.mdte
                )
                n = len(contracts_df)
                day_seconds = overhead + n / (self.workers * rate)
                est_seconds += day_seconds
                per_day.append(
                    {
                        "symbol": s,
                        "date": date_str,
                        "contracts": n,
                        "est_seconds": round(day_seconds, 1),
                    }
                )

        return {
            "days_total": len(self.date_strs) * len(self.syms),
            "days_cached": days_cached,
            "days_to_fetch": len(per_day),
            "est_seconds": round(est_seconds, 1),
            "est_human": _human_duration(est_seconds),
            "per_day": per_day,
            "confidence": (
                "contract counts are real (probed); throughput is one calibrated "
                "constant -- 429s/retries/illiquid-contract fallback windows aren't "
                "modeled. Treat as order-of-magnitude, not an ETA."
            ),
        }


def _human_duration(seconds: float) -> str:
    m, s = divmod(round(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


class OptionChainFetcher:
    """Owned by MassiveConnector as `self.option_chain`; nothing here is
    reused by bars()/bars_intraday()."""

    def __init__(self, cfg: dict, http, vendor: str):
        self.cfg = cfg
        self.http = http
        self._vendor = vendor
        self._session = None  # lazy pooled requests.Session

    def __call__(
        self,
        *symbols,
        date=None,
        start=None,
        end=None,
        spot=None,
        moneyness=_UNSET,
        min_dte=_UNSET,
        max_dte=_UNSET,
        max_workers=None,
        raw: bool = False,
    ) -> OptionChainRequest:
        """Build (and validate) an option_chain request -- no network I/O yet;
        call `.fetch()` on the result to actually run it, or `.estimate()` for
        a cheap pre-flight time estimate first.

        Pass either `date` (one day) or `start`+`end` (every real trading day
        in between, inclusive -- taken from the first symbol's own daily
        bars, not a generic calendar guess, so a listing gap or holiday
        doesn't silently ask Massive for a day that never happened).

        `spot` -- pass the underlier close for an accurate moneyness filter;
        if omitted, a narrow (+-7 day) bars window around `date` is fetched
        just for this. Not supported with `start`/`end` (spot moves every
        day) -- each day's own close is looked up automatically instead.

        `moneyness` / `min_dte` / `max_dte` -- omit any of them to use the
        catalog default band (moneyness=0.25, DTE 15-60, same convention
        databento.option_chain and backfill.py use). Pass `None` EXPLICITLY
        for any of them to drop that filter entirely, not just widen it --
        this is NOT the same as omitting the argument. Deliberate divergence
        from databento.option_chain, where `None` means "use the default":
        Massive's own completeness question ("is this day's band genuinely
        everything, not silently missing contracts") needs a real no-filter
        mode, and was validated interactively in
        notebooks/data_exploration/massive.ipynb before landing here.

        `max_workers` -- per-contract EOD quotes are fetched concurrently on
        `.fetch()`. Massive's `/v3/quotes/{ticker}` endpoint takes exactly
        one ticker per call (unlike Databento's batched `cbbo-1m`), so this
        is the only place in `nd` that needs real request concurrency. That
        runs through a dedicated, pooled `requests.Session` local to this
        module -- the shared `HttpClient` (every other connector, and this
        one's own `bars`/`bars_intraday`/contract-reference pagination) stays
        stdlib-only, by design (see core/http.py's module docstring). An
        UNPOOLED attempt at this same concurrency (module-level
        `requests.get()`, or threading directly over `HttpClient`) produced
        real `ConnectTimeout`s in testing -- failures at the TCP connect()
        step, nothing to do with Massive's own rate limits -- so this isn't
        optional plumbing. Needs the `requests` package:
        `pip install 'nussif-data[massive-options]'`. Default (catalog
        `option_chain.max_workers`, currently 80) is the last concurrency
        level validated clean (0 429s, 0 connection errors) after pooling
        was added; not a confirmed hard ceiling on Massive's side, the last
        real, tested-clean data point.

        `raw` -- only changes `.fetch()`'s return packaging now: `True` ->
        `{symbol: frame}` (one canonical option_chain table per symbol),
        `False` -> one combined frame across every symbol/day. Column
        content is identical either way -- there's no separate "processed"
        shape to opt out of; see `_assemble`'s own docstring for why.
        moneyness/min_dte/max_dte still narrow which contracts get
        requested regardless of `raw`.
        """
        if (date is None) == (start is None and end is None):
            raise ValueError(
                "nd.massive.option_chain(...) needs date=, or both start= and end= "
                "(not both date= and start=/end=)"
            )
        if (start is None) != (end is None):
            raise ValueError("nd.massive.option_chain(...): start= and end= must both be given")
        if start is not None and spot is not None:
            raise ValueError(
                "spot= isn't supported with start=/end= -- spot moves every day, so each "
                "day's own close is looked up automatically; omit spot= for a range"
            )

        spec = self.cfg["composites"]["option_chain"]
        mny = spec.get("default_moneyness", 0.25) if moneyness is _UNSET else moneyness
        ndte = spec.get("default_min_dte", 15) if min_dte is _UNSET else min_dte
        mdte = spec.get("default_max_dte", 60) if max_dte is _UNSET else max_dte
        workers = int(max_workers if max_workers is not None else spec.get("max_workers", 80))

        syms = [
            str(s).upper()
            for s in (
                symbols[0]
                if len(symbols) == 1 and isinstance(symbols[0], (list, tuple))
                else symbols
            )
        ]
        if not syms:
            raise ValueError("nd.massive.option_chain(...) needs at least one symbol")

        if date is not None:
            date_strs = [pd.Timestamp(date).strftime("%Y-%m-%d")]
        else:
            date_strs = self._trading_days(syms[0], start, end)
            if not date_strs:
                raise ValueError(f"no trading days for {syms[0]} between {start} and {end}")

        return OptionChainRequest(self, syms, date_strs, spot, mny, ndte, mdte, workers, raw)

    def _trading_days(self, symbol: str, start, end) -> list[str]:
        """Real trading days in [start, end], from `symbol`'s own daily bars --
        same cache key nd.massive.bars() uses (massive/daily_bars/<TICKER>),
        so this doesn't trigger an extra network call if bars for `symbol`
        are already cached."""
        df = cached(
            f"massive/daily_bars/{symbol.upper()}",
            lambda: bars.fetch_daily(self.http, self.cfg, symbol),
            refresh=False,
        )
        lo, hi = pd.Timestamp(start), pd.Timestamp(end)
        days = df.loc[df["date"].between(lo, hi), "date"]
        return sorted(d.strftime("%Y-%m-%d") for d in days)

    # -- stages --
    def _one_chain(
        self, sym: str, date_str: str, spot, mny, ndte, mdte, max_workers: int
    ) -> pd.DataFrame:
        api_key = get_key(self._vendor)
        s = float(spot) if spot is not None else self._spot_on(sym, date_str)
        contracts_df = self._get_contracts(sym, date_str, s, mny, ndte, mdte)
        if contracts_df.empty:
            raise UpstreamError(f"massive: no contracts in band for {sym} {date_str}")
        quotes_df = self._get_quotes_concurrent(
            contracts_df["ticker"].tolist(), date_str, api_key, max_workers
        )
        _save_raw("quotes", sym, date_str, quotes_df)
        if quotes_df.empty:
            raise UpstreamError(
                f"massive: no EOD quotes found for {sym} {date_str} "
                f"({len(contracts_df)} contracts in band)"
            )
        out = self._assemble(contracts_df, quotes_df)
        if out.empty:
            raise UpstreamError(
                f"massive: no EOD quotes found for {sym} {date_str} "
                f"({len(contracts_df)} contracts in band)"
            )
        return OPTION_CHAIN.validate(out, where="massive.option_chain")

    def _spot_on(self, sym: str, date_str: str) -> float:
        """Narrow (+-7 day) bars window around `date_str`, nearest close --
        avoids pulling/caching the whole bars history just to answer 'what was
        spot on this one date' for the moneyness filter."""
        day = pd.Timestamp(date_str)
        start = (day - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
        end = (day + pd.Timedelta(days=7)).strftime("%Y-%m-%d")
        j = self.http.get_json(
            f"/v2/aggs/ticker/{sym}/range/1/day/{start}/{end}",
            {"adjusted": "true", "sort": "asc", "limit": "50000"},
        )
        bars = j.get("results") or []
        if not bars:
            raise UpstreamError(f"massive: no bars near {date_str} for {sym} (spot lookup)")
        bar = min(bars, key=lambda b: abs(pd.Timestamp(b["t"], unit="ms") - day))
        return float(bar["c"])

    def _get_contracts(self, sym: str, date_str: str, spot: float, mny, ndte, mdte) -> pd.DataFrame:
        """That date's contract band -- Endpoint 5 (contract reference),
        as_of + strike/DTE filters, paginated through the shared HttpClient
        (sequential -- this is a handful of requests, not the per-contract
        quote stage that needs concurrency).

        `expired` is deliberately omitted (falls back to its documented
        default, false) -- expired=true alongside as_of=<past date> returned
        0 contracts on every day in real testing. Leading hypothesis, NOT
        confirmed in Massive's docs (which don't state how expired/as_of
        interact): expired=true may be evaluated relative to as_of rather
        than today, directly contradicting expiration_date.gte/lte asking for
        expirations after that same date. Whatever the exact mechanism,
        expired=false ("not yet expired as of as_of") is the documented-
        default semantics actually wanted here."""
        day = pd.Timestamp(date_str)
        contracts_path = self.cfg["endpoints"]["option_contracts"]["endpoint"]
        params = {"underlying_ticker": sym, "as_of": date_str, "limit": 1000}
        if mny is not None:
            params["strike_price.gte"] = spot * (1 - mny)
            params["strike_price.lte"] = spot * (1 + mny)
        if ndte is not None:
            params["expiration_date.gte"] = (day + pd.Timedelta(days=ndte)).strftime("%Y-%m-%d")
        if mdte is not None:
            params["expiration_date.lte"] = (day + pd.Timedelta(days=mdte)).strftime("%Y-%m-%d")

        rows: list[dict] = []
        next_url = None
        while True:
            j = self.http.get_json(next_url or contracts_path, {} if next_url else params)
            rows.extend(j.get("results") or [])
            next_url = j.get("next_url")
            if not next_url:
                break
        df = pd.DataFrame(rows)
        _save_raw("contracts", sym, date_str, df)
        return df

    def _pooled_session(self):
        """Lazily built, shared across calls -- module-level requests.get()
        (or a fresh Session per call) opens a new TCP+TLS connection every
        time with no reuse, which is exactly what produced real ConnectTimeouts
        at high thread counts in testing. pool_maxsize is set well above any
        realistic max_workers so concurrent threads always find a pooled
        connection to reuse instead of opening a new one."""
        if self._session is None:
            try:
                import requests
            except ImportError as e:  # pragma: no cover
                raise RuntimeError(
                    "requests not installed -- pip install 'nussif-data[massive-options]'"
                ) from e
            self._requests = requests
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=200, pool_maxsize=200)
            session.mount("https://", adapter)
            self._session = session
        return self._session

    def _pooled_get(self, url: str, params: dict, timeout: int = 30, max_retries: int = 5):
        """Retries two real failure modes seen in testing, both backed off the
        same way HttpClient already handles its own 429s: an explicit 429
        (honoring Retry-After), and a connection-level failure
        (ConnectTimeout/ConnectionError) -- the latter showed up at high
        concurrency BEFORE the pooled session above was added, and isn't
        necessarily gone forever just because it hasn't recurred since."""
        session = self._pooled_session()
        last_exc = None
        r = None
        for attempt in range(max_retries):
            try:
                r = session.get(url, params=params, timeout=timeout)
            except (
                self._requests.exceptions.ConnectionError,
                self._requests.exceptions.Timeout,
            ) as e:
                last_exc = e
                log.debug("massive quote GET transport error (attempt %d): %s", attempt + 1, e)
                time.sleep(2.0 * (attempt + 1))
                continue
            if r.status_code != 429:
                return r
            ra = r.headers.get("Retry-After")
            wait = float(ra) if ra and ra.isdigit() else 2.0 * (attempt + 1)
            log.debug("massive quote GET 429 (attempt %d), waiting %.1fs", attempt + 1, wait)
            time.sleep(wait)
        if r is not None:
            return (
                r  # exhausted retries on 429s -- caller sees the real status via raise_for_status()
            )
        raise UpstreamError(
            f"massive: transport error after {max_retries} tries: {last_exc}"
        ) from last_exc

    def _get_quotes_concurrent(
        self, tickers: list[str], date_str: str, api_key: str, max_workers: int
    ) -> pd.DataFrame:
        day = pd.Timestamp(date_str)
        rows: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._quote_one, tk, day, api_key): tk for tk in tickers}
            for future in as_completed(futures):
                row = future.result()
                if row is not None:
                    rows.append(row)
        return pd.DataFrame(rows)

    def _quote_one(self, ticker: str, day: pd.Timestamp, api_key: str) -> dict | None:
        """Narrow-window-then-fallback EOD quote for ONE contract, ONE date."""
        spec = self.cfg["composites"]["option_chain"]
        quote_endpoint = self.cfg["endpoints"]["quotes"]["endpoint"]
        url = self.cfg["base_url"] + quote_endpoint.format(ticker=ticker)
        for lookback in spec.get("fallback_lookbacks_min", (3, 60, 390)):
            start, end = _close_window_utc(
                day,
                spec.get("close_tz", "America/New_York"),
                spec.get("close_time", "16:00"),
                lookback,
            )
            params = {
                "timestamp.gte": start,
                "timestamp.lte": end,
                "limit": 1,
                "sort": "timestamp",
                "order": "desc",
                "apiKey": api_key,
            }
            r = self._pooled_get(url, params)
            if r.status_code == 429:
                raise RateLimited(f"massive: 429 fetching quotes for {ticker} after retries")
            r.raise_for_status()
            results = r.json().get("results") or []
            if results:
                row = dict(results[0])
                row["ticker"] = ticker
                row["date"] = day.strftime("%Y-%m-%d")
                row["lookback_min_used"] = lookback
                return row
        return None

    @staticmethod
    def _assemble(contracts_df: pd.DataFrame, quotes_df: pd.DataFrame) -> pd.DataFrame:
        """Merge contracts + quotes on ticker, then shape into nd's canonical
        silver form -- matches core/schema.py's OPTION_CHAIN (symbol/date/
        expiration/strike/right/bid/ask), the same shape alphavantage's/
        databento's own option_chain()s already return. `nd` is a
        pandas-datareader-like convenience tool that hands back generic,
        immediately-usable data, not a vendor mirror -- preserving Massive's
        response untouched is `_save_raw`'s job, already done upstream of
        this call (see _one_chain) before any renaming/dropping happens
        here, so nothing below is actually lost.

        Renamed/derived: symbol (from underlying_ticker), right ('C'/'P',
        from contract_type), expiration/strike/bid/ask (renamed from
        expiration_date/strike_price/bid_price/ask_price), timestamp
        (tz-aware America/New_York, derived from sip_timestamp -- the
        precise moment of the quote itself, not just the requested day, so
        this stays meaningful if `nd` ever extends past one EOD snapshot per
        day). Kept as-is: ticker, bid_size, ask_size, date, lookback_min_used
        (fetch provenance -- which fallback window found this quote; a high
        value flags a stale close worth filtering out of a close-based
        backtest).

        Dropped from this shape (still sitting untouched in `_save_raw`'s
        archive if ever needed): cfi, sequence_number, primary_exchange,
        shares_per_contract, exercise_style, ask_exchange, bid_exchange, and
        the raw fields superseded by the renames above (underlying_ticker,
        contract_type, strike_price, expiration_date, bid_price, ask_price,
        sip_timestamp)."""
        if quotes_df.empty:
            return pd.DataFrame()
        merged = contracts_df.merge(quotes_df, on="ticker", how="inner")
        if merged.empty:
            return pd.DataFrame()
        out = pd.DataFrame(
            {
                "symbol": merged["underlying_ticker"],
                "date": merged["date"],
                "expiration": pd.to_datetime(merged["expiration_date"]),
                "strike": merged["strike_price"],
                "right": merged["contract_type"].str[0].str.upper(),
                "bid": merged["bid_price"],
                "ask": merged["ask_price"],
                "ticker": merged["ticker"],
                "bid_size": merged["bid_size"],
                "ask_size": merged["ask_size"],
                "lookback_min_used": merged["lookback_min_used"],
                "timestamp": pd.to_datetime(
                    merged["sip_timestamp"], unit="ns", utc=True
                ).dt.tz_convert("America/New_York"),
            }
        )
        return out.sort_values(["expiration", "strike"]).reset_index(drop=True)
