"""Massive connector (Polygon.io-compatible). Split/div-adjusted daily bars,
one ticker per REST call. Needs an API key ($MASSIVE_API_KEY or
~/.config/nussif-data/keys.env). Rate limiting + retry are handled by the
shared HttpClient.

Split across three files: this one is the thin public `MassiveConnector`
(dataset declarations, `bars()`/`bars_intraday()` shaping and caching);
bars.py is the plain per-ticker HTTP fetch for daily/intraday bars;
options.py is the option-chain path (contract listing + the pooled
`requests.Session` concurrent per-contract quote fetch it needs, plus the
`OptionChainRequest.fetch()`/`.estimate()` split) -- kept separate because
it's both the largest piece and the only one that needs its own connection
pool instead of the shared HttpClient.

`option_chain` is a callable object (`OptionChainFetcher`, from options.py),
not a plain method: `nd.massive.option_chain(*symbols, date=...)` builds and
validates a request (no network I/O yet), and `.fetch()`/`.estimate()` on
the result actually run it -- see options.py's own module docstring.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from ... import _util
from ..._config import get_key
from ...cache import cached
from ...core import Connector, Dataset, HttpClient, QueryKeyAuth
from ...core.errors import UpstreamError
from ...core.schema import BARS_LONG, EXCHANGES, OPTION_CHAIN, TRADES
from . import bars
from .options import OptionChainFetcher

BAR_FIELDS = bars.BAR_FIELDS


class MassiveConnector(Connector):
    name = "massive"
    primary_method = "bars"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        auth_cfg = cfg.get("auth", {})
        self._vendor = auth_cfg.get("vendor", "massive")
        self.http = HttpClient(
            base_url=cfg["base_url"],
            rate_limit_rpm=cfg.get("rate_limit_rpm", 40),
            auth=QueryKeyAuth(auth_cfg.get("param", "apiKey"), lambda: get_key(self._vendor)),
            name="massive",
        )
        self.option_chain = OptionChainFetcher(cfg, self.http, self._vendor)

    def datasets(self) -> list[Dataset]:
        return [
            Dataset(
                "daily_bars",
                BARS_LONG,
                needs_key=True,
                description="split/div-adjusted daily OHLCV",
            ),
            Dataset(
                "option_chain",
                OPTION_CHAIN,
                needs_key=True,
                description=(
                    "historical EOD option chain (contract reference + per-contract "
                    "NBBO quote), from 2014-01"
                ),
            ),
            Dataset(
                "exchanges",
                EXCHANGES,
                needs_key=True,
                description=(
                    "exchange id -> name/mic/participant_id mapping -- asset_class "
                    'required (stocks/options/crypto/fx/futures); "options" decodes '
                    "option_chain()'s own ask_exchange/bid_exchange codes"
                ),
            ),
            Dataset(
                "trades",
                TRADES,
                needs_key=True,
                description=(
                    "tick-level trade prints (price/size/exchange/timestamps) for one "
                    "or more OPTION CONTRACT tickers on one day"
                ),
            ),
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
        d = self.cfg["composites"]["intraday_bars"]
        lo = pd.Timestamp(start) if start else pd.Timestamp(d["history_start"])
        hi = pd.Timestamp(end) if end else pd.Timestamp(date.today())
        frames = []
        for tk in _util.flatten_symbols(tickers):
            key = f"{self.name}/intraday_bars/{tk.upper()}_{multiplier}{timespan[:3]}"
            frames.append(
                cached(
                    key,
                    lambda tk=tk: bars.fetch_intraday(
                        self.http, self.cfg, tk, multiplier, timespan
                    ),
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

    # -- exchange id -> name/mic mapping (decodes ask_exchange/bid_exchange) --
    def exchanges(self, asset_class: str, *, refresh: bool = False) -> pd.DataFrame:
        """Exchange id -> name/mic/participant_id mapping, live from Massive's
        own `/v3/reference/exchanges` -- decodes the numeric `ask_exchange` /
        `bid_exchange` codes `option_chain()` returns. One global reference
        table, not per-symbol: fetched once per `asset_class` and cached
        (exchange listings barely change; pass refresh=True to force a
        re-pull).

        `asset_class` is required, not defaulted -- Massive serves at least
        five (stocks, options, crypto, fx, futures), each its own disjoint id
        space, and picking one silently is more likely to surprise you than
        help you. For decoding option_chain()'s own ask_exchange/bid_exchange
        (ids 300-325, live-verified against a real chain), pass "options".
        """
        d = self.cfg["endpoints"]["exchanges"]
        df = cached(
            f"massive/exchanges/{asset_class}",
            lambda: self._fetch_exchanges(d, asset_class),
            refresh=refresh,
        )
        return EXCHANGES.validate(df, where="massive.exchanges")

    def _fetch_exchanges(self, d: dict, asset_class: str) -> pd.DataFrame:
        j = self.http.get_json(d["endpoint"], {**d.get("params", {}), "asset_class": asset_class})
        rows = j.get("results") or []
        if not rows:
            raise UpstreamError(f"massive: no exchanges for asset_class={asset_class!r}")
        return pd.DataFrame(rows)

    # -- tick-level trade prints for one or more OPTION CONTRACT tickers --
    def trades(self, *tickers, date: str, refresh: bool = False) -> pd.DataFrame:
        """Tick-level trade prints for one or more OPTION CONTRACT tickers on
        `date` (YYYY-MM-DD) -- e.g. "O:SPY240621C00340000", NOT an underlier
        symbol (option_chain()'s own `ticker` column gives you real contract
        tickers to pass here).

        One row per print: Massive's own fields verbatim (price, size,
        exchange, participant_timestamp, sip_timestamp, sequence_number,
        correction, conditions) plus a derived tz-aware `timestamp` column
        (America/New_York, from sip_timestamp) for convenience -- nothing
        renamed or dropped otherwise. There's no single canonical shape for
        tick data the way there is for option_chain()'s one-row-per-contract
        snapshot, so this doesn't reshape into one.

        Paginated sequentially over the shared HttpClient (same convention as
        OptionChainFetcher._get_contracts) -- a liquid contract's full-day
        trade tape can be thousands of prints, but that's a handful of
        paginated requests, not per-contract concurrency the way
        option_chain()'s quote stage needs.
        """
        ep = self.cfg["endpoints"]["trades"]
        tks = _util.flatten_symbols(tickers)
        if not tks:
            raise ValueError("nd.massive.trades(...) needs at least one option contract ticker")
        frames = [
            cached(
                f"massive/trades/{tk}/{date}",
                lambda tk=tk: self._fetch_trades(ep, tk, date),
                refresh=refresh,
            )
            for tk in tks
        ]
        out = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        out = TRADES.validate(out, where="massive.trades")
        return out.sort_values(["ticker", "timestamp"]).reset_index(drop=True)

    def _fetch_trades(self, ep: dict, ticker: str, date: str) -> pd.DataFrame:
        path = ep["endpoint"].format(ticker=ticker)
        params = {"timestamp": date, "limit": 50000, "sort": "timestamp", "order": "asc"}
        rows: list[dict] = []
        next_url = None
        while True:
            j = self.http.get_json(next_url or path, {} if next_url else params)
            rows.extend(j.get("results") or [])
            next_url = j.get("next_url")
            if not next_url:
                break
        if not rows:
            raise UpstreamError(f"massive: no trades for {ticker!r} on {date}")
        df = pd.DataFrame(rows)
        df["ticker"] = ticker
        df["timestamp"] = pd.to_datetime(df["sip_timestamp"], unit="ns", utc=True).dt.tz_convert(
            "America/New_York"
        )
        return df

    def _cache_symbol(self, dataset: str, symbol: str) -> str:
        return symbol.upper()

    def _fetch_symbol(self, dataset: str, symbol: str, raw: bool = False) -> pd.DataFrame:
        if dataset == "option_chain":
            raise NotImplementedError("use nd.massive.option_chain(symbol, date=...).fetch()")
        return bars.fetch_daily(self.http, self.cfg, symbol, raw=raw)

    def _combine(self, dataset: str, frames: list[pd.DataFrame]) -> pd.DataFrame:
        return bars.combine_daily(frames, self.cfg["composites"][dataset].get("trim_gap_days", 15))

    def health(self) -> bool:
        try:
            get_key("massive")  # cheap: is a key configured at all?
            return True
        except Exception:
            return False
