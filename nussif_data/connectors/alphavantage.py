"""Alpha Vantage connector -- HISTORICAL_OPTIONS: one request returns the whole
EOD option chain for a (symbol, date).

NOTE: HISTORICAL_OPTIONS is a PREMIUM Alpha Vantage endpoint (free-tier keys get
'premium endpoint' -> NotEntitled). Premium $50/mo = 75 req/min: a full
historical backfill runs in minutes, then downgrade. Every (symbol, date) is
cached forever.

An option chain is a 2-D object (many contracts per request), not a 1-D time
series, so this connector doesn't use the base `fetch()` template -- it exposes
`option_chain(*symbols, date=...)` directly, backed by the shared HttpClient
(auth, rate limit, retry) and the parquet cache.
"""

from __future__ import annotations

import pandas as pd

from .._config import get_key
from ..cache import cached
from ..core import Connector, Dataset, HttpClient, QueryKeyAuth
from ..core.errors import AuthError, NotEntitled, RateLimited, UpstreamError
from ..core.schema import Schema

OPTION_CHAIN = Schema(
    {
        "date": "datetime",
        "expiration": "datetime",
        "strike": "float",
        "type": "string",
        "bid": "float",
        "ask": "float",
        "*": "float",  # last, mark, sizes, volume, OI, iv, greeks
    }
)

_NUM = [
    "strike",
    "last",
    "mark",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "volume",
    "open_interest",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
]


class AlphaVantageConnector(Connector):
    name = "alphavantage"
    primary_method = "option_chain"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        auth_cfg = cfg.get("auth", {})
        self.http = HttpClient(
            base_url=cfg["base_url"],
            rate_limit_rpm=cfg.get("rate_limit_rpm", 5),
            auth=QueryKeyAuth(
                auth_cfg.get("param", "apikey"),
                lambda: get_key(auth_cfg.get("vendor", "alphavantage")),
            ),
            name="alphavantage",
        )

    def datasets(self) -> list[Dataset]:
        return [
            Dataset(
                "option_chain",
                OPTION_CHAIN,
                needs_key=True,
                description="full EOD option chain for a (symbol, date) -- HISTORICAL_OPTIONS",
            )
        ]

    # the base template is for 1-D series; not used here
    def _fetch_symbol(self, dataset, symbol, raw=False):
        raise NotImplementedError("use nd.alphavantage.option_chain(symbol, date=...)")

    # -- the real accessor --
    def option_chain(self, *symbols, date=None, refresh: bool = False):
        """One or more underliers, one date. date=None -> latest trading day;
        'YYYY-MM-DD' -> that EOD chain (Alpha Vantage covers 2008-01-01+).
        Returns a long tidy frame; `symbol` column present when >1 underlier."""
        syms = [
            str(s).upper()
            for s in (
                symbols[0]
                if len(symbols) == 1 and isinstance(symbols[0], (list, tuple))
                else symbols
            )
        ]
        if not syms:
            raise ValueError("nd.alphavantage.option_chain(...) needs at least one symbol")
        d = str(date) if date is not None else "latest"
        frames = [
            cached(
                f"alphavantage/option_chain/{s}/{d}",
                lambda s=s: self._fetch_chain(s, date),
                refresh=refresh,
            )
            for s in syms
        ]
        out = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return OPTION_CHAIN.validate(out, where="alphavantage.option_chain")

    # -- internals --
    def _fetch_chain(self, symbol: str, date) -> pd.DataFrame:
        spec = self.cfg["datasets"]["option_chain"]
        params = {**spec.get("params", {}), "symbol": symbol}
        if date is not None:
            params["date"] = str(date)
        j = self.http.get_json(spec["endpoint"], params)
        _raise_for_av(j)
        rows = j.get("data") or []
        if not rows:
            raise UpstreamError(f"alphavantage: empty chain for {symbol} {date or 'latest'}")
        df = pd.DataFrame(rows)
        for c in _NUM:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        for c in ("date", "expiration"):
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")
        keep = [
            "symbol",
            "contractID",
            *[c for c in df.columns if c not in ("symbol", "contractID")],
        ]
        return (
            df[[c for c in keep if c in df.columns]]
            .sort_values(["expiration", "strike", "type"])
            .reset_index(drop=True)
        )


def _raise_for_av(j: dict) -> None:
    """Alpha Vantage returns 200 with an error/notice in the body, not an HTTP code."""
    for k in ("Error Message", "Information", "Note"):
        msg = j.get(k)
        if not msg:
            continue
        low = msg.lower()
        if "premium endpoint" in low or "premium plan" in low:
            raise NotEntitled(
                f"alphavantage: HISTORICAL_OPTIONS needs a premium key "
                f"(https://www.alphavantage.co/premium/). {msg}"
            )
        if "rate limit" in low or "requests per day" in low or "call frequency" in low:
            raise RateLimited(f"alphavantage: {msg}")
        if "apikey" in low or "api key" in low or "invalid api call" in low:
            raise AuthError(f"alphavantage: {msg}")
        raise UpstreamError(f"alphavantage: {msg}")
