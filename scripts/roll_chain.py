#!/usr/bin/env python3
"""Reconstruct a historical EOD option-price panel for one underlier, ourselves.

No bulk endpoint exists, so: enumerate contracts -> one /v2/aggs call per contract
(whole date range in a single response) -> stitch into a tidy daily panel.

This is a deliberately small proof slice (default: SPY, last 3 trading days, one
expiry, +-5% strikes). Widen the CONFIG knobs once the shape looks right. The
script is rate-limited and resumable (re-run to fill gaps; done contracts skipped).

Output: <out_dir()>/roll/{UNDERLIER}_chain.parquet -- $NUSSIF_DATA_OUT, else the
cwd (same convention every other nussif_data out= uses; nothing here hardcodes
a folder, since this is a self-serve tool and everyone's output lands wherever
they've pointed $NUSSIF_DATA_OUT).
"""

from __future__ import annotations

import os
import sys
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from nussif_data import get_key
from nussif_data._catalog import connector_cfg
from nussif_data.cache import out_dir
from nussif_data.core import HttpClient, QueryKeyAuth

# ------------------------------- CONFIG -------------------------------------
UNDERLIER = "SPY"
N_TRADING_DAYS = 3  # how many recent sessions to reconstruct
MONEYNESS_BAND = 0.05  # keep strikes within +-5% of spot (on the first day)
DTE_MAX = 45  # only expirations within this many days of the first day
DTE_MIN = 0
N_EXPIRATIONS = 1  # cap distinct expirations (nearest first) -- raise to widen
# --------------------------------------------------------------------------

OUT_DIR = os.path.join(out_dir(), "roll")
OUT_PARQUET = os.path.join(OUT_DIR, f"{UNDERLIER}_chain.parquet")

# base_url / rate_limit_rpm / auth / endpoint paths all come from catalog.yaml
# -- the same source nussif_data.massive uses -- instead of being duplicated
# here. massive's catalog is split into endpoints: (pure vendor calls) and
# composites: (datasets built from them, e.g. daily_bars = the custom_bars
# endpoint pinned to multiplier=1/timespan=day) -- see catalog.yaml's own
# header comment for the full convention.
_CFG = connector_cfg("massive")
_CONTRACTS_PATH = _CFG["endpoints"]["option_contracts"]["endpoint"]

try:
    get_key("massive")  # fail fast, same check the old load_key() did
except RuntimeError as e:
    sys.exit(str(e))

# rate-limit/retry/backoff/auth all come from the shared HttpClient -- no
# hand-rolled urllib loop here.
H = HttpClient(
    base_url=_CFG["base_url"],
    rate_limit_rpm=_CFG.get("rate_limit_rpm", 40),
    auth=QueryKeyAuth(_CFG.get("auth", {}).get("param", "apiKey"), lambda: get_key("massive")),
    name="roll_chain",
)
CALLS = 0


def get_json(path_or_url: str, params: dict | None = None) -> dict:
    global CALLS
    CALLS += 1
    return H.get_json(path_or_url, params)


# ------------------------------ steps ------------------------------------
_CUSTOM_BARS = _CFG["endpoints"]["custom_bars"]
_DAILY = _CFG["composites"]["daily_bars"]


def recent_trading_days(n):
    """Ask the API: pull ~15 calendar days of underlier daily bars, take the last n dates."""
    end = date.today()
    start = end - timedelta(days=20)
    path = _CUSTOM_BARS["endpoint"].format(
        ticker=UNDERLIER, multiplier=_DAILY["multiplier"], timespan=_DAILY["timespan"],
        start=start, end=end,
    )
    j = get_json(path, _CUSTOM_BARS.get("params", {}))
    rows = j.get("results") or []
    if not rows:
        sys.exit("no underlier bars returned; check entitlement/date")
    days = [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date(), r["c"]) for r in rows]
    return days[-n:]


def list_contracts(first_day, spot):
    lo, hi = spot * (1 - MONEYNESS_BAND), spot * (1 + MONEYNESS_BAND)
    exp_lo = first_day + timedelta(days=DTE_MIN)
    exp_hi = first_day + timedelta(days=DTE_MAX)
    params = {
        "underlying_ticker": UNDERLIER,
        "expiration_date.gte": exp_lo.isoformat(),
        "expiration_date.lte": exp_hi.isoformat(),
        "strike_price.gte": f"{lo:.2f}",
        "strike_price.lte": f"{hi:.2f}",
        "limit": "1000",
        "expired": "false",
    }
    out, next_url = [], None
    while True:
        # next_url from Massive is an absolute URL -- HttpClient passes it straight
        # through instead of re-hitting base_url (same pagination as
        # nussif_data/connectors/massive/options.py::OptionChainFetcher._get_contracts).
        j = get_json(next_url or _CONTRACTS_PATH, {} if next_url else params)
        out.extend(j.get("results") or [])
        next_url = j.get("next_url")
        if not next_url:
            break
    # nearest N_EXPIRATIONS expirations
    exps = sorted({c["expiration_date"] for c in out})[:N_EXPIRATIONS]
    keep = [c for c in out if c["expiration_date"] in exps]
    return keep, exps


def fetch_bars(contract, d0, d1):
    tk = contract["ticker"]
    path = _CUSTOM_BARS["endpoint"].format(
        ticker=urllib.parse.quote(tk), multiplier=_DAILY["multiplier"], timespan=_DAILY["timespan"],
        start=d0, end=d1,
    )
    j = get_json(path, _CUSTOM_BARS.get("params", {}))
    rows = []
    for r in j.get("results") or []:
        rows.append(
            {
                "date": datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date(),
                "underlier": UNDERLIER,
                "ticker": tk,
                "expiration": contract["expiration_date"],
                "strike": contract["strike_price"],
                "right": contract["contract_type"][0].upper(),  # C / P
                "o": r.get("o"),
                "h": r.get("h"),
                "l": r.get("l"),
                "c": r.get("c"),
                "v": r.get("v"),
                "vw": r.get("vw"),
                "n": r.get("n"),
            }
        )
    return rows


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    days = recent_trading_days(N_TRADING_DAYS)
    d0, d1 = days[0][0], days[-1][0]
    spot0 = days[0][1]
    print(f"underlier={UNDERLIER}  trading days: {[str(d) for d, _ in days]}")
    print(
        f"spot on {d0} = {spot0:.2f}  |  strike band +-{MONEYNESS_BAND:.0%} "
        f"= [{spot0 * (1 - MONEYNESS_BAND):.0f}, {spot0 * (1 + MONEYNESS_BAND):.0f}]"
    )

    contracts, exps = list_contracts(d0, spot0)
    print(f"expirations kept: {exps}")
    print(f"contracts in slice: {len(contracts)}  (~{len(contracts)} agg calls)")

    # resume: skip tickers already in the parquet
    done = set()
    if os.path.exists(OUT_PARQUET):
        prev = pd.read_parquet(OUT_PARQUET)
        done = set(prev["ticker"].unique())
        print(f"resuming: {len(done)} contracts already fetched")
    else:
        prev = pd.DataFrame()

    todo = [c for c in contracts if c["ticker"] not in done]
    all_rows = []
    t_start = time.time()
    for i, c in enumerate(todo, 1):
        all_rows.extend(fetch_bars(c, d0, d1))
        if i % 25 == 0 or i == len(todo):
            print(f"  [{i}/{len(todo)}] calls={CALLS} elapsed={time.time() - t_start:.0f}s")
            # checkpoint
            df = pd.concat([prev, pd.DataFrame(all_rows)], ignore_index=True)
            df.to_parquet(OUT_PARQUET, index=False)

    df = pd.concat([prev, pd.DataFrame(all_rows)], ignore_index=True) if all_rows else prev
    if df.empty:
        print("no rows.")
        return
    df = df.drop_duplicates(["date", "ticker"]).sort_values(
        ["date", "expiration", "strike", "right"]
    )
    df.to_parquet(OUT_PARQUET, index=False)

    # -------- quality report --------
    print("\n===== SLICE SUMMARY =====")
    print(
        f"rows: {len(df)}   contracts: {df['ticker'].nunique()}   dates: {sorted(map(str, df['date'].unique()))}"
    )
    grid = df.groupby("date").agg(
        contracts=("ticker", "nunique"),
        with_print=("c", lambda s: s.notna().sum()),
        median_vol=("v", "median"),
        thin=("n", lambda s: (s <= 2).mean()),
    )
    print(grid.to_string())
    print(
        "\nexpected contract-days:",
        df["ticker"].nunique() * df["date"].nunique(),
        " actual bars:",
        len(df),
        f" (coverage {len(df) / (df['ticker'].nunique() * df['date'].nunique()):.1%})",
    )
    print("\nsample (ATM-ish rows on last day):")
    last = df[df["date"] == df["date"].max()].copy()
    last["dist"] = (last["strike"] - spot0).abs()
    print(
        last.nsmallest(8, "dist")[
            ["date", "expiration", "strike", "right", "c", "v", "n"]
        ].to_string(index=False)
    )
    print(f"\ntotal API calls this run: {CALLS}")
    print(f"parquet: {OUT_PARQUET}")


if __name__ == "__main__":
    main()
