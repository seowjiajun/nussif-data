#!/usr/bin/env python3
"""Reconstruct a historical EOD option-price panel for one underlier, ourselves.

No bulk endpoint exists, so: enumerate contracts -> one /v2/aggs call per contract
(whole date range in a single response) -> stitch into a tidy daily panel.

This is a deliberately small proof slice (default: SPY, last 3 trading days, one
expiry, +-5% strikes). Widen the CONFIG knobs once the shape looks right. The
script is rate-limited and resumable (re-run to fill gaps; done contracts skipped).

Output: nussif-volpremia/data/roll/{UNDERLIER}_chain.parquet
"""

from __future__ import annotations

import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

import pandas as pd

# ------------------------------- CONFIG -------------------------------------
UNDERLIER = "SPY"
N_TRADING_DAYS = 3  # how many recent sessions to reconstruct
MONEYNESS_BAND = 0.05  # keep strikes within +-5% of spot (on the first day)
DTE_MAX = 45  # only expirations within this many days of the first day
DTE_MIN = 0
N_EXPIRATIONS = 1  # cap distinct expirations (nearest first) -- raise to widen
RATE_LIMIT_RPM = 40  # be gentle: shared key, 9 users. Adaptive backoff on 429.
# --------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(_HERE)
OUT_DIR = os.path.join(PROJECT, "data", "roll")
OUT_PARQUET = os.path.join(OUT_DIR, f"{UNDERLIER}_chain.parquet")
SECRETS_CANDIDATES = [
    os.environ.get("MASSIVE_ENV_FILE"),
    "/home/jseow/code/.secrets/massive.env",
    os.path.join(PROJECT, "..", ".secrets", "massive.env"),
]
BASE = "https://api.massive.com"


def load_key() -> str:
    if os.environ.get("MASSIVE_API_KEY"):
        return os.environ["MASSIVE_API_KEY"].strip()
    for p in SECRETS_CANDIDATES:
        if p and os.path.exists(p):
            with open(p) as fh:
                for line in fh:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        k, _, v = line.partition("=")
                        if k.strip() in ("MASSIVE_API_KEY", "API_KEY"):
                            return v.strip().strip('"').strip("'")
    sys.exit("no MASSIVE_API_KEY found")


KEY = load_key()


# -------------------------- rate-limited HTTP ------------------------------
class Http:
    def __init__(self, rpm):
        self.min_interval = 60.0 / rpm
        self.last = 0.0
        self.calls = 0

    def get(self, path, params=None, retries=4):
        params = dict(params or {})
        params["apiKey"] = KEY
        url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
        for attempt in range(retries):
            wait = self.min_interval - (time.monotonic() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.monotonic()
            self.calls += 1
            req = urllib.request.Request(url, method="GET")
            req.add_header("Connection", "close")
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    return json.loads(r.read().decode("utf-8", "replace"))
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    back = 5 * (attempt + 1)
                    print(f"    429 rate-limited; backing off {back}s")
                    time.sleep(back)
                    self.min_interval *= 1.5  # permanently slow down
                    continue
                if e.code in (500, 502, 503, 504) and attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                    continue
                body = e.read().decode("utf-8", "replace")[:200]
                raise RuntimeError(f"HTTP {e.code} on {path}: {body}")
            except (urllib.error.URLError, http.client.HTTPException, OSError):
                if attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError(f"gave up on {path}")


H = Http(RATE_LIMIT_RPM)


# ------------------------------ steps ------------------------------------
def recent_trading_days(n):
    """Ask the API: pull ~15 calendar days of underlier daily bars, take the last n dates."""
    end = date.today()
    start = end - timedelta(days=20)
    j = H.get(
        f"/v2/aggs/ticker/{UNDERLIER}/range/1/day/{start}/{end}",
        {"adjusted": "true", "sort": "asc", "limit": "50000"},
    )
    rows = j.get("results") or []
    if not rows:
        sys.exit("no underlier bars returned; check entitlement/date")
    days = [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date(), r["c"]) for r in rows]
    return days[-n:]


def list_contracts(first_day, spot):
    lo, hi = spot * (1 - MONEYNESS_BAND), spot * (1 + MONEYNESS_BAND)
    exp_lo = first_day + timedelta(days=DTE_MIN)
    exp_hi = first_day + timedelta(days=DTE_MAX)
    out, cursor = [], None
    while True:
        params = {
            "underlying_ticker": UNDERLIER,
            "expiration_date.gte": exp_lo.isoformat(),
            "expiration_date.lte": exp_hi.isoformat(),
            "strike_price.gte": f"{lo:.2f}",
            "strike_price.lte": f"{hi:.2f}",
            "limit": "1000",
            "expired": "false",
        }
        if cursor:
            params = {"cursor": cursor}
        j = H.get("/v3/reference/options/contracts", params)
        out.extend(j.get("results") or [])
        nxt = j.get("next_url")
        if not nxt:
            break
        cursor = urllib.parse.parse_qs(urllib.parse.urlparse(nxt).query).get("cursor", [None])[0]
        if not cursor:
            break
    # nearest N_EXPIRATIONS expirations
    exps = sorted({c["expiration_date"] for c in out})[:N_EXPIRATIONS]
    keep = [c for c in out if c["expiration_date"] in exps]
    return keep, exps


def fetch_bars(contract, d0, d1):
    tk = contract["ticker"]
    j = H.get(
        f"/v2/aggs/ticker/{urllib.parse.quote(tk)}/range/1/day/{d0}/{d1}",
        {"adjusted": "true", "sort": "asc", "limit": "50000"},
    )
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
            print(f"  [{i}/{len(todo)}] calls={H.calls} elapsed={time.time() - t_start:.0f}s")
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
    print(f"\ntotal API calls this run: {H.calls}")
    print(f"parquet: {OUT_PARQUET}")


if __name__ == "__main__":
    main()
