#!/usr/bin/env python3
"""Probe the Massive market-data API to discover entitlements and coverage.

Reads MASSIVE_API_KEY from ../.secrets/massive.env (or the env var), finds the working
base URL + auth scheme, then exercises the endpoints we care about for the
variance-risk-premium and skew-demand projects. Stdlib only.

Usage:
    python3 scripts/discover_massive.py
    python3 scripts/discover_massive.py --verbose      # print JSON bodies
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

socket.setdefaulttimeout(8)  # bound EVERY socket op, incl. probes to dead hosts

_HERE = os.path.dirname(os.path.abspath(__file__))
# Look for the key file in a few sensible spots; env var wins, then explicit path,
# then .secrets/ next to the repo root (../../) and next to the project (../).
SECRETS_CANDIDATES = [
    os.environ.get("MASSIVE_ENV_FILE"),
    "/home/jseow/code/.secrets/massive.env",
    os.path.join(_HERE, "..", "..", ".secrets", "massive.env"),
    os.path.join(_HERE, "..", ".secrets", "massive.env"),
]

# Docs live at massive.com/docs/rest/... so api.massive.com is the overwhelming
# favourite. Others are cheap fallbacks and are skipped fast if DNS fails.
CANDIDATE_BASES = [
    "https://api.massive.com",
    "https://rest.massive.com",
    "https://api.massive.io",
]

# (name, method-of-passing-key)
AUTH_SCHEMES = [
    ("query:apiKey", lambda url, key: _add_query(url, {"apiKey": key})),
    ("query:api_key", lambda url, key: _add_query(url, {"api_key": key})),
    ("header:Bearer", lambda url, key: url),  # header added separately
]

TIMEOUT = 12
UNDERLYINGS = ["SPY", "QQQ", "IWM", "TLT", "GLD"]


def host_resolves(url: str) -> bool:
    host = urllib.parse.urlparse(url).hostname
    try:
        socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        return True
    except (TimeoutError, socket.gaierror, OSError):
        return False


def _add_query(url: str, params: dict) -> str:
    parts = urllib.parse.urlparse(url)
    q = dict(urllib.parse.parse_qsl(parts.query))
    q.update(params)
    return urllib.parse.urlunparse(parts._replace(query=urllib.parse.urlencode(q)))


def load_key() -> str:
    if os.environ.get("MASSIVE_API_KEY"):
        return os.environ["MASSIVE_API_KEY"].strip()
    for path in SECRETS_CANDIDATES:
        if not path or not os.path.exists(path):
            continue
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                k, _, v = line.partition("=")
                if k.strip() in ("API_KEY", "MASSIVE_API_KEY"):
                    print(f"key file: {path}")
                    return v.strip().strip('"').strip("'")
    searched = "\n  ".join(p for p in SECRETS_CANDIDATES if p)
    sys.exit(
        "No API key found. Searched:\n  " + searched + "\n\n"
        "Create /home/jseow/code/.secrets/massive.env containing:\n"
        "  MASSIVE_API_KEY=your_key_here\n"
        "or export MASSIVE_API_KEY, or set MASSIVE_ENV_FILE=/path/to/file."
    )


class Resp:
    def __init__(self, status, headers, body, url):
        self.status = status
        self.headers = headers
        self.body = body
        self.url = url

    @property
    def json(self):
        try:
            return json.loads(self.body)
        except Exception:
            return None


def request(url: str, key: str, scheme: str, transform, retries: int = 3) -> Resp:
    full = transform(url, key)
    req = urllib.request.Request(full, method="GET")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "nussif-volpremia-discovery/1.0")
    req.add_header("Connection", "close")  # server drops keep-alive sockets on us
    if scheme == "header:Bearer":
        req.add_header("Authorization", f"Bearer {key}")
    last = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return Resp(r.status, dict(r.headers), r.read().decode("utf-8", "replace"), full)
        except urllib.error.HTTPError as e:
            # 429/5xx are worth a retry; everything else is a real answer
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
                last = e
                continue
            return Resp(e.code, dict(e.headers), e.read().decode("utf-8", "replace"), full)
        except (urllib.error.URLError, http.client.HTTPException, OSError, TimeoutError) as e:
            last = e
            time.sleep(1.0 * (attempt + 1))
    return Resp(None, {}, f"__transport_error__ after {retries} tries: {last}", full)


def redact(url: str, key: str) -> str:
    return url.replace(key, "***KEY***") if key else url


# ---------------------------------------------------------------------------
# step 1: find a base URL + auth scheme that authenticates
# ---------------------------------------------------------------------------
def find_endpoint(key: str):
    probe_path = "/v3/reference/tickers"
    for base in CANDIDATE_BASES:
        if not host_resolves(base):
            print(f"  [dns]  {base:28s} does not resolve, skipping")
            continue
        for scheme, transform in AUTH_SCHEMES:
            url = _add_query(base + probe_path, {"limit": "1"})
            r = request(url, key, scheme, transform)
            tag = f"{base:28s} {scheme:16s}"
            if r.status is None:
                print(f"  [skip] {tag}  {r.body[:80]}")
                continue
            if r.status == 200 and r.json is not None:
                print(f"  [OK]   {tag}  200")
                return base, scheme, transform
            if r.status in (401, 403):
                # reachable + understood the auth attempt, but rejected
                print(f"  [auth] {tag}  {r.status} {r.body[:120]}")
            else:
                print(f"  [{r.status}]  {tag}  {r.body[:120]}")
    return None, None, None


# ---------------------------------------------------------------------------
# step 2: probe the endpoints we need
# ---------------------------------------------------------------------------
def hit(base, key, scheme, transform, path, params=None, verbose=False, label=None):
    url = base + path
    if params:
        url = _add_query(url, params)
    time.sleep(0.3)  # be gentle; the server drops connections when pushed
    r = request(url, key, scheme, transform)
    label = label or path
    rl = {k: v for k, v in r.headers.items() if "rate" in k.lower() or "limit" in k.lower()}
    n = None
    j = r.json
    if isinstance(j, dict):
        if isinstance(j.get("results"), list):
            n = len(j["results"])
        elif isinstance(j.get("results"), dict):
            n = 1
        elif isinstance(j.get("data"), list):
            n = len(j["data"])
    print(f"  {r.status}  {label:52s} results={n}  {('rl=' + json.dumps(rl)) if rl else ''}")
    if verbose and j is not None:
        print(json.dumps(j, indent=2)[:2000])
    if r.status and r.status >= 400:
        print(f"       -> {r.body[:200]}")
    return r


def earliest_date(
    base, key, scheme, transform, ticker, timespan="day", lo=date(2000, 1, 1), hi=None
):
    """Binary-search the earliest date with aggregate data for `ticker`."""
    hi = hi or date.today()

    def has_data(d):
        path = f"/v2/aggs/ticker/{urllib.parse.quote(ticker)}/range/1/{timespan}/{d.isoformat()}/{(d.replace(day=28) if timespan != 'day' else d).isoformat()}"
        # widen window: use d .. d+ ~40d
        end = date.fromordinal(d.toordinal() + 40)
        path = f"/v2/aggs/ticker/{urllib.parse.quote(ticker)}/range/1/{timespan}/{d.isoformat()}/{end.isoformat()}"
        r = request(
            _add_query(base + path, {"limit": "1", "adjusted": "true"}), key, scheme, transform
        )
        j = r.json or {}
        return bool(j.get("results"))

    lo_o, hi_o = lo.toordinal(), hi.toordinal()
    if not has_data(date.fromordinal(hi_o - 45)):
        return None  # no data at all / not entitled
    while hi_o - lo_o > 20:
        mid = (lo_o + hi_o) // 2
        if has_data(date.fromordinal(mid)):
            hi_o = mid
        else:
            lo_o = mid
        time.sleep(0.15)
    return date.fromordinal(hi_o)


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)  # progress visible even when piped
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    key = load_key()
    print(f"key loaded ({len(key)} chars, prefix {key[:4]}…)\n")

    print("== step 1: locate base URL + auth scheme ==")
    base, scheme, transform = find_endpoint(key)
    if not base:
        sys.exit(
            "\nCould not authenticate against any candidate base URL. "
            "Paste the onboarding email (key removed) so we can get the real host."
        )
    print(f"\nUSING  base={base}  auth={scheme}\n")

    def H(path, params=None, label=None):
        return hit(base, key, scheme, transform, path, params, args.verbose, label)

    print("== step 2: reference data ==")
    H("/v3/reference/tickers", {"market": "stocks", "limit": "1"}, "reference/tickers stocks")
    H("/v3/reference/tickers", {"market": "indices", "limit": "1"}, "reference/tickers indices")
    H("/v3/reference/tickers", {"market": "options", "limit": "1"}, "reference/tickers options")
    contracts = H(
        "/v3/reference/options/contracts",
        {"underlying_ticker": "SPY", "limit": "5"},
        "options/contracts SPY (current)",
    )
    H(
        "/v3/reference/options/contracts",
        {"underlying_ticker": "SPY", "expired": "true", "as_of": "2019-06-03", "limit": "5"},
        "options/contracts SPY as_of 2019-06 (expired)",
    )
    H(
        "/v3/reference/options/contracts",
        {"underlying_ticker": "SPY", "expired": "true", "as_of": "2016-06-01", "limit": "5"},
        "options/contracts SPY as_of 2016-06 (expired)",
    )

    sample_opt = None
    j = contracts.json or {}
    if j.get("results"):
        sample_opt = j["results"][0].get("ticker")
    print(f"\n  sample option ticker: {sample_opt}")

    print("\n== step 3: aggregates (bars) ==")
    H("/v2/aggs/ticker/SPY/range/1/day/2024-01-02/2024-02-01", label="SPY daily aggs 2024")
    H("/v2/aggs/ticker/SPY/range/5/minute/2024-06-03/2024-06-03", label="SPY 5-min aggs (intraday)")
    H("/v2/aggs/ticker/I:VIX/range/1/day/2024-01-02/2024-02-01", label="I:VIX daily aggs")
    H("/v2/aggs/ticker/I:VIX1D/range/1/day/2024-06-03/2024-07-03", label="I:VIX1D daily aggs")
    H("/v2/aggs/ticker/I:VIX3M/range/1/day/2024-01-02/2024-02-01", label="I:VIX3M daily aggs")
    if sample_opt:
        H(
            f"/v2/aggs/ticker/{urllib.parse.quote(sample_opt)}/range/1/day/2024-06-03/2024-07-03",
            label=f"option daily aggs ({sample_opt})",
        )

    print("\n== step 4: quotes / trades (tick) ==")
    if sample_opt:
        H(f"/v3/quotes/{urllib.parse.quote(sample_opt)}", {"limit": "3"}, f"quotes {sample_opt}")
        H(f"/v3/trades/{urllib.parse.quote(sample_opt)}", {"limit": "3"}, f"trades {sample_opt}")
    H("/v3/quotes/SPY", {"limit": "3"}, "quotes SPY (stock)")

    print("\n== step 5: snapshots (greeks / IV / open interest) ==")
    H("/v3/snapshot/options/SPY", {"limit": "3"}, "options chain snapshot SPY")
    if sample_opt:
        H(
            f"/v3/snapshot/options/SPY/{urllib.parse.quote(sample_opt)}",
            label=f"single-contract snapshot {sample_opt}",
        )
    H("/v2/snapshot/locale/us/markets/stocks/tickers/SPY", label="stock snapshot SPY")

    print("\n== step 6: historical option snapshot (as-of past date) ==")
    if sample_opt:
        for d in ("2025-06-02", "2024-06-03", "2023-06-01", "2022-06-01", "2021-06-01"):
            H(
                f"/v3/snapshot/options/SPY/{urllib.parse.quote(sample_opt)}",
                {"as_of": d},
                f"snapshot {sample_opt} as_of {d}",
            )

    print("\n== step 7: earliest available date (binary search) ==")
    for tk in ("SPY", "I:VIX"):
        try:
            e = earliest_date(base, key, scheme, transform, tk)
            print(f"  {tk:10s} earliest daily bar ~ {e}")
        except Exception as ex:
            print(f"  {tk:10s} probe failed: {ex}")
    if sample_opt:
        # options: check which years return option aggs at all
        yrs = []
        for y in (2016, 2018, 2020, 2021, 2022, 2023):
            path = (
                f"/v2/aggs/ticker/{urllib.parse.quote(sample_opt)}/range/1/day/{y}-06-01/{y}-07-10"
            )
            r = request(_add_query(base + path, {"limit": "1"}), key, scheme, transform)
            ok = bool((r.json or {}).get("results"))
            yrs.append(f"{y}:{'Y' if ok else '-'}")
        print(f"  option aggs by year (same contract, may not exist pre-listing): {' '.join(yrs)}")

    print("\n== done ==")
    print("Rate-limit headers seen above (rl=…) tell us the request budget.")
    print(
        "403 = not entitled on this plan; 404 = wrong path/ticker; 200 empty = entitled but no data that range."
    )


if __name__ == "__main__":
    main()
