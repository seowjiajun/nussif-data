"""Shared `option_chain` argument parsing / dispatch for both the scriptable
argparse CLI (cli.py) and the interactive shell (repl.py) -- one place for the
flag semantics so the two front-ends can't drift apart.
"""

from __future__ import annotations

import argparse

import nussif_data as nd

# distinct from the `None` that `_band_value("none")` produces below, so
# build_request can tell "flag omitted -> use nd's own catalog default"
# apart from "flag explicitly set to none/off -> drop the filter entirely" --
# see nd.massive.option_chain(...)'s own _UNSET docstring for the same split.
_UNSET_CLI = object()


def _band_value(kind):
    def convert(s: str):
        return None if s.lower() in ("none", "off") else kind(s)

    return convert


def add_option_chain_args(sp: argparse.ArgumentParser) -> None:
    sp.add_argument("symbols", nargs="+", help="underlier ticker(s), e.g. SPY QQQ")
    sp.add_argument("--date", help="one day, e.g. 2024-06-03")
    sp.add_argument("--start", help="range start (use with --end, not --date)")
    sp.add_argument("--end", help="range end (use with --start, not --date)")
    sp.add_argument(
        "--moneyness",
        type=_band_value(float),
        default=_UNSET_CLI,
        help="+-fraction around spot, e.g. 0.25; 'none' drops the filter (default: catalog default)",
    )
    sp.add_argument(
        "--min-dte",
        type=_band_value(int),
        default=_UNSET_CLI,
        dest="min_dte",
        help="skip expiries closer than this many days; 'none' drops the filter",
    )
    sp.add_argument(
        "--max-dte",
        type=_band_value(int),
        default=_UNSET_CLI,
        dest="max_dte",
        help="skip expiries further than this many days; 'none' drops the filter",
    )
    sp.add_argument(
        "--max-workers", type=int, dest="max_workers", help="per-contract quote concurrency"
    )
    sp.add_argument("--refresh", action="store_true", help="refetch from vendor (ignore cache)")
    sp.add_argument(
        "--raw",
        action="store_true",
        help="fetch mode only: {symbol: frame} instead of one combined frame",
    )
    sp.add_argument(
        "--mode",
        choices=["fetch", "estimate", "download"],
        default="fetch",
        help="fetch (default, returns data), estimate (pre-flight time estimate), "
        "download (memory-bounded, writes straight to cache)",
    )
    sp.add_argument(
        "-o", "--out", help="fetch mode only: write to .parquet/.csv/.json/.feather/.xlsx"
    )
    sp.add_argument("--head", type=int, default=12, help="rows to show when no --out (0 = all)")


def build_request(a: argparse.Namespace):
    kw = {"raw": a.raw}
    if a.date:
        kw["date"] = a.date
    if a.start:
        kw["start"] = a.start
    if a.end:
        kw["end"] = a.end
    if a.moneyness is not _UNSET_CLI:
        kw["moneyness"] = a.moneyness
    if a.min_dte is not _UNSET_CLI:
        kw["min_dte"] = a.min_dte
    if a.max_dte is not _UNSET_CLI:
        kw["max_dte"] = a.max_dte
    if a.max_workers:
        kw["max_workers"] = a.max_workers
    return nd.massive.option_chain(*a.symbols, **kw)


def run(a: argparse.Namespace):
    """Build the request and run everything except `download` (returned
    un-run so the caller decides how to run it -- blocking in a one-shot
    script, backgrounded in the interactive shell). Returns (mode, result)."""
    req = build_request(a)
    if a.mode == "estimate":
        return "estimate", req.estimate(refresh=a.refresh)
    if a.mode == "download":
        return "download", req
    out = a.out if not a.raw else None
    return "fetch", req.fetch(refresh=a.refresh, out=out)
