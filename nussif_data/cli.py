"""`nussif-data` command line — download without writing Python.

    nussif-data                                          # no args -> interactive shell
    nussif-data cboe VIX VIX3M --start 2015 -o vix.parquet
    nussif-data fred BAA10Y NFCI --start 2010 --end 2020 -o macro.csv
    nussif-data massive SPY QQQ TLT --start 2020 -o bars.parquet
    nussif-data massive SPY QQQ --field close      # WIDE by ticker
    nussif-data option_chain SPY --date 2024-06-03
    nussif-data option_chain QQQ --start 2020-01-01 --end 2026-09-23 --mode download --max-workers 250
    nussif-data catalog
    nussif-data connectors

One subcommand per vendor; it operates on that vendor's primary dataset.
`option_chain` is its own subcommand (massive-only for now) since it's a
build-then-fetch/estimate/download request, not a single call -- see
_option_chain_cli.py, shared with the interactive shell's own command.
"""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__, catalog, cboe, connectors, fred, massive
from ._option_chain_cli import add_option_chain_args
from ._option_chain_cli import run as oc_run
from .connectors.massive import BAR_FIELDS

_VENDORS = {"cboe": cboe, "fred": fred, "massive": massive}
_HELP = {
    "cboe": "CBOE volatility indices",
    "fred": "FRED macro/funding series (aliases or raw ids)",
    "massive": "Massive split/div-adjusted daily OHLCV",
}


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="nussif-data", description="Download market data from CBOE / FRED / Massive."
    )
    p.add_argument("--version", action="version", version=f"nussif-data {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    for name, help_ in _HELP.items():
        sp = sub.add_parser(name, help=help_)
        sp.add_argument("symbols", nargs="+", help="symbols / ids / tickers (>= 1)")
        sp.add_argument("--start", help="e.g. 2015, 2015-06, 2015-06-01")
        sp.add_argument("--end", help="e.g. 2020-12-31")
        sp.add_argument("--refresh", action="store_true", help="refetch from vendor (ignore cache)")
        sp.add_argument(
            "-o",
            "--out",
            help="write to file: .parquet .csv .json .feather "
            "(with --raw: one file per symbol, name gets a _SYMBOL suffix)",
        )
        sp.add_argument(
            "--head", type=int, default=12, help="rows to print when no --out (0 = all)"
        )
        sp.add_argument(
            "--raw",
            action="store_true",
            help="vendor frames verbatim (original columns, no coercion), keyed by symbol",
        )
        if name == "massive":
            sp.add_argument(
                "--field",
                choices=BAR_FIELDS,
                help="one field, WIDE by ticker (like cboe/fred); default = tidy long",
            )

    oc = sub.add_parser(
        "option_chain", help="Massive EOD option chain: fetch / estimate / download"
    )
    add_option_chain_args(oc)

    sub.add_parser("catalog", help="list fetchable datasets")
    sub.add_parser("connectors", help="list connectors and their datasets")
    return p


def _run_option_chain(a: argparse.Namespace) -> int:
    mode, result = oc_run(a)

    if mode == "estimate":
        print(json.dumps(result, indent=2))
        return 0
    if mode == "download":
        summary = result.download(refresh=a.refresh)
        print(json.dumps(summary, indent=2))
        return 0

    # mode == "fetch"
    if a.raw:  # {symbol: frame}
        for sym, df in result.items():
            print(f"# {sym}  ({len(df)} rows, columns: {list(df.columns)})")
            print((df if a.head == 0 else df.head(a.head)).to_string(index=False))
            print()
        return 0

    df = result
    if a.out:
        print(f"wrote {a.out}  ({len(df)} rows x {df.shape[1]} cols)")
    else:
        shown = df if a.head == 0 else df.head(a.head)
        print(shown.to_string(index=False))
        if a.head and len(df) > a.head:
            print(f"... {len(df)} rows total — --head 0 for all, -o FILE to save")
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        from .repl import run_repl

        return run_repl()

    a = _build_parser().parse_args(argv)

    if a.cmd == "catalog":
        print(json.dumps(catalog(), indent=2, default=str))
        return 0
    if a.cmd == "connectors":
        print(json.dumps(connectors(), indent=2, default=str))
        return 0
    if a.cmd == "option_chain":
        return _run_option_chain(a)

    kw = {"start": a.start, "end": a.end, "refresh": a.refresh, "raw": a.raw}
    if not a.raw:
        kw["out"] = a.out
    if a.cmd == "massive":
        kw["field"] = a.field
    result = _VENDORS[a.cmd](*a.symbols, **kw)  # nd.<vendor>(...) primary-dataset shorthand

    if a.raw:  # dict {symbol: vendor frame}
        import os

        from ._util import write_frame

        for sym, df in result.items():
            if a.out:
                stem, ext = os.path.splitext(a.out)
                path = write_frame(df, f"{stem}_{sym}{ext}")
                print(f"wrote {path}  ({len(df)} rows x {df.shape[1]} cols)")
            else:
                print(f"# {sym}  ({len(df)} rows, columns: {list(df.columns)})")
                print((df if a.head == 0 else df.head(a.head)).to_string(index=False))
                print()
        return 0

    df = result
    span = f"{df['date'].min().date()}..{df['date'].max().date()}" if len(df) else "empty"
    if a.out:
        print(f"wrote {a.out}  ({len(df)} rows x {df.shape[1]} cols, {span})")
    else:
        shown = df if a.head == 0 else df.head(a.head)
        print(shown.to_string(index=False))
        if a.head and len(df) > a.head:
            print(f"... {len(df)} rows total ({span}) — --head 0 for all, -o FILE to save")
    return 0


if __name__ == "__main__":
    sys.exit(main())
