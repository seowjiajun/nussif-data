"""Fill the local cache with a span of per-day Databento pulls.

    days = nd.trading_days("2013-04-03", "2026-09-03")
    nd.backfill("option_chain", ["SPY", "QQQ"], days, moneyness=0.25, min_dte=15, max_dte=60)
    nd.backfill("open_interest", "SPY", days)

    nussif-data backfill option_chain SPY QQQ --start 2013-04-03 --end 2026-09-03 \\
        --moneyness 0.25 --min-dte 15 --max-dte 60
    nussif-data backfill option_chain SPY TLT --start 2013-04-03 --weekday WED

Fetch-once: every pull goes through the dataset's own cache, so already-cached
(symbol, date) pairs return instantly and a crash just means re-running the
same command. The option-chain cache is keyed by the band (`moneyness`,
`min_dte`, `max_dte`), so pass the same band the readers use.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

DATASETS = ("option_chain", "open_interest")
_WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI")


def trading_days(
    start, end=None, *, reference: str = "SPY", weekday: str | None = None
) -> pd.DatetimeIndex:
    """The days `reference` has a Massive daily bar in [start, end] -- the
    market's actual trading calendar, holidays and half-days included.
    `weekday` ("WED", ...) keeps only that day of the week."""
    end = end or pd.Timestamp.today().strftime("%Y-%m-%d")
    from . import massive

    bars = massive.bars(reference, start=start, end=end, field="close")
    days = pd.DatetimeIndex(pd.to_datetime(bars["date"])).normalize().sort_values()
    if weekday is not None:
        wd = weekday.upper()[:3]
        if wd not in _WEEKDAYS:
            raise ValueError(f"weekday must be one of {_WEEKDAYS}, got {weekday!r}")
        days = days[days.dayofweek == _WEEKDAYS.index(wd)]
    return days


def _spots(symbols: list[str], days: pd.DatetimeIndex) -> pd.DataFrame:
    """Underlier closes on `days` (forward-filled over any gaps) -- the option
    chain's moneyness band is centred on them."""
    from . import massive

    px = massive.bars(*symbols, start=days.min().strftime("%Y-%m-%d"),
                      end=days.max().strftime("%Y-%m-%d"), field="close")  # fmt: skip
    px["date"] = pd.to_datetime(px["date"]).dt.normalize()
    return px.set_index("date").sort_index().reindex(days).ffill()


def backfill(
    dataset: str,
    symbols: str | Iterable[str],
    days: Iterable,
    *,
    workers: int = 6,
    progress: bool = True,
    **band,
) -> pd.DataFrame:
    """Pull `dataset` ("option_chain" | "open_interest", both Databento) for every
    (symbol, day) into the cache. `band` (option_chain only): `moneyness`,
    `min_dte`, `max_dte`. Returns one row per pull -- symbol, date, status ("ok"
    or the error), rows -- so the caller sees what failed without re-running.
    Failures don't stop the run."""
    if dataset not in DATASETS:
        raise ValueError(f"dataset must be one of {DATASETS}, got {dataset!r}")
    if band and dataset != "option_chain":
        raise ValueError(f"{dataset} takes no band arguments, got {sorted(band)}")
    from . import databento

    syms = [symbols.upper()] if isinstance(symbols, str) else [s.upper() for s in symbols]
    days = pd.DatetimeIndex(pd.to_datetime(list(days))).normalize().unique().sort_values()
    spots = _spots(syms, days) if dataset == "option_chain" else None

    def pull(sym: str, day: pd.Timestamp) -> tuple[str, int]:
        date = day.strftime("%Y-%m-%d")
        try:
            if dataset == "option_chain":
                spot = spots.at[day, sym] if sym in spots.columns else None
                spot = float(spot) if spot is not None and pd.notna(spot) else None
                out = databento.option_chain(sym, date=date, spot=spot, **band)
            else:
                out = databento.open_interest(sym, date=date)
            return "ok", len(out)
        except Exception as e:  # recorded per pull, not raised: one bad day mustn't stop the run
            return f"{type(e).__name__}: {str(e)[:120]}", 0

    tasks = [(s, d) for d in days for s in syms]
    rows, t0 = [], time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(pull, s, d): (s, d) for s, d in tasks}
        for i, fut in enumerate(as_completed(futures), 1):
            s, d = futures[fut]
            status, n = fut.result()
            rows.append({"symbol": s, "date": d, "status": status, "rows": n})
            if progress and (i % 100 == 0 or i == len(tasks)):
                rate = i / max(time.time() - t0, 1e-9)
                errors = sum(r["status"] != "ok" for r in rows)
                print(f"[{time.time() - t0:6.0f}s] {i}/{len(tasks)}  {rate:.1f}/s  "
                      f"eta {(len(tasks) - i) / rate / 60:4.0f}m  errors={errors}",
                      file=sys.stderr, flush=True)  # fmt: skip
    return pd.DataFrame(rows).sort_values(["date", "symbol"]).reset_index(drop=True)
