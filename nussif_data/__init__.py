"""nussif-data — one-line access to CBOE, FRED and Massive market data.

    import nussif_data as nd

    nd.cboe("VIX", "VIX3M", "VXTLT")        # CBOE vol indices, wide by date
    nd.fred("BAA10Y", "NFCI", "UNRATE")     # any FRED series (aliases or raw ids)
    nd.bars("SPY", "QQQ", start="2015")     # adjusted daily OHLCV, long/tidy

    nd.set_key("massive", "…")              # or set $MASSIVE_API_KEY / ~/.config/nussif-data/keys.env
    nd.catalog()                            # what's fetchable
    nd.clear_cache("cboe")                  # drop cached parquet

Every getter takes start=, end=, refresh=. Results are cached under
$NUSSIF_DATA_CACHE (default ~/.cache/nussif-data/).
"""
from __future__ import annotations

from . import cboe as _cboe
from . import fred as _fred
from . import massive as _massive
from ._catalog import load as _catalog_load
from ._config import get_key, set_key
from .cache import cache_dir, clear_cache

__version__ = "0.1.0"
__all__ = ["cboe", "fred", "bars", "catalog", "set_key", "get_key",
           "cache_dir", "clear_cache", "__version__"]


def cboe(*symbols, start=None, end=None, refresh=False):
    """CBOE vol-index EOD levels -> wide frame (date + one col per symbol)."""
    return _cboe.history(*symbols, start=start, end=end, refresh=refresh)


def fred(*ids, start=None, end=None, refresh=False):
    """FRED series -> wide frame. Args are catalog aliases or raw FRED ids."""
    return _fred.series(*ids, start=start, end=end, refresh=refresh)


def bars(*tickers, start=None, end=None, refresh=False):
    """Split/div-adjusted daily OHLCV (Massive) -> long tidy frame."""
    return _massive.bars(*tickers, start=start, end=end, refresh=refresh)


def catalog() -> dict:
    """Default bundles / what's fetchable, from catalog.yaml."""
    c = _catalog_load()
    return {
        "cboe": list(c["cboe"]["daily_prices"]["symbols"]),
        "fred": dict(c["fred"]["series"]),
        "bars": list(c["massive"]["daily_bars"]["default_tickers"]),
    }
