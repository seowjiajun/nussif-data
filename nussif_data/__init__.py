"""nussif-data — one-line access to CBOE, FRED and Massive market data.

    import nussif_data as nd

    nd.cboe("VIX", "VIX3M", "VXTLT")        # CBOE vol indices, wide by date
    nd.fred("BAA10Y", "NFCI", "UNRATE")     # any FRED series (aliases or raw ids)
    nd.bars("SPY", "QQQ", start="2015")     # adjusted daily OHLCV, tidy long

    nd.set_key("massive", "…")              # or $MASSIVE_API_KEY / ~/.config/nussif-data/keys.env
    nd.catalog()                            # datasets -> connector
    nd.connectors()                         # connector health / datasets
    nd.clear_cache("massive")               # drop cached parquet

Architecture: a `ConnectorRegistry` of vendor `Connector`s, each backed by a
shared `HttpClient` (rate limit, retry/backoff, auth, logging). `catalog.yaml`
holds connection config; `core/` holds the machinery; `connectors/` holds one
class per vendor. Add a vendor = a `Connector` subclass + a catalog block +
`REGISTRY.register(...)` below.
"""
from __future__ import annotations

from . import _catalog
from ._config import get_key, set_key
from .cache import cache_dir, clear_cache
from .connectors import CboeConnector, FredConnector, MassiveConnector
from .core import ConnectorRegistry
from .core.errors import (
    AuthError, DatasetNotFound, NotEntitled, NussifDataError,
    RateLimited, SchemaError, UpstreamError,
)

__version__ = "0.2.0"
__all__ = [
    "cboe", "fred", "bars", "catalog", "connectors", "REGISTRY",
    "set_key", "get_key", "cache_dir", "clear_cache",
    "NussifDataError", "AuthError", "NotEntitled", "RateLimited",
    "UpstreamError", "SchemaError", "DatasetNotFound", "__version__",
]

_cfg = _catalog.connectors()
REGISTRY = ConnectorRegistry()
REGISTRY.register(CboeConnector(_cfg["cboe"]))
REGISTRY.register(FredConnector(_cfg["fred"]))
REGISTRY.register(MassiveConnector(_cfg["massive"]))


def cboe(*symbols, start=None, end=None, refresh=False):
    """CBOE vol-index EOD levels -> wide frame (date + one col per symbol)."""
    return REGISTRY.fetch("vol_index", symbols, start=start, end=end, refresh=refresh)


def fred(*ids, start=None, end=None, refresh=False):
    """FRED series -> wide frame. Args are catalog aliases or raw FRED ids."""
    return REGISTRY.fetch("macro_series", ids, start=start, end=end, refresh=refresh)


def bars(*tickers, start=None, end=None, refresh=False):
    """Split/div-adjusted daily OHLCV (Massive) -> long tidy frame."""
    return REGISTRY.fetch("daily_bars", tickers, start=start, end=end, refresh=refresh)


def catalog() -> dict:
    """dataset -> {connector, needs_key, default_symbols, description}."""
    return REGISTRY.catalog()


def connectors() -> dict:
    """connector -> {datasets, healthy}."""
    return REGISTRY.list()
