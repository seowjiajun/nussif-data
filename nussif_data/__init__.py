"""nussif-data — one-line access to multiple market-data sources.

Vendor-namespaced: nd.<vendor>.<dataset>(...), or nd.<vendor>(...) for the
vendor's primary dataset.

    import nussif_data as nd

    nd.cboe.vol_index("VIX", "VIX3M")     # or  nd.cboe("VIX", "VIX3M")
    nd.fred.series("BAA10Y", "NFCI")      # or  nd.fred("BAA10Y", "NFCI")
    nd.massive.bars("SPY", "QQQ")         # or  nd.massive("SPY", "QQQ")
    nd.massive.bars("SPY", "QQQ", field="close")   # WIDE by ticker

    nd.set_key("massive", "…")            # or $MASSIVE_API_KEY / ~/.config/nussif-data/keys.env
    nd.catalog()      nd.connectors()     nd.clear_cache("cboe")

Every accessor takes start=, end=, refresh=, out= (write to .parquet/.csv/.json/
.feather). Results cache per symbol under $NUSSIF_DATA_CACHE (default ~/.cache/nussif-data/).

Architecture: a ConnectorRegistry of vendor Connectors, each backed by a shared
HttpClient (rate limit, retry/backoff, auth, logging). catalog.yaml = connection
config; core/ = machinery; connectors/ = one class per vendor. Add a source:
subclass Connector, add a catalog block, register it below.
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
    "cboe", "fred", "massive", "catalog", "connectors", "REGISTRY",
    "set_key", "get_key", "cache_dir", "clear_cache",
    "NussifDataError", "AuthError", "NotEntitled", "RateLimited",
    "UpstreamError", "SchemaError", "DatasetNotFound", "__version__",
]

_cfg = _catalog.connectors()

# vendor namespaces — the public API surface
cboe = CboeConnector(_cfg["cboe"])
fred = FredConnector(_cfg["fred"])
massive = MassiveConnector(_cfg["massive"])

REGISTRY = ConnectorRegistry()
for _c in (cboe, fred, massive):
    REGISTRY.register(_c)


def catalog() -> dict:
    """dataset -> {connector, needs_key, default_symbols, description}."""
    return REGISTRY.catalog()


def connectors() -> dict:
    """connector -> {datasets}."""
    return REGISTRY.list()
