"""nussif-data — one-line access to multiple market-data sources.

Vendor-namespaced: nd.<vendor>.<dataset>(...), or nd.<vendor>(...) for the
vendor's primary dataset.

    import nussif_data as nd

    nd.cboe.vol_index("VIX", "VIX3M")     # or  nd.cboe("VIX", "VIX3M")
    nd.fred.series("BAA10Y", "NFCI")      # or  nd.fred("BAA10Y", "NFCI")
    nd.massive.bars("SPY", "QQQ")         # or  nd.massive("SPY", "QQQ")
    nd.massive.bars("SPY", "QQQ", field="close")   # WIDE by ticker
    nd.alphavantage.option_chain("SPY", date="2024-06-03")   # full EOD chain (premium)
    nd.databento.option_chain("SPY", date="2024-06-03", spot=530)   # OPRA EOD chain, 2013+

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
from .connectors import (
    AlphaVantageConnector,
    CboeConnector,
    DatabentoConnector,
    FredConnector,
    MassiveConnector,
)
from .core import ConnectorRegistry
from .core.errors import (
    AuthError,
    DatasetNotFound,
    NotEntitled,
    NussifDataError,
    RateLimited,
    SchemaError,
    UpstreamError,
)

__version__ = "0.4.0"
__all__ = [
    "REGISTRY",
    "AuthError",
    "DatasetNotFound",
    "NotEntitled",
    "NussifDataError",
    "RateLimited",
    "SchemaError",
    "UpstreamError",
    "__version__",
    "alphavantage",
    "cache_dir",
    "catalog",
    "cboe",
    "clear_cache",
    "connectors",
    "databento",
    "fred",
    "get_key",
    "massive",
    "set_key",
]

_cfg = _catalog.connectors()

# vendor namespaces — the public API surface
cboe = CboeConnector(_cfg["cboe"])
fred = FredConnector(_cfg["fred"])
massive = MassiveConnector(_cfg["massive"])
alphavantage = AlphaVantageConnector(_cfg["alphavantage"])
databento = DatabentoConnector(_cfg["databento"])

REGISTRY = ConnectorRegistry()
for _c in (cboe, fred, massive, alphavantage, databento):
    REGISTRY.register(_c)


def catalog() -> dict:
    """dataset -> {connector, needs_key, symbols, description}."""
    return REGISTRY.catalog()


def connectors() -> dict:
    """connector -> {datasets}."""
    return REGISTRY.list()
