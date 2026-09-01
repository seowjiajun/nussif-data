# nussif-data

One-line access to **CBOE**, **FRED** and **Massive** market data. NUSSIF's `yfinance`.

```python
import nussif_data as nd

nd.cboe("VIX", "VIX3M", "VXTLT")          # CBOE vol indices, wide by date
nd.fred("BAA10Y", "NFCI", "UNRATE")       # any FRED series (aliases or raw ids)
nd.bars("SPY", "QQQ", start="2015")       # split/div-adjusted daily OHLCV (tidy long)
```

- Every getter takes `start=`, `end=`, `refresh=`.
- Results are cached per symbol to `~/.cache/nussif-data/` (override `$NUSSIF_DATA_CACHE`).
- `nd.catalog()` — dataset → connector; `nd.connectors()` — connectors & their datasets.

## Install
```bash
pip install -e .
# or from another project:  pip install "nussif-data @ git+https://github.com/<you>/nussif-data.git"
```

## Keys
Only **Massive** needs one:
```bash
export MASSIVE_API_KEY=...
# or:  python -c "import nussif_data as nd; nd.set_key('massive','...')"   # -> ~/.config/nussif-data/keys.env (600)
```

## Architecture

```
nussif_data/
  __init__.py        public API — thin routing over the registry
  catalog.yaml       connectors + the datasets they serve (URLs, symbols, auth shape)
  core/
    http.py          HttpClient — connection reuse, token-bucket rate limit,
                     retry/backoff, pluggable auth (query-key | bearer | none),
                     secret-redacting request logs
    connector.py     Connector ABC + Dataset; base owns per-symbol cache, combine,
                     schema-validate, date-slice
    registry.py      ConnectorRegistry — register / resolve dataset->connector / route
    schema.py        lightweight column+dtype validation of every result
    errors.py        AuthError · NotEntitled · RateLimited · UpstreamError · SchemaError · DatasetNotFound
  connectors/
    cboe.py  fred.py  massive.py     one Connector subclass per vendor
```

**Add a vendor:** subclass `Connector` (implement `datasets()` + `_fetch_symbol()`),
add a block to `catalog.yaml`, `REGISTRY.register(...)` in `__init__.py`.

**Add a dataset to an existing vendor:** a new `Dataset` in its `datasets()` + a
catalog entry; if the response shape differs, that's all in `_fetch_symbol` /
`_combine`.

## Errors
```python
try:
    nd.bars("SPY")
except nd.AuthError:      ...   # no / rejected key
except nd.NotEntitled:    ...   # plan doesn't include it (403)
except nd.RateLimited:    ...   # 429, retries exhausted
except nd.UpstreamError:  ...   # 5xx / network / bad response
```

## What's fetchable
| dataset | getter | notes |
|---|---|---|
| `vol_index` | `nd.cboe(*symbols)` | any CBOE index publishing `<SYM>_History.csv` (VIX, VIX1D/9D/3M/6M, VVIX, VXN, RVX, VXTLT, GVZ, OVX, SKEW, …) |
| `macro_series` | `nd.fred(*ids)` | catalog aliases **or** any raw FRED id. ICE BofA OAS series are licence-capped to ~3y on the public CSV — use Moody's `BAA10Y` |
| `daily_bars` | `nd.bars(*tickers)` | adjusted daily OHLCV back to ~2003; multi-year vendor history holes auto-trimmed |
