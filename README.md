# nussif-data

Vendor-namespaced access to multiple market-data sources — CBOE, FRED, Massive
(yfinance and others slot in the same way).

```python
import nussif_data as nd

nd.cboe.vol_index("VIX", "VIX3M", "VXTLT")   # or  nd.cboe("VIX", "VIX3M")
nd.fred.series("BAA10Y", "NFCI", "UNRATE")   # or  nd.fred("BAA10Y", "NFCI")
nd.massive.bars("SPY", "QQQ", start="2015")  # or  nd.massive("SPY", "QQQ")

nd.massive.bars("SPY", "QQQ", field="close") # WIDE by ticker (matches cboe/fred shape)
nd.cboe.vol_index("VIX", raw=True)           # {symbol: vendor frame verbatim}
```

- `nd.<vendor>.<dataset>(...)`; `nd.<vendor>(...)` is shorthand for the vendor's primary dataset.
- Every accessor takes `start=`, `end=`, `refresh=`, `out=` (write to `.parquet/.csv/.json/.feather`),
  `raw=` (skip renaming/coercion/reshaping — returns `{symbol: frame}`).
- Results cache per symbol to `~/.cache/nussif-data/` (override `$NUSSIF_DATA_CACHE`).
- `nd.catalog()` — dataset → connector; `nd.connectors()` — connectors & their datasets.

## CLI
```bash
nussif-data cboe VIX VIX3M --start 2015 -o vix.parquet
nussif-data fred BAA10Y NFCI --start 2010 --end 2020 -o macro.csv
nussif-data massive SPY QQQ TLT --start 2020 --field close -o closes.parquet
nussif-data massive SPY --raw --head 5
nussif-data catalog
```

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
    nd.massive.bars("SPY")
except nd.AuthError:      ...   # no / rejected key
except nd.NotEntitled:    ...   # plan doesn't include it (403)
except nd.RateLimited:    ...   # 429, retries exhausted
except nd.UpstreamError:  ...   # 5xx / network / bad response
```

## What's fetchable
| dataset | getter | notes |
|---|---|---|
| `vol_index` | `nd.cboe.vol_index(*symbols)` | any CBOE index publishing `<SYM>_History.csv` (VIX, VIX1D/9D/3M/6M, VVIX, VXN, RVX, VXTLT, GVZ, OVX, SKEW, …) — pass the ones you want |
| `macro_series` | `nd.fred.series(*ids)` | any FRED id (or a friendly alias like `baa10y`). ICE BofA OAS series are licence-capped to ~3y on the public CSV — use Moody's `BAA10Y` |
| `daily_bars` | `nd.massive.bars(*tickers)` | adjusted daily OHLCV back to ~2003; multi-year vendor history holes auto-trimmed |
