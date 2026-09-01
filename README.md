# nussif-data

Vendor-namespaced access to multiple market-data sources — CBOE, FRED, Massive
(yfinance and others slot in the same way).

```python
import nussif_data as nd

nd.cboe.vol_index("VIX", "VIX3M", "VXTLT")   # or  nd.cboe("VIX", "VIX3M")
nd.fred.series("BAA10Y", "NFCI", "UNRATE")   # or  nd.fred("BAA10Y", "NFCI")
nd.massive.bars("SPY", "QQQ", start="2015")  # or  nd.massive("SPY", "QQQ")
nd.alphavantage.option_chain("SPY", date="2024-06-03")   # full EOD option chain (one request)

nd.massive.bars("SPY", "QQQ", field="close") # WIDE by ticker (matches cboe/fred shape)
nd.cboe.vol_index("VIX", raw=True)           # {symbol: vendor frame verbatim}
```

- `nd.<vendor>.<dataset>(...)`; `nd.<vendor>(...)` is shorthand for the vendor's primary dataset.
- Symbols as varargs (`nd.fred.series("BAA10Y", "NFCI")`) or a single list (`nd.fred.series(ids)`) — both work.
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
| `macro_series` | `nd.fred.series(*ids)` | any FRED id, FRED's own symbology — no invented aliases. ICE BofA OAS series are licence-capped to ~3y on the public CSV — use Moody's `BAA10Y` |
| `daily_bars` | `nd.massive.bars(*tickers)` | adjusted daily OHLCV back to ~2003; multi-year vendor history holes auto-trimmed |
| `option_chain` | `nd.alphavantage.option_chain(*symbols, date=)` | full EOD chain per `(symbol, date)` in **one** request — bid/ask/sizes, IV, greeks, OI. `date` back to 2008; omit for latest. OPRA-sourced quotes; IV/greeks are Alpha Vantage's own (recompute for the deep wings). **`HISTORICAL_OPTIONS` is a PREMIUM endpoint** — free keys raise `NotEntitled`. Premium ~$50/mo (75 req/min) → full backfill in minutes, then downgrade. Needs `ALPHAVANTAGE_API_KEY`. |
| `option_chain` | `nd.databento.option_chain(*symbols, date=, spot=, moneyness=, min_dte=, max_dte=)` | historical OPRA EOD chain per `(symbol, date)` back to 2013-04, pay-as-you-go (~cents/name/date). Two-stage: `definition` → filter to a moneyness / DTE band → `cbbo-1m` closing NBBO. Returns bid/ask/sizes only — **no IV/greeks/OI** (use `desk.estimators.black_scholes`; OI is a separate `statistics` pull). Pass `spot` for an accurate strike filter and `min_dte` (e.g. 15) to skip daily/weekly expiries — SPY/QQQ otherwise exceed Databento's 2,000-symbol quote cap (handled by chunking, but you pay for the extra strikes). `pip install "nussif-data[databento]"`, needs `DATABENTO_API_KEY`. |

**Finding a symbol:**
- **FRED** — search [fred.stlouisfed.org/search](https://fred.stlouisfed.org/search) by name; the id is in the result and in the series page URL (e.g. `.../series/BAA10Y` → `BAA10Y`).
- **CBOE** — [cboe.com/tradable-products/vix/vix-historical-data](https://www.cboe.com/tradable-products/vix/vix-historical-data/) lists most downloadable volatility indices (VIX, VVIX, VIX9D, OVX, GVZ, …) with their `_History.csv` links — not a complete catalog (no single one exists), but it's the same file shape `vol_index` consumes. Other indices (VIX3M, VIX6M, VXN, RVX, VXTLT, SKEW, …) are on each index's own [dashboard](https://www.cboe.com/us/indices/dashboard/vix/) page under "Historical Data."

### FRED ids this project commonly pulls

| id | series |
|---|---|
| `BAA10Y` / `AAA10Y` | Moody's Baa / Aaa corporate yield − 10y Treasury |
| `NFCI` / `ANFCI` | Chicago Fed financial conditions (weekly) |
| `SOFR` | Secured Overnight Financing Rate |
| `DGS3MO` / `DGS2` / `DGS10` | 3-month / 2y / 10y Treasury yield |
| `T10Y3M` / `T10Y2Y` | 10y − 3m / 10y − 2y term spread |
