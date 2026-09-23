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
- Every accessor takes `start=`, `end=`, `refresh=`, `out=` (write to `.parquet/.csv/.json/.feather/.xlsx`;
  a relative path resolves against `$NUSSIF_DATA_OUT`, else the cwd — this is a self-serve tool, so
  nothing hardcodes a folder, each user points their own output wherever they want),
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
**Massive**, **Alpha Vantage** and **Databento** each need one (CBOE / FRED are keyless).
There is no Python setter — a key must never be a literal that could be committed.
Easiest path: just call anything that needs one (e.g. `nd.massive.bars("SPY")`) with no
key set — in an interactive terminal, it'll prompt (input hidden, via `getpass`) and offer
to save it to `~/.config/nussif-data/keys.env` for you, so you never have to know that path
yourself. Non-interactive contexts (CI, piped stdin, scripts) never prompt — same error as
always, telling you what to set.

Or provide it out of band yourself:
```bash
export MASSIVE_API_KEY=...            # shell / CI secret
# or persist without re-exporting:
mkdir -p ~/.config/nussif-data
printf 'MASSIVE_API_KEY=%s\n' "$KEY" >> ~/.config/nussif-data/keys.env
chmod 600 ~/.config/nussif-data/keys.env
```
Resolution order: `$<VENDOR>_API_KEY` → `~/.config/nussif-data/keys.env` → interactive prompt.

## Architecture

```
nussif_data/
  __init__.py        public API — thin routing over the registry
  catalog.yaml       connectors + what they serve (URLs, auth shape), split per
                     connector into endpoints: (pure vendor calls, one name -> one
                     vendor call) and composites: (datasets this lib defined,
                     built from endpoints: named in their own `uses:` list --
                     validated against endpoints: at load time)
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
    cboe.py  fred.py                 one Connector subclass per vendor
    massive/                         massive is split: __init__.py (Connector,
      __init__.py                    thin), bars.py (daily/intraday, shared
      bars.py                        HttpClient), options.py (option_chain --
      options.py                     contracts + its own pooled quote fetch)
```

**Add a vendor:** subclass `Connector` (implement `datasets()` + `_fetch_symbol()`),
add a block to `catalog.yaml` (an `endpoints:` entry per vendor call; a
`composites:` entry only if you're assembling something the vendor doesn't
provide directly), `REGISTRY.register(...)` in `__init__.py`.

**Add a dataset to an existing vendor:** if it's one vendor call, a new
`endpoints:` entry (same shape as its neighbors); if it's built from existing
calls, a `composites:` entry with `uses: [...]` naming them. Either way, a new
`Dataset` in the connector's `datasets()`; if the response shape differs,
that's all in `_fetch_symbol` / `_combine`.

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
| `option_chain` | `nd.massive.option_chain(*symbols, date= or start=/end=).fetch(out=)` | EOD chain per `(symbol, day)` back to 2014-01, one day or a real-trading-day range. `option_chain(...)` only builds + validates the request (no network I/O); call `.fetch()` on it to actually run it, or `.estimate()` first for a cheap pre-flight time estimate (contracts-only probe + calibrated throughput projection — useful before committing to a wide date range). `.fetch(out="path.parquet")` writes the result (`.parquet`/`.csv`/`.json`/`.feather`/`.xlsx`, same convention as every other accessor's `out=`) and still returns it. For a very wide range (years), use `.download()` instead: writes each day straight to the cache as it completes (never holds the whole range in memory — `.fetch()` does), and a failing day is logged and skipped rather than aborting the rest of the range — returns `{"succeeded": [...], "failed": [...]}`, and re-running only retries the failed days. **Canonical shape** (matches alphavantage's/databento's own `option_chain()`, `nd` is a pandas-datareader-like convenience tool, not a vendor mirror) — `symbol`/`date`/`expiration`/`strike`/`right`/`bid`/`ask` plus `ticker`/`bid_size`/`ask_size`/`lookback_min_used` (fetch provenance: which fallback lookback window found this quote — a high value flags a stale close) and `timestamp` (tz-aware, the quote's actual moment, not just the requested day). Massive's untouched contract-reference and quote responses (`cfi`, `sequence_number`, `primary_exchange`, `exercise_style`, `ask_exchange`/`bid_exchange`, `sip_timestamp`, …) aren't lost — every `_get_contracts`/`_get_quotes_concurrent` call archives its raw response first, under `$NUSSIF_DATA_CACHE/massive/raw/{contracts,quotes}/<symbol>/<date>.parquet`. `raw=True` only changes `.fetch()`'s *return shape* (`{symbol: frame}` vs. one combined frame) — column content is identical either way. `.fetch(out=...)` embeds real Parquet file-level metadata (`pyarrow.parquet.read_schema(path).metadata`) recording exactly which columns `nd` added/renamed, plus fetch provenance (vendor, symbols, `nd` version, UTC fetch time) — travels with the file, and every per-day cache write gets it too, not just deliberate exports. `max_workers` tunes the per-contract quote concurrency (catalog default 80). Needs `MASSIVE_API_KEY`. |
| `exchanges` | `nd.massive.exchanges(asset_class)` | exchange id → name/mic/participant_id mapping — decodes `option_chain()`'s own `ask_exchange`/`bid_exchange` codes. One global reference table (not per-symbol), cached. `asset_class` is required (`stocks`/`options`/`crypto`/`fx`/`futures`, each a disjoint id space, no default); `"options"` is the id space (300-325) that matches `option_chain()`'s own codes. |
| `trades` | `nd.massive.trades(*tickers, date=)` | tick-level trade prints for one or more **option contract** tickers (e.g. `O:SPY240621C00540000` — use `option_chain()`'s own `ticker` column, not the underlier symbol) on one day. One row per print, every vendor field kept (price/size/exchange/conditions/sequence_number/timestamps) plus a derived tz-aware `timestamp`. Paginated (up to 50k/page). Needs `MASSIVE_API_KEY`. |
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
