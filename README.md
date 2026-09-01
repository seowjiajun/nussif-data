# nussif-data

One-line access to **CBOE**, **FRED** and **Massive** market data. NUSSIF's `yfinance`.

```python
import nussif_data as nd

nd.cboe("VIX", "VIX3M", "VXTLT")          # CBOE vol indices, wide by date
nd.fred("BAA10Y", "NFCI", "UNRATE")       # any FRED series (aliases or raw ids)
nd.bars("SPY", "QQQ", start="2015")       # split/div-adjusted daily OHLCV (tidy long)
```

- Every getter takes `start=`, `end=`, `refresh=`.
- Results are cached to `~/.cache/nussif-data/` (override with `$NUSSIF_DATA_CACHE`).
- `nd.catalog()` lists the default bundles; you can always pass symbols/ids explicitly.

## Install
```bash
pip install -e .            # from a clone
# or, from another project:
pip install "nussif-data @ git+https://github.com/<you>/nussif-data.git"
```

## Keys
Only **Massive** needs one:
```bash
export MASSIVE_API_KEY=...            # or
python -c "import nussif_data as nd; nd.set_key('massive', '...')"   # -> ~/.config/nussif-data/keys.env (chmod 600)
```
CBOE and FRED are keyless.

## What's fetchable
| vendor | getter | notes |
|---|---|---|
| CBOE | `nd.cboe(*symbols)` | any index publishing `<SYM>_History.csv` (VIX, VIX1D/9D/3M/6M, VVIX, VXN, RVX, VXTLT, GVZ, OVX, SKEW, …) |
| FRED | `nd.fred(*ids)` | catalog aliases **or** any raw FRED series id. ICE BofA OAS series are licence-capped to ~3y on the public CSV — use Moody's `BAA10Y` |
| Massive | `nd.bars(*tickers)` | daily OHLCV, adjusted, back to ~2003; multi-year vendor history holes auto-trimmed |

Add a dataset: edit `nussif_data/catalog.yaml` (the *what*); a genuinely new
response shape gets a new handler function in `nussif_data/<vendor>.py` (the *how*).

## Design
- `catalog.yaml` — declarative registry of datasets + access shape
- `<vendor>.py` — auth, retries, format quirks; fetches & caches **per symbol**
- `__init__.py` — the public API
- vendor-neutral: returns tidy DataFrames, has no finance opinions
