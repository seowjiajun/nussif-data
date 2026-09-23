# Data

Market data comes from the standalone **`nussif-data`** package
(`/home/jseow/code/nussif-data` — CBOE, FRED, Massive; cached under `~/.cache/nussif-data/`).

```python
import nussif_data as nd
nd.cboe.vol_index("VIX", "VIX3M", "VXTLT")
nd.fred.series("BAA10Y", "NFCI")
nd.massive.bars("SPY", "QQQ", "TLT", start="2015")
```
Massive/Databento need `$MASSIVE_API_KEY` / `$DATABENTO_API_KEY` (or a line in `~/.config/nussif-data/keys.env`, chmod 600). CBOE/FRED are keyless. No Python key setter — keys stay out of committed code.
Dev install: `pip install -e ../nussif-data`.

The VRP panel is assembled inline in the `nussif-research` repo (`notebooks/vrp_baseline.ipynb`)
(fetch via `nd` + `nussif_quant` estimators) — no intermediate parquet.

---

## Massive option-data findings (for P2 — not in nussif-data yet)

Kept here because P2's chain-history decision depends on them.

Earliest-date probe re-run **2026-09-05** (shared key, base `https://api.massive.com`,
`apiKey` query auth); scripts in scratchpad. Findings below supersede the earlier "back to 2012" note.

| what | endpoint | status |
|---|---|---|
| Option contracts (incl. expired) | `/v3/reference/options/contracts` | ✅ metadata back to **2010-02-20** (earliest expiration in DB); no pricing behind pre-2014 contracts. `as_of` param IGNORED |
| Option daily bars | `/v2/aggs/ticker/{O:...}/range/1/day/...` | ✅ trade OHLC + vol + VWAP, **no bid/ask, no OI**; 1 call/contract (whole life). **Hard floor 2014-06-02** — every expiry living past that date starts exactly there, nothing before |
| Option trades (tick) | `/v3/trades/{O:...}` | ✅ same **2014-06-02** floor (bars are built from these) |
| Option EOD NBBO | `/v3/quotes/{O:...}?timestamp.gte=...T{close-5m}&order=desc&limit=1` | ✅ real bid/ask, back to **2022-03-07** (09:30 ET, same first day for every contract); 1 call per contract-**day** |
| Chain snapshot | `/v3/snapshot/options/{underlier}` | ⚠️ **current only**; greeks+IV+OI, **no bid/ask** on this tier. `as_of=<past date>` silently ignored — 2015 / 2018 / 2020 / 2022 all return today's chain verbatim |
| Index options (I:VIX, I:VIX1D, …) | `/v2/aggs/ticker/I:VIX/...` | ❌ **403 NOT_AUTHORIZED** — plan has no index data |
| Grouped daily (all names, 1 call) | `/v2/aggs/grouped/.../market/{m}/{date}` | stocks/fx/crypto only — **not options** |
| Flat files (S3) | `s3://flatfiles` @ `files.massive.com` | ❌ **CONFIRMED NOT available on this account** (2026-09) |

**No historical option-chain endpoint exists** on any tier, and **no flat-file
access** — so a past chain must be reconstructed per-contract, or bought from a 3rd party.

### Option-chain history — candidate sources for P2 (ranked, 2026-09)
1. **WRDS / OptionMetrics IvyDB** — if NUS has access (likely NOT — NUS WRDS = CRSP/Compustat/ISS per public sources; user verifying via WRDS login). Clean daily chains, IV/greeks/OI, 1996+. **← check first; moots the rest.**
2. **Alpha Vantage `HISTORICAL_OPTIONS`** — connector built (`nd.alphavantage.option_chain`). One call = one date's full EOD chain (bid/ask, greeks, IV, OI). **Live-tested: PREMIUM endpoint — a free key raises `NotEntitled`.** ~$50/mo (75 req/min) → SPY/QQQ/TLT/GLD × ~185 weekly dates ≈ 740 calls in ~10 min, then downgrade. One-time ~$50.
3. **optionsdx.com** — free monthly CSV downloads (registration), SPY/QQQ EOD + 15-min intraday, ~2020+. CBOE-derived. Manual-ish assembly.
4. **DoltHub `post-no-preference/options`** — free `dolt clone`, EOD chains + greeks/IV/OI, ~2019+; updates may have lapsed.
5. **Massive REST roll** (`scripts/roll_chain.py`) — last resort. EOD NBBO back to 2022-03-07, but ~100–200k `/v3/quotes` calls on the **shared 9-person key**; a rate-capped background job over days.
