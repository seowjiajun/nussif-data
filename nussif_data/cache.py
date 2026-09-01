"""On-disk parquet cache + a small HTTP GET with retries.

Cache location: $NUSSIF_DATA_CACHE, else ~/.cache/nussif-data/. Keys may contain
'/' -> nested dirs (e.g. "cboe/VIX", "fred/BAA10Y", "massive/bars/SPY").
"""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.request

import pandas as pd


def cache_dir() -> str:
    d = os.environ.get("NUSSIF_DATA_CACHE") or os.path.join(
        os.path.expanduser("~"), ".cache", "nussif-data"
    )
    os.makedirs(d, exist_ok=True)
    return d


def http_get(url: str, timeout: int = 60, retries: int = 3) -> bytes:
    last = None
    for a in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "nussif-data/0.1"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = e
            time.sleep(1.5 * (a + 1))
    raise RuntimeError(f"GET failed {url}: {last}")


def cached(key: str, build_fn, refresh: bool = False) -> pd.DataFrame:
    path = os.path.join(cache_dir(), *key.split("/")) + ".parquet"
    if os.path.exists(path) and not refresh:
        return pd.read_parquet(path)
    df = build_fn()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    return df


def clear_cache(prefix: str | None = None) -> int:
    """Delete cached parquet files (optionally only those whose key starts with
    `prefix`, e.g. 'cboe'). Returns count removed."""
    root = cache_dir()
    n = 0
    for dirpath, _, files in os.walk(root):
        for f in files:
            if not f.endswith(".parquet"):
                continue
            rel = os.path.relpath(os.path.join(dirpath, f), root)
            key = rel[: -len(".parquet")].replace(os.sep, "/")
            if prefix is None or key.startswith(prefix):
                os.remove(os.path.join(dirpath, f))
                n += 1
    return n
