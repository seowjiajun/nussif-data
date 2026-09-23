"""On-disk parquet cache + a small HTTP GET with retries.

Cache location: $NUSSIF_DATA_CACHE, else ~/.cache/nussif-data/. Keys may contain
'/' -> nested dirs (e.g. "cboe/VIX", "fred/BAA10Y", "massive/bars/SPY").

Output location (every accessor's `out=`): $NUSSIF_DATA_OUT, else the current
working directory -- see out_dir(). This is a self-serve tool (no shared
infra yet, each user runs their own copy), so a script/notebook should never
hardcode where its output lands: a bare filename in `out=` resolves against
out_dir(), which each user points wherever they want (a scratch dir, a
personal synced folder, wherever) via the env var -- same convention
cache_dir() already uses for $NUSSIF_DATA_CACHE. An absolute path in `out=`
is respected as-is, not redirected.
"""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.request
import uuid

import pandas as pd


def cache_dir() -> str:
    d = os.environ.get("NUSSIF_DATA_CACHE") or os.path.join(
        os.path.expanduser("~"), ".cache", "nussif-data"
    )
    os.makedirs(d, exist_ok=True)
    return d


def out_dir() -> str:
    """Default directory a relative `out=` path resolves against --
    $NUSSIF_DATA_OUT, else the current working directory. See this module's
    own docstring for why nothing should hardcode this instead."""
    d = os.environ.get("NUSSIF_DATA_OUT") or os.getcwd()
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


def _path_for(key: str) -> str:
    return os.path.join(cache_dir(), *key.split("/")) + ".parquet"


def write_parquet(df: pd.DataFrame, path: str, metadata: dict[str, str] | None = None) -> None:
    """The one place that actually writes a parquet file for this package --
    `cached()` and `_util.write_frame()` both call this, so provenance
    metadata is embedded identically whichever path a file was written
    through, not just deliberate `out=` exports."""
    if metadata:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(df, preserve_index=False)
        existing = table.schema.metadata or {}
        combined = {**existing, **{k.encode(): v.encode() for k, v in metadata.items()}}
        pq.write_table(table.replace_schema_metadata(combined), path)
    else:
        df.to_parquet(path, index=False)


def cached(
    key: str, build_fn, refresh: bool = False, metadata: dict[str, str] | None = None
) -> pd.DataFrame:
    """`metadata` -- string key/value pairs embedded as real Parquet
    file-level metadata (see `write_parquet`) on this specific write. Only
    applied when the cache is actually (re)built -- reading an existing
    cache hit never rewrites it, so passing `metadata` doesn't retroactively
    add it to a file already on disk from before this was called with it."""
    path = _path_for(key)
    if os.path.exists(path) and not refresh:
        return pd.read_parquet(path)
    df = build_fn()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # write atomically -- build the file fully under a temp name, then swap it
    # into place with one filesystem-level rename, so a reader (this process
    # next run, or another one sharing the cache dir) never sees a partial
    # parquet file, e.g. if this process is killed mid-write during a long
    # option_chain backfill.
    tmp = f"{path}.tmp.{uuid.uuid4().hex}"
    try:
        write_parquet(df, tmp, metadata)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return df


def is_cached(key: str) -> bool:
    """Whether `key` already has a cached result on disk -- lets a caller skip
    work for free (no build_fn call) instead of paying for a fetch just to
    find out it would've been a cache hit anyway."""
    return os.path.exists(_path_for(key))


def cache_summary() -> list[dict]:
    """One row per top-level `<vendor>/<dataset>` key prefix under the cache
    root: file count, total size, most recent write. Walks the whole cache
    once (os.stat only, no parquet reads) -- meant for interactive browsing
    (repl.py's `cache` command), not a hot path."""
    root = cache_dir()
    groups: dict[str, dict] = {}
    for dirpath, _, files in os.walk(root):
        for f in files:
            if not f.endswith(".parquet"):
                continue
            full = os.path.join(dirpath, f)
            rel = os.path.relpath(full, root)
            key = rel[: -len(".parquet")].replace(os.sep, "/")
            parts = key.split("/")
            prefix = "/".join(parts[:2]) if len(parts) > 1 else parts[0]
            st = os.stat(full)
            g = groups.setdefault(
                prefix, {"prefix": prefix, "files": 0, "size_bytes": 0, "newest": 0.0}
            )
            g["files"] += 1
            g["size_bytes"] += st.st_size
            g["newest"] = max(g["newest"], st.st_mtime)
    return sorted(groups.values(), key=lambda g: g["prefix"])


def cache_files(prefix: str = "") -> list[dict]:
    """Every cached file whose key starts with `prefix` (e.g.
    'massive/option_chain/QQQ') -- key, size, mtime. Empty prefix matches
    everything. For drilling into one `cache_summary()` row."""
    root = cache_dir()
    rows = []
    for dirpath, _, files in os.walk(root):
        for f in files:
            if not f.endswith(".parquet"):
                continue
            full = os.path.join(dirpath, f)
            rel = os.path.relpath(full, root)
            key = rel[: -len(".parquet")].replace(os.sep, "/")
            if not key.startswith(prefix):
                continue
            st = os.stat(full)
            rows.append({"key": key, "size_bytes": st.st_size, "mtime": st.st_mtime})
    return sorted(rows, key=lambda r: r["key"])


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
