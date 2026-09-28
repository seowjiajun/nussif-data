"""One-off (2026-09-28): rewrite cached massive option chains whose strike /
bid / ask columns were stored as int64 -- written before `_assemble` cast them
to float64, on days when every value came back from the JSON as a whole
number. Mixed dtypes across days stopped `cache.read_many` reading a range of
days as one table.

Each file is copied to `--backup` first, rewritten atomically through
`cache.write_parquet` with its provenance metadata kept, and re-read to check
it equals the original apart from the cast. Dry run unless `--apply`.

    python scripts/migrate_float_prices.py --backup /path/to/backup            # list
    python scripts/migrate_float_prices.py --backup /path/to/backup --apply
"""

import argparse
import glob
import os
import shutil
import uuid

import pandas as pd
import pyarrow.parquet as pq

from nussif_data.cache import cache_dir, write_parquet

COLUMNS = ("strike", "bid", "ask")


def int_columns(path: str) -> list[str]:
    schema = pq.read_schema(path)
    return [c for c in COLUMNS if c in schema.names and str(schema.field(c).type) == "int64"]


def migrate(path: str, cols: list[str], backup_root: str) -> None:
    root = cache_dir()
    backup = os.path.join(backup_root, os.path.relpath(path, root))
    os.makedirs(os.path.dirname(backup), exist_ok=True)
    shutil.copy2(path, backup)

    before = pd.read_parquet(path)
    meta = {
        k.decode(): v.decode()
        for k, v in (pq.read_metadata(path).metadata or {}).items()
        if k != b"pandas"  # pandas' own metadata is regenerated for the new dtypes
    }
    after = before.astype(dict.fromkeys(cols, "float64"))
    tmp = f"{path}.tmp.{uuid.uuid4().hex}"
    write_parquet(after, tmp, meta)
    os.replace(tmp, path)

    reread = pd.read_parquet(path)
    if not reread.equals(after) or not (reread[list(cols)] == before[list(cols)]).all().all():
        raise SystemExit(f"{path}: rewritten file doesn't match the original -- restore {backup}")
    kept = {k.decode() for k in (pq.read_metadata(path).metadata or {})}
    if not set(meta) <= kept:
        raise SystemExit(f"{path}: provenance metadata lost -- restore {backup}")


def main(backup_root: str, apply: bool) -> None:
    files = sorted(
        glob.glob(os.path.join(cache_dir(), "massive", "option_chain", "*", "*", "*.parquet"))
    )
    todo = [(f, cols) for f in files if (cols := int_columns(f))]
    print(f"{len(todo)} of {len(files)} files have int64 {'/'.join(COLUMNS)}")
    for f, cols in todo:
        if apply:
            migrate(f, cols, backup_root)
        print(("rewrote " if apply else "would rewrite ") + os.path.relpath(f, cache_dir()), cols)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--backup", required=True, help="directory the originals are copied to")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    main(args.backup, args.apply)
