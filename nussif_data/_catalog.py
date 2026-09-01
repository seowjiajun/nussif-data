from __future__ import annotations

import os
from functools import lru_cache

import yaml

_PATH = os.path.join(os.path.dirname(__file__), "catalog.yaml")


@lru_cache(maxsize=1)
def load() -> dict:
    with open(_PATH) as fh:
        return yaml.safe_load(fh)


def section(vendor: str) -> dict:
    d = load()
    if vendor not in d:
        raise KeyError(f"no '{vendor}' section in {_PATH}")
    return d[vendor]


def endpoint(vendor: str, name: str) -> dict:
    v = section(vendor)
    if name not in v:
        raise KeyError(f"no endpoint '{name}' under '{vendor}' in {_PATH}")
    return v[name]
