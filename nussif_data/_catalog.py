from __future__ import annotations

import os
from functools import lru_cache

import yaml

_PATH = os.path.join(os.path.dirname(__file__), "catalog.yaml")


@lru_cache(maxsize=1)
def load() -> dict:
    with open(_PATH) as fh:
        return yaml.safe_load(fh)


def connectors() -> dict:
    return load()["connectors"]


def connector_cfg(name: str) -> dict:
    c = connectors()
    if name not in c:
        raise KeyError(f"no connector '{name}' in {_PATH}")
    return c[name]
