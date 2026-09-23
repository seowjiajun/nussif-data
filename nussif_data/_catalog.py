"""catalog.yaml loader + accessors.

`endpoints(name)` -- pure vendor calls for connector `name`: one entry name
-> one vendor call, named and shaped exactly as the vendor's own docs do.

`composites(name)` -- datasets this library defined, not the vendor's own,
each built from one or more `endpoints(name)` entries named in its own
`uses:` list. Validated against `endpoints(name)` at load time, so a
typo'd or renamed endpoint fails immediately -- not on the first live call
that happens to hit it.

(A connector not yet migrated to this split -- currently just databento --
has neither key populated; `endpoints`/`composites` return {} for it and its
own code still reads the flat `datasets:` block directly.)
"""

from __future__ import annotations

import os
from functools import lru_cache

import yaml

_PATH = os.path.join(os.path.dirname(__file__), "catalog.yaml")


@lru_cache(maxsize=1)
def load() -> dict:
    with open(_PATH) as fh:
        cfg = yaml.safe_load(fh)
    _validate(cfg)
    return cfg


def _validate(cfg: dict) -> None:
    for cname, c in cfg["connectors"].items():
        eps = set(c.get("endpoints", {}))
        for dname, d in c.get("composites", {}).items():
            for used in d.get("uses", ()):
                if used not in eps:
                    raise ValueError(
                        f"catalog.yaml: connectors.{cname}.composites.{dname} uses "
                        f"{used!r}, not declared in connectors.{cname}.endpoints "
                        f"(have {sorted(eps)})"
                    )


def connectors() -> dict:
    return load()["connectors"]


def connector_cfg(name: str) -> dict:
    c = connectors()
    if name not in c:
        raise KeyError(f"no connector '{name}' in {_PATH}")
    return c[name]


def endpoints(name: str) -> dict:
    """Pure vendor endpoints for connector `name`."""
    return connector_cfg(name).get("endpoints", {})


def composites(name: str) -> dict:
    """Derived datasets for connector `name`, each built from `endpoints(name)`."""
    return connector_cfg(name).get("composites", {})
