"""ConnectorRegistry -- register connectors, resolve a dataset to its connector,
route fetch calls. First connector registered for a dataset wins; a preference /
fallback policy slots in here when a second provider for the same dataset exists.
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from .connector import Connector
from .errors import DatasetNotFound


class ConnectorRegistry:
    def __init__(self):
        self._by_name: dict[str, Connector] = {}
        self._dataset_providers: dict[str, list[str]] = {}

    def register(self, conn: Connector) -> None:
        self._by_name[conn.name] = conn
        for ds in conn.datasets():
            self._dataset_providers.setdefault(ds.name, [])
            if conn.name not in self._dataset_providers[ds.name]:
                self._dataset_providers[ds.name].append(conn.name)

    def connector(self, name: str) -> Connector:
        if name not in self._by_name:
            raise KeyError(f"no connector '{name}' (have {sorted(self._by_name)})")
        return self._by_name[name]

    def for_dataset(self, dataset: str) -> Connector:
        names = self._dataset_providers.get(dataset)
        if not names:
            raise DatasetNotFound(
                f"no connector serves '{dataset}' (have {sorted(self._dataset_providers)})"
            )
        return self._by_name[names[0]]

    def fetch(self, dataset: str, symbols: Sequence[str] = (), **kw) -> pd.DataFrame:
        return self.for_dataset(dataset).fetch(dataset, symbols, **kw)

    # -- introspection --
    def list(self, probe: bool = False) -> dict:
        """Connectors and their datasets. probe=True also runs each connector's
        health() (may hit the network / check keys)."""
        out = {}
        for name, c in self._by_name.items():
            row = {"datasets": [d.name for d in c.datasets()]}
            if probe:
                row["healthy"] = _safe_health(c)
            out[name] = row
        return out

    def catalog(self) -> dict:
        """dataset -> {connector (primary = first registered), providers, needs_key, description}."""
        out = {}
        for name in self._dataset_providers:
            primary = self._by_name[self._dataset_providers[name][0]]
            d = primary.dataset(name)
            out[name] = {
                "connector": primary.name,
                "providers": list(self._dataset_providers[name]),
                "needs_key": d.needs_key,
                "description": d.description,
            }
        return out


def _safe_health(c: Connector) -> bool:
    try:
        return bool(c.health())
    except Exception:
        return False
