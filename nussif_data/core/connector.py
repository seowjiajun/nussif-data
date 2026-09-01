"""Connector ABC. A connector knows one vendor: how to authenticate, which
datasets it serves, and how to fetch one symbol of one dataset. The base class
owns the repetitive part -- per-symbol caching, combining, schema validation,
date slicing -- so a concrete connector is small.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

import pandas as pd

from .. import _util
from ..cache import cached
from .errors import DatasetNotFound
from .schema import Schema


class Dataset:
    def __init__(self, name: str, schema: Schema, *, needs_key: bool = False,
                 description: str = "", default_symbols: Sequence[str] = ()):
        self.name = name
        self.schema = schema
        self.needs_key = needs_key
        self.description = description
        self.default_symbols = list(default_symbols)


class Connector(ABC):
    name: str = "connector"

    # -- declare what you serve --
    @abstractmethod
    def datasets(self) -> list[Dataset]: ...

    def dataset(self, name: str) -> Dataset:
        for d in self.datasets():
            if d.name == name:
                return d
        raise DatasetNotFound(f"{self.name} has no dataset '{name}'")

    # -- the one method a connector must implement --
    @abstractmethod
    def _fetch_symbol(self, dataset: str, symbol: str) -> pd.DataFrame: ...

    # -- optional hooks --
    def _cache_symbol(self, dataset: str, symbol: str) -> str:
        """Cache key component for `symbol` (override to normalise, e.g. alias->id)."""
        return symbol

    def _combine(self, dataset: str, frames: list[pd.DataFrame]) -> pd.DataFrame:
        """Merge per-symbol frames. Default: wide outer-join on 'date'.
        Override for long/tidy output (e.g. bars)."""
        return _util.outer_merge_on_date(frames)

    def health(self) -> bool:  # cheap liveness probe; override per vendor
        return True

    # -- the template method callers hit (via the registry) --
    def fetch(self, dataset: str, symbols: Sequence[str] = (), *,
              start=None, end=None, refresh: bool = False) -> pd.DataFrame:
        ds = self.dataset(dataset)
        syms = [str(s) for s in symbols] or list(ds.default_symbols)
        if not syms:
            raise ValueError(f"{self.name}.{dataset}: no symbols and no default_symbols")
        frames = [
            cached(f"{self.name}/{dataset}/{self._cache_symbol(dataset, s)}",
                   lambda s=s: self._fetch_symbol(dataset, s), refresh=refresh)
            for s in syms
        ]
        df = self._combine(dataset, frames)
        df = ds.schema.validate(df, where=f"{self.name}.{dataset}")
        return _util.slice_dates(df, start, end)
