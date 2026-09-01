"""Connector ABC. A connector knows one vendor: how to authenticate, which
datasets it serves, and how to fetch one symbol of one dataset. The base class
owns the repetitive part -- per-symbol caching, combining, schema validation,
date slicing -- so a concrete connector is small.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

import pandas as pd

from .. import _util
from ..cache import cached
from .errors import DatasetNotFound
from .schema import Schema


class Dataset:
    def __init__(
        self, name: str, schema: Schema, *, needs_key: bool = False, description: str = ""
    ):
        self.name = name
        self.schema = schema
        self.needs_key = needs_key
        self.description = description


class Connector(ABC):
    name: str = "connector"
    primary_method: str | None = None  # method name for the nd.<vendor>(...) shorthand

    def __call__(self, *symbols, **kw):
        """nd.<vendor>(...) == nd.<vendor>.<primary dataset method>(...)."""
        if self.primary_method is None:
            raise TypeError(
                f"nd.{self.name} has no default dataset — call one explicitly, "
                f"e.g. nd.{self.name}.{self.datasets()[0].name}(...)"
            )
        return getattr(self, self.primary_method)(*symbols, **kw)

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
    def _fetch_symbol(self, dataset: str, symbol: str, raw: bool = False) -> pd.DataFrame:
        """One symbol of one dataset. raw=True -> the vendor's frame verbatim
        (original column names, no coercion, no reshaping)."""

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
    def fetch(
        self,
        dataset: str,
        symbols: Sequence[str] = (),
        *,
        start=None,
        end=None,
        refresh: bool = False,
        out=None,
        raw: bool = False,
    ):
        """raw=False -> one tidy, schema-validated, date-sliced frame.
        raw=True  -> dict {symbol: vendor frame verbatim}; start/end/out not applied."""
        ds = self.dataset(dataset)
        syms = [str(s) for s in symbols]
        if not syms:
            raise ValueError(
                f"nd.{self.name}.{self.primary_method or dataset}(...) needs "
                f"at least one symbol/id/ticker"
            )
        prefix = "raw/" if raw else ""
        frames = {
            s: cached(
                f"{self.name}/{dataset}/{prefix}{self._cache_symbol(dataset, s)}",
                lambda s=s: self._fetch_symbol(dataset, s, raw=raw),
                refresh=refresh,
            )
            for s in syms
        }
        if raw:
            if out:
                raise ValueError("out= is not supported with raw=True (per-symbol payloads differ)")
            return dict(frames)
        df = self._combine(dataset, list(frames.values()))
        df = ds.schema.validate(df, where=f"{self.name}.{dataset}")
        df = _util.slice_dates(df, start, end)
        if out:
            _util.write_frame(df, out)
        return df
