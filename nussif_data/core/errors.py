"""Typed errors so callers can react to *why* a fetch failed."""
from __future__ import annotations


class NussifDataError(Exception):
    """Base for everything this package raises deliberately."""


class AuthError(NussifDataError):
    """Missing or rejected API key (HTTP 401 / no key configured)."""


class NotEntitled(NussifDataError):
    """Authenticated, but the plan/tier does not include this data (HTTP 403)."""


class RateLimited(NussifDataError):
    """HTTP 429 and retries exhausted."""


class UpstreamError(NussifDataError):
    """5xx, network failure, or an unparseable/empty response from the vendor."""


class SchemaError(NussifDataError):
    """A returned frame did not match the dataset's declared schema."""


class DatasetNotFound(NussifDataError):
    """No registered connector serves the requested dataset."""
