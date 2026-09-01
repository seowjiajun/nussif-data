"""One HTTP client for every connector: connection reuse, token-bucket rate
limiting, retry/backoff, pluggable auth, secret-redacting request logs.

Replaces the hand-rolled urllib loops that used to live in each vendor module.
Stdlib only.
"""

from __future__ import annotations

import json as _json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable

from .errors import AuthError, NotEntitled, RateLimited, UpstreamError

log = logging.getLogger("nussif_data.http")

_SECRET_RE = re.compile(r"(apiKey|api_key|token|key)=([^&\s]+)", re.I)


def _redact(url: str) -> str:
    return _SECRET_RE.sub(r"\1=***", url)


# --- auth strategies --------------------------------------------------------
class NoAuth:
    def apply(self, url: str, params: dict, headers: dict) -> None:
        pass


class QueryKeyAuth:
    """Append ?<param>=<key> to every request. `value` is a callable so the key
    is resolved lazily (and errors surface as AuthError)."""

    def __init__(self, param: str, value: Callable[[], str]):
        self.param, self.value = param, value

    def apply(self, url: str, params: dict, headers: dict) -> None:
        try:
            params[self.param] = self.value()
        except Exception as e:
            raise AuthError(str(e)) from e


class BearerAuth:
    def __init__(self, value: Callable[[], str]):
        self.value = value

    def apply(self, url: str, params: dict, headers: dict) -> None:
        try:
            headers["Authorization"] = f"Bearer {self.value()}"
        except Exception as e:
            raise AuthError(str(e)) from e


# --- client ---------------------------------------------------------------
class HttpClient:
    def __init__(
        self,
        base_url: str = "",
        *,
        rate_limit_rpm: float | None = None,
        retries: int = 4,
        timeout: int = 45,
        auth=None,
        name: str = "http",
    ):
        self.base_url = base_url.rstrip("/")
        self.retries = retries
        self.timeout = timeout
        self.auth = auth or NoAuth()
        self.name = name
        self._min_interval = 60.0 / rate_limit_rpm if rate_limit_rpm else 0.0
        self._last = 0.0

    # -- rate limiter (token bucket of size 1) --
    def _pace(self) -> None:
        if self._min_interval:
            wait = self._min_interval - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
        self._last = time.monotonic()

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            return path_or_url
        return f"{self.base_url}{path_or_url}"

    def request(
        self, path_or_url: str, params: dict | None = None, headers: dict | None = None
    ) -> bytes:
        params = dict(params or {})
        headers = {"User-Agent": "nussif-data/0.2", **(headers or {})}
        url = self._url(path_or_url)
        self.auth.apply(url, params, headers)
        if params:
            url = f"{url}{'&' if urllib.parse.urlparse(url).query else '?'}{urllib.parse.urlencode(params)}"

        for attempt in range(self.retries):
            self._pace()
            t0 = time.monotonic()
            try:
                req = urllib.request.Request(url, headers={**headers, "Connection": "close"})
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    body = r.read()
                log.debug(
                    "%s GET %s -> %s %.0fms",
                    self.name,
                    _redact(url),
                    r.status,
                    (time.monotonic() - t0) * 1000,
                )
                return body
            except urllib.error.HTTPError as e:
                body = e.read()[:300]
                log.debug(
                    "%s GET %s -> %s (attempt %d) %s",
                    self.name,
                    _redact(url),
                    e.code,
                    attempt + 1,
                    body[:120],
                )
                if e.code == 401:
                    raise AuthError(f"{self.name}: 401 {body!r}") from e
                if e.code == 403:
                    raise NotEntitled(f"{self.name}: 403 {body!r}") from e
                if e.code == 429:
                    ra = e.headers.get("Retry-After")
                    time.sleep(float(ra) if ra and ra.isdigit() else 2.0 * (attempt + 1))
                    last = RateLimited(f"{self.name}: 429 after {self.retries} tries")
                    continue
                if 500 <= e.code < 600:
                    time.sleep(1.5 * (attempt + 1))
                    last = UpstreamError(f"{self.name}: {e.code} {body!r}")
                    continue
                raise UpstreamError(f"{self.name}: {e.code} {body!r}") from e
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                log.debug(
                    "%s GET %s -> transport err (attempt %d): %s",
                    self.name,
                    _redact(url),
                    attempt + 1,
                    e,
                )
                time.sleep(1.5 * (attempt + 1))
                last = UpstreamError(f"{self.name}: transport error: {e}")
        raise last

    def get_bytes(self, path_or_url: str, params: dict | None = None) -> bytes:
        return self.request(path_or_url, params)

    def get_json(self, path: str, params: dict | None = None) -> dict:
        return _json.loads(self.request(path, params).decode("utf-8", "replace"))
