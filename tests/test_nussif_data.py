"""Offline tests: parsing, schema, registry, HTTP client behaviour. No real network."""
import io
import os
import tempfile
import urllib.error

import pandas as pd
import pytest

import nussif_data as nd
from nussif_data import _util
from nussif_data.connectors.cboe import _parse_history
from nussif_data.core import HttpClient, QueryKeyAuth
from nussif_data.core.errors import AuthError, NotEntitled, RateLimited, SchemaError
from nussif_data.core.http import _redact
from nussif_data.core.schema import BARS_LONG, Schema


# --- catalog / registry ---------------------------------------------------
def test_catalog_and_connectors():
    cat = nd.catalog()
    assert cat["vol_index"]["connector"] == "cboe"
    assert cat["daily_bars"]["connector"] == "massive"
    assert cat["daily_bars"]["needs_key"] is True
    assert "VIX" in cat["vol_index"]["default_symbols"]
    assert set(nd.connectors()) == {"cboe", "fred", "massive"}


def test_registry_unknown_dataset():
    from nussif_data.core.errors import DatasetNotFound
    with pytest.raises(DatasetNotFound):
        nd.REGISTRY.fetch("no_such_dataset", ["X"])


# --- CBOE parser --------------------------------------------------------
def test_cboe_parser_handles_title_line():
    raw = (b"Cboe VIX Index History\nDATE,OPEN,HIGH,LOW,CLOSE\n"
           b"01/02/2020,13.00,14.00,12.50,13.78\n01/03/2020,13.8,13.9,13.1,13.2\n")
    df = _parse_history("VIX", raw)
    assert list(df.columns) == ["date", "VIX"]
    assert df.loc[df.date == pd.Timestamp("2020-01-02"), "VIX"].iloc[0] == 13.78


def test_cboe_parser_single_value_column():
    df = _parse_history("VVIX", b"DATE,VVIX\n03/06/2006,71.73\n03/07/2006,72.10\n")
    assert list(df.columns) == ["date", "VVIX"] and len(df) == 2


# --- schema -----------------------------------------------------------
def test_schema_missing_column_raises():
    with pytest.raises(SchemaError, match="missing column 'date'"):
        BARS_LONG.validate(pd.DataFrame({"ticker": ["SPY"]}), where="t")


def test_schema_wildcard_dtype():
    s = Schema({"date": "datetime", "*": "float"})
    ok = pd.DataFrame({"date": pd.to_datetime(["2020-01-01"]), "VIX": [13.0]})
    s.validate(ok)
    bad = ok.assign(VIX=["oops"])
    with pytest.raises(SchemaError):
        s.validate(bad)


# --- util -----------------------------------------------------------
def test_slice_dates_loose_input():
    df = pd.DataFrame({"date": pd.to_datetime(["2019-06-01", "2020-06-01", "2021-06-01"]), "x": [1, 2, 3]})
    assert list(_util.slice_dates(df, start="2020")["x"]) == [2, 3]
    assert list(_util.slice_dates(df, end="2020-06-01")["x"]) == [1, 2]


# --- HTTP client ------------------------------------------------------
def test_redact_strips_key():
    assert _redact("https://x/y?apiKey=SECRET&z=1") == "https://x/y?apiKey=***&z=1"


class _FakeHTTPError(urllib.error.HTTPError):
    def __init__(self, code, body=b""):
        super().__init__("http://x", code, "err", {}, io.BytesIO(body))


def test_http_401_is_autherror(monkeypatch):
    def boom(*a, **k):
        raise _FakeHTTPError(401, b"nope")
    monkeypatch.setattr("urllib.request.urlopen", boom)
    with pytest.raises(AuthError):
        HttpClient(name="t", retries=1).get_bytes("http://x/")


def test_http_403_is_notentitled(monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: (_ for _ in ()).throw(_FakeHTTPError(403)))
    with pytest.raises(NotEntitled):
        HttpClient(name="t", retries=1).get_bytes("http://x/")


def test_http_429_retries_then_ratelimited(monkeypatch):
    calls = {"n": 0}

    def always_429(*a, **k):
        calls["n"] += 1
        raise _FakeHTTPError(429)
    monkeypatch.setattr("urllib.request.urlopen", always_429)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    with pytest.raises(RateLimited):
        HttpClient(name="t", retries=3).get_bytes("http://x/")
    assert calls["n"] == 3


def test_query_key_auth_injects_and_wraps_error():
    a = QueryKeyAuth("apiKey", lambda: (_ for _ in ()).throw(RuntimeError("no key")))
    with pytest.raises(AuthError):
        a.apply("http://x", {}, {})
    a2 = QueryKeyAuth("apiKey", lambda: "K")
    p = {}
    a2.apply("http://x", p, {})
    assert p == {"apiKey": "K"}


def test_missing_massive_key_is_clear(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    from nussif_data import _config
    monkeypatch.setattr(_config, "KEYS_FILE", os.path.join(tempfile.gettempdir(), "nope.env"))
    _config._MEM.clear()
    with pytest.raises(RuntimeError, match="no API key for 'massive'"):
        _config.get_key("massive")
