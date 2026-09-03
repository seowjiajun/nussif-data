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
    assert cat["vol_index"]["description"].startswith("CBOE")
    assert set(nd.connectors()) == {"cboe", "fred", "massive", "alphavantage", "databento"}


def test_registry_unknown_dataset():
    from nussif_data.core.errors import DatasetNotFound

    with pytest.raises(DatasetNotFound):
        nd.REGISTRY.fetch("no_such_dataset", ["X"])


# --- CBOE parser --------------------------------------------------------
def test_cboe_parser_handles_title_line():
    raw = (
        b"Cboe VIX Index History\nDATE,OPEN,HIGH,LOW,CLOSE\n"
        b"01/02/2020,13.00,14.00,12.50,13.78\n01/03/2020,13.8,13.9,13.1,13.2\n"
    )
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


def test_schema_date_family_normalises_to_naive_midnight():
    s = Schema({"date": "date", "*": "float"})
    # tz-aware, non-midnight (Massive stamps daily bars at 05:00 UTC) + a string date
    raw = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-02 05:00", "2020-01-03 05:00"], utc=True),
            "VIX": [13.0, 14.0],
        }
    )
    out = s.validate(raw)
    assert out["date"].dt.tz is None
    assert (out["date"] == pd.to_datetime(["2020-01-02", "2020-01-03"])).all()
    assert raw["date"].dt.tz is not None  # input not mutated


# --- util -----------------------------------------------------------
def test_slice_dates_loose_input():
    df = pd.DataFrame(
        {"date": pd.to_datetime(["2019-06-01", "2020-06-01", "2021-06-01"]), "x": [1, 2, 3]}
    )
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
    monkeypatch.setattr(
        "urllib.request.urlopen", lambda *a, **k: (_ for _ in ()).throw(_FakeHTTPError(403))
    )
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
    with pytest.raises(RuntimeError, match="no API key for 'massive'"):
        _config.get_key("massive")


def test_no_python_key_setter():
    # keys must never be settable from Python -- env / keys.env only
    import nussif_data as nd
    from nussif_data import _config

    assert not hasattr(nd, "set_key")
    assert not hasattr(_config, "set_key")


# --- file export + CLI ------------------------------------------------
def test_write_frame_roundtrip(tmp_path):
    from nussif_data._util import write_frame

    df = pd.DataFrame({"date": pd.to_datetime(["2020-01-01", "2020-01-02"]), "VIX": [13.0, 14.0]})
    p1 = write_frame(df, tmp_path / "x.parquet")
    pd.testing.assert_frame_equal(df, pd.read_parquet(p1))
    p2 = write_frame(df, tmp_path / "x.csv")
    pd.testing.assert_frame_equal(df, pd.read_csv(p2, parse_dates=["date"]))


def test_write_frame_rejects_unknown_ext(tmp_path):
    from nussif_data._util import write_frame

    with pytest.raises(ValueError, match="unsupported output extension"):
        write_frame(pd.DataFrame({"a": [1]}), tmp_path / "x.txt")


def test_bars_field_validation():
    with pytest.raises(ValueError, match="field must be one of"):
        nd.massive.bars("SPY", field="bogus")


def test_vendor_namespaces_and_shorthand():
    # namespaced accessor and the nd.<vendor>(...) primary shorthand both exist
    assert callable(nd.cboe.vol_index) and callable(nd.fred.series) and callable(nd.massive.bars)
    assert callable(nd.cboe) and callable(nd.fred) and callable(nd.massive)
    assert nd.cboe.primary_method == "vol_index" and nd.massive.primary_method == "bars"
    import pytest as _pt

    with _pt.raises(ValueError, match="needs at least one"):
        nd.cboe.vol_index()


def test_cli_catalog(capsys):
    from nussif_data.cli import main

    assert main(["catalog"]) == 0
    assert "vol_index" in capsys.readouterr().out


def test_cli_parser_has_end_and_field():
    from nussif_data.cli import _build_parser

    p = _build_parser()
    ns = p.parse_args(
        [
            "massive",
            "SPY",
            "--start",
            "2020",
            "--end",
            "2021",
            "--field",
            "close",
            "-o",
            "x.parquet",
        ]
    )
    assert (ns.start, ns.end, ns.field, ns.out) == ("2020", "2021", "close", "x.parquet")


def test_raw_parsers_keep_vendor_columns():
    from nussif_data.connectors.cboe import _read_history

    raw = b"Cboe VIX History\nDATE,OPEN,HIGH,LOW,CLOSE\n01/02/2020,13,14,12.5,13.78\n"
    df = _read_history(raw)
    assert list(df.columns) == ["DATE", "OPEN", "HIGH", "LOW", "CLOSE"]  # not renamed to date/VIX
    assert df["CLOSE"].iloc[0] == 13.78


# --- flatten_symbols: *args or a single list, never explode a lone string ---
def test_flatten_symbols_varargs_and_list():
    assert _util.flatten_symbols(("A", "B")) == ["A", "B"]
    assert _util.flatten_symbols((["A", "B"],)) == ["A", "B"]
    assert _util.flatten_symbols((("A", "B"),)) == ["A", "B"]
    assert set(_util.flatten_symbols(({"A", "B"},))) == {"A", "B"}
    assert _util.flatten_symbols(("A",)) == ["A"]  # lone string -> one symbol, not chars


# --- alphavantage connector (offline) --------------------------------------
def test_alphavantage_in_registry():
    assert nd.catalog()["option_chain"]["connector"] == "alphavantage"
    assert set(nd.catalog()["option_chain"]["providers"]) == {"alphavantage", "databento"}
    assert nd.catalog()["option_chain"]["needs_key"] is True
    assert callable(nd.alphavantage.option_chain)


def test_alphavantage_parse_chain(monkeypatch):
    canned = {
        "endpoint": "Historical Options",
        "data": [
            {
                "contractID": "SPY240621C00530000",
                "symbol": "SPY",
                "expiration": "2024-06-21",
                "strike": "530.00",
                "type": "call",
                "last": "1.23",
                "mark": "1.25",
                "bid": "1.20",
                "bid_size": "10",
                "ask": "1.30",
                "ask_size": "8",
                "volume": "100",
                "open_interest": "5000",
                "date": "2024-06-03",
                "implied_volatility": "0.12",
                "delta": "0.30",
                "gamma": "0.02",
                "theta": "-0.05",
                "vega": "0.10",
                "rho": "0.01",
            },
            {
                "contractID": "SPY240621P00520000",
                "symbol": "SPY",
                "expiration": "2024-06-21",
                "strike": "520.00",
                "type": "put",
                "last": "0.80",
                "mark": "0.82",
                "bid": "0.78",
                "bid_size": "4",
                "ask": "0.86",
                "ask_size": "6",
                "volume": "50",
                "open_interest": "3000",
                "date": "2024-06-03",
                "implied_volatility": "0.14",
                "delta": "-0.25",
                "gamma": "0.02",
                "theta": "-0.04",
                "vega": "0.09",
                "rho": "-0.01",
            },
        ],
    }
    monkeypatch.setattr(nd.alphavantage.http, "get_json", lambda *a, **k: canned)
    df = nd.alphavantage._fetch_chain("SPY", "2024-06-03")
    assert list(df["type"]) == ["put", "call"]  # sorted by strike then type
    assert df["strike"].dtype.kind == "f" and df["date"].dtype.kind == "M"
    assert df.loc[df.type == "call", "bid"].iloc[0] == 1.20


def test_alphavantage_premium_maps_to_NotEntitled(monkeypatch):
    note = {"Information": "Thank you for using Alpha Vantage! This is a premium endpoint."}
    monkeypatch.setattr(nd.alphavantage.http, "get_json", lambda *a, **k: note)
    with pytest.raises(nd.NotEntitled, match="premium key"):
        nd.alphavantage._fetch_chain("SPY", "2024-06-03")


def test_alphavantage_daily_cap_maps_to_RateLimited(monkeypatch):
    note = {"Note": "You have exceeded the standard API call frequency of 25 requests per day."}
    monkeypatch.setattr(nd.alphavantage.http, "get_json", lambda *a, **k: note)
    with pytest.raises(nd.RateLimited):
        nd.alphavantage._fetch_chain("SPY", "2024-06-03")


def test_alphavantage_empty_is_upstream(monkeypatch):
    monkeypatch.setattr(nd.alphavantage.http, "get_json", lambda *a, **k: {"data": []})
    with pytest.raises(nd.UpstreamError):
        nd.alphavantage._fetch_chain("SPY", "1990-01-01")


# --- databento connector (offline) --------------------------------------
def test_databento_registered():
    assert "databento" in nd.connectors()
    assert callable(nd.databento.option_chain)


def test_databento_close_utc_handles_dst():
    from nussif_data.connectors.databento import _close_utc

    edt = _close_utc("2024-06-03", "America/New_York", "16:00")  # EDT -> 20:00Z
    est = _close_utc("2024-01-03", "America/New_York", "16:00")  # EST -> 21:00Z
    assert (edt.hour, est.hour) == (20, 21)


def test_databento_filter_moneyness_and_dte():
    from nussif_data.connectors.databento import DatabentoConnector

    defn = pd.DataFrame(
        {
            "raw_symbol": [f"O{i}" for i in range(6)],
            "instrument_id": range(6),
            "strike_price": [80.0, 95.0, 100.0, 105.0, 130.0, 100.0],
            "expiration": pd.to_datetime(
                ["2024-06-21", "2024-06-21", "2024-06-21", "2024-06-21", "2024-06-21", "2025-06-20"]
            ).tz_localize("UTC"),
            "instrument_class": ["P", "P", "C", "C", "C", "C"],
        }
    )
    out = DatabentoConnector._filter(defn, "2024-06-03", spot=100.0, mny=0.10, ndte=0, mdte=150)
    assert set(out["strike"]) == {95.0, 100.0, 105.0}  # 80/130 out of band; 2025 expiry > 150 DTE

    # min_dte floor drops the near expiries (18 DTE here) -> only the 2025 leg
    far = DatabentoConnector._filter(defn, "2024-06-03", spot=100.0, mny=0.10, ndte=30, mdte=400)
    assert set(far["strike"]) == {100.0}  # the 2025-06-20 contract


def test_databento_get_quotes_chunks_above_symbol_cap():
    from nussif_data.connectors.databento import DatabentoConnector

    calls = []

    class _FakeData:
        def to_df(self):
            return pd.DataFrame({"instrument_id": [], "bid_px_00": [], "ask_px_00": []})

    class _FakeTS:
        def get_range(self, **kw):
            calls.append(len(kw["symbols"]))
            return _FakeData()

    class _FakeClient:
        timeseries = _FakeTS()

    conn = DatabentoConnector.__new__(DatabentoConnector)
    conn._cli = _FakeClient()
    spec = {"close_tz": "America/New_York", "close_time": "16:00", "max_quote_symbols": 2000}
    conn._get_quotes([f"O{i}" for i in range(4500)], "2024-06-03", spec)
    assert calls == [2000, 2000, 500]  # split into <=cap batches


def test_databento_assemble_to_canonical():
    from nussif_data.connectors.databento import DatabentoConnector
    from nussif_data.core.schema import OPTION_CHAIN

    defn = pd.DataFrame(
        {
            "raw_symbol": ["A", "B"],
            "instrument_id": [1, 2],
            "strike": [100.0, 105.0],
            "expiration": pd.to_datetime(["2024-06-21", "2024-06-21"]),
            "instrument_class": ["C", "P"],
        }
    )
    close = pd.Timestamp("2024-06-03T20:00:00Z")
    quotes = pd.DataFrame(
        {
            "instrument_id": [1, 1, 2],
            "ts_event": [
                close - pd.Timedelta(minutes=2),
                close - pd.Timedelta(minutes=1),
                close - pd.Timedelta(minutes=1),
            ],
            "bid_px_00": [1.20, 1.25, 0.80],
            "ask_px_00": [1.30, 1.35, 0.90],
            "bid_sz_00": [10, 12, 4],
            "ask_sz_00": [8, 9, 6],
        }
    )
    spec = {"close_tz": "America/New_York", "close_time": "16:00"}
    out = DatabentoConnector._assemble("SPY", "2024-06-03", defn, quotes, spec)
    OPTION_CHAIN.validate(out, where="test")
    assert list(out["right"]) == ["C", "P"]
    assert out.loc[out.right == "C", "bid"].iloc[0] == 1.25  # latest pre-close quote for id 1
    assert (out["symbol"] == "SPY").all()


def test_massive_intraday_parse_and_rth_filter(monkeypatch, tmp_path):
    import nussif_data as nd

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))  # never touch the real cache

    # two 5-min bars: 10:00 ET (RTH) and 03:00 ET (pre-market), as Polygon ms epochs
    ny = "America/New_York"
    b_rth = int(pd.Timestamp("2020-06-02 10:00", tz=ny).tz_convert("UTC").timestamp() * 1000)
    b_pre = int(pd.Timestamp("2020-06-02 03:00", tz=ny).tz_convert("UTC").timestamp() * 1000)
    canned = {
        "results": [
            {"t": b_pre, "o": 1, "h": 1, "l": 1, "c": 1, "v": 10, "vw": 1, "n": 2},
            {"t": b_rth, "o": 2, "h": 3, "l": 2, "c": 2.5, "v": 99, "vw": 2.4, "n": 7},
        ]
    }
    monkeypatch.setattr(nd.massive.http, "get_json", lambda *a, **k: canned)

    raw = nd.massive._fetch_intraday("spy", 5, "minute")
    assert str(raw["timestamp"].dt.tz) == ny
    assert set(raw["ticker"]) == {"SPY"}
    assert {"timestamp", "ticker", "open", "close", "vwap", "trades"} <= set(raw.columns)
    assert len(raw) == 2  # de-duped across the repeated 6-month windows

    monkeypatch.setattr(nd.massive, "_fetch_intraday", lambda *a, **k: raw)
    df = nd.massive.bars_intraday("SPY", start="2020-01-01", end="2020-12-31")  # rth=True default
    assert len(df) == 1 and df["timestamp"].iloc[0].hour == 10  # pre-market bar dropped
    df_all = nd.massive.bars_intraday("SPY", start="2020-01-01", end="2020-12-31", rth=False)
    assert len(df_all) == 2
