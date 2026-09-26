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
from nussif_data.core.errors import AuthError, NotEntitled, RateLimited, SchemaError, UpstreamError
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


def test_get_key_never_prompts_when_not_interactive(monkeypatch):
    # sanity check on the guard itself, independent of pytest's own env var --
    # a real non-tty stdin with no ipykernel loaded (true in every CI/plain
    # test runner) must never prompt
    from nussif_data import _config

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(_config.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(
        _config.sys, "modules", {k: v for k, v in _config.sys.modules.items() if k != "ipykernel"}
    )
    assert _config._can_prompt() is False


def test_get_key_can_prompt_inside_jupyter_kernel(monkeypatch):
    # the actual bug this guards against: sys.stdin.isatty() is False inside
    # every real Jupyter kernel (verified live), but ipykernel monkey-patches
    # getpass.getpass/input to a masked input box in the notebook UI, so it IS
    # interactive -- isatty() alone would silently disable prompting in the
    # environment this library is mostly used in.
    from nussif_data import _config

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(_config.sys.stdin, "isatty", lambda: False)
    monkeypatch.setitem(_config.sys.modules, "ipykernel", object())
    assert _config._can_prompt() is True


def test_get_key_prompts_and_persists_when_interactive(monkeypatch, tmp_path):
    from nussif_data import _config

    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    keys_file = tmp_path / "keys.env"
    monkeypatch.setattr(_config, "CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(_config, "KEYS_FILE", str(keys_file))
    monkeypatch.setattr(_config, "_can_prompt", lambda: True)
    monkeypatch.setattr(_config.getpass, "getpass", lambda prompt: "PASTED_KEY")
    monkeypatch.setattr("builtins.input", lambda prompt: "y")

    key = _config.get_key("massive")
    assert key == "PASTED_KEY"
    assert keys_file.exists()
    assert keys_file.read_text().strip() == "MASSIVE_API_KEY=PASTED_KEY"
    assert oct(os.stat(keys_file).st_mode)[-3:] == "600"

    # second call resolves straight from the now-persisted file -- must not prompt again
    monkeypatch.setattr(
        _config,
        "_can_prompt",
        lambda: (_ for _ in ()).throw(AssertionError("should not prompt again")),
    )
    assert _config.get_key("massive") == "PASTED_KEY"


def test_get_key_prompt_declines_save(monkeypatch, tmp_path):
    from nussif_data import _config

    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    keys_file = tmp_path / "keys.env"
    monkeypatch.setattr(_config, "CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(_config, "KEYS_FILE", str(keys_file))
    monkeypatch.setattr(_config, "_can_prompt", lambda: True)
    monkeypatch.setattr(_config.getpass, "getpass", lambda prompt: "EPHEMERAL_KEY")
    monkeypatch.setattr("builtins.input", lambda prompt: "n")

    key = _config.get_key("alphavantage")
    assert key == "EPHEMERAL_KEY"
    assert not keys_file.exists()  # declined -- nothing persisted


def test_get_key_prompt_eof_falls_through_to_runtime_error(monkeypatch, tmp_path):
    from nussif_data import _config

    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setattr(_config, "KEYS_FILE", str(tmp_path / "nope.env"))
    monkeypatch.setattr(_config, "_can_prompt", lambda: True)

    def _boom(prompt):
        raise EOFError

    monkeypatch.setattr(_config.getpass, "getpass", _boom)
    with pytest.raises(RuntimeError, match="no API key for 'massive'"):
        _config.get_key("massive")


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


def test_write_frame_relative_path_resolves_against_nussif_data_out(monkeypatch, tmp_path):
    from nussif_data._util import write_frame

    monkeypatch.setenv("NUSSIF_DATA_OUT", str(tmp_path))
    p = write_frame(pd.DataFrame({"a": [1]}), "sub/x.parquet")
    assert p == str(tmp_path / "sub" / "x.parquet")
    assert os.path.exists(p)


def test_write_frame_absolute_path_ignores_nussif_data_out(monkeypatch, tmp_path):
    from nussif_data._util import write_frame

    other = tmp_path / "elsewhere"
    other.mkdir()
    monkeypatch.setenv("NUSSIF_DATA_OUT", str(tmp_path / "not-here"))
    p = write_frame(pd.DataFrame({"a": [1]}), str(other / "x.parquet"))
    assert p == str(other / "x.parquet")
    assert os.path.exists(p)


def test_cached_embeds_parquet_metadata(monkeypatch, tmp_path):
    import pyarrow.parquet as pq

    from nussif_data.cache import cached

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    cached("some/key", lambda: pd.DataFrame({"a": [1]}), metadata={"nd_vendor": "massive"})
    path = tmp_path / "some" / "key.parquet"
    assert pq.read_schema(str(path)).metadata[b"nd_vendor"] == b"massive"


def test_cached_hit_does_not_retroactively_add_metadata(monkeypatch, tmp_path):
    import pyarrow.parquet as pq

    from nussif_data.cache import cached

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    cached("some/key", lambda: pd.DataFrame({"a": [1]}))  # written with no metadata
    # second call is a cache hit -- passing metadata now must not rewrite the file
    cached(
        "some/key",
        lambda: (_ for _ in ()).throw(AssertionError("should be a cache hit")),
        metadata={"nd_vendor": "massive"},
    )
    path = tmp_path / "some" / "key.parquet"
    meta = pq.read_schema(str(path)).metadata or {}
    assert b"nd_vendor" not in meta


def test_write_frame_embeds_parquet_metadata(tmp_path):
    import pyarrow.parquet as pq

    from nussif_data._util import write_frame

    p = write_frame(
        pd.DataFrame({"a": [1]}),
        str(tmp_path / "x.parquet"),
        metadata={"nd_vendor": "massive", "nd_dataset": "option_chain"},
    )
    meta = pq.read_schema(p).metadata
    assert meta[b"nd_vendor"] == b"massive"
    assert meta[b"nd_dataset"] == b"option_chain"
    # data itself is unaffected by attaching metadata
    pd.testing.assert_frame_equal(pd.read_parquet(p), pd.DataFrame({"a": [1]}))


def test_write_frame_metadata_ignored_for_non_parquet(tmp_path):
    from nussif_data._util import write_frame

    # must not raise -- metadata is simply inapplicable to csv, not an error
    p = write_frame(pd.DataFrame({"a": [1]}), str(tmp_path / "x.csv"), metadata={"k": "v"})
    assert os.path.exists(p)


def test_out_dir_defaults_to_cwd(monkeypatch, tmp_path):
    from nussif_data.cache import out_dir

    monkeypatch.delenv("NUSSIF_DATA_OUT", raising=False)
    monkeypatch.chdir(tmp_path)
    assert os.path.realpath(out_dir()) == os.path.realpath(str(tmp_path))


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
    assert set(nd.catalog()["option_chain"]["providers"]) == {
        "alphavantage",
        "databento",
        "massive",
    }
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


def test_databento_get_quotes_chunks_above_symbol_cap(monkeypatch, tmp_path):
    from nussif_data.connectors.databento import DatabentoConnector

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))  # _get_quotes saves the raw response
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
    conn._get_quotes("SPY", [f"O{i}" for i in range(4500)], "2024-06-03", spec)
    assert calls == [2000, 2000, 500]  # split into <=cap batches


def test_databento_early_closes_match_nyse_2013_2025():
    from nussif_data.connectors.databento import _nyse_early_close

    nyse = {  # NYSE's published 1pm closes
        "2013-07-03", "2013-11-29", "2013-12-24", "2014-07-03", "2014-11-28", "2014-12-24",
        "2015-11-27", "2015-12-24", "2016-11-25", "2017-07-03", "2017-11-24", "2018-07-03",
        "2018-11-23", "2018-12-24", "2019-07-03", "2019-11-29", "2019-12-24", "2020-11-27",
        "2020-12-24", "2021-11-26", "2022-11-25", "2023-07-03", "2023-11-24", "2024-07-03",
        "2024-11-29", "2024-12-24", "2025-07-03", "2025-11-28", "2025-12-24",
    }  # fmt: skip
    days = pd.bdate_range("2013-01-01", "2025-12-31").strftime("%Y-%m-%d")
    assert {d for d in days if _nyse_early_close(d)} == nyse


def test_databento_quotes_snapshot_at_1pm_on_half_days(monkeypatch, tmp_path):
    from nussif_data.connectors.databento import DatabentoConnector

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    starts = []

    class _FakeTS:
        def get_range(self, **kw):
            starts.append(kw["start"])

            class _D:
                def to_df(self):
                    return pd.DataFrame({"instrument_id": [], "bid_px_00": [], "ask_px_00": []})

            return _D()

    conn = DatabentoConnector.__new__(DatabentoConnector)
    conn._cli = type("C", (), {"timeseries": _FakeTS()})()
    spec = {"close_tz": "America/New_York", "close_time": "16:00", "max_quote_symbols": 2000}
    conn._get_quotes("SPY", ["O1"], "2024-11-29", spec)  # day after Thanksgiving
    conn._get_quotes("SPY", ["O1"], "2024-11-27", spec)  # ordinary day
    assert starts[0].startswith("2024-11-29T17:57")  # 13:00 EST - 3min = 17:57Z
    assert starts[1].startswith("2024-11-27T20:57")  # 16:00 EST - 3min = 20:57Z


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


def test_databento_option_chain_normalizes_timestamp_date(monkeypatch, tmp_path):
    """Same bug as `open_interest`'s, found later in the same session: a bare
    `str(pd.Timestamp(...))` is "2020-02-07 00:00:00", and Databento's `start`/
    `end` reject the embedded time-of-day. Every existing caller happened to
    pass a clean date string (never a raw Timestamp), so this was live in
    `option_chain` for a long time without tripping -- caught only when a new
    caller (`_vrp.load_chain`) passed a `pd.Timestamp` straight through."""
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    from nussif_data.connectors.databento import DatabentoConnector

    conn = DatabentoConnector.__new__(DatabentoConnector)
    conn.cfg = {"datasets": {"option_chain": {}}}
    seen = {}

    def fake_one(sym, date_str, spot, mny, ndte, mdte):
        seen["date_str"] = date_str
        return pd.DataFrame(
            {
                "symbol": ["SPY"],
                "date": [pd.Timestamp(date_str)],
                "expiration": [pd.Timestamp("2020-03-20")],
                "strike": [330.0],
                "right": ["P"],
                "bid": [1.30],
                "ask": [1.35],
            }
        )

    conn._one = fake_one
    conn.option_chain("SPY", date=pd.Timestamp("2020-02-07"))
    assert seen["date_str"] == "2020-02-07"  # no embedded " 00:00:00"


def test_databento_registered_open_interest():
    assert callable(nd.databento.open_interest)


def test_databento_open_interest_normalizes_timestamp_date(monkeypatch, tmp_path):
    """A bare `str(pd.Timestamp(...))` is "2022-04-07 00:00:00" -- Databento's
    `start`/`end` reject the embedded time-of-day. Regression: passing a
    `pd.Timestamp` (not a plain date string) used to reach `get_range` with
    that unstripped string and fail with a 400 from the real API."""
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))  # never touch the real cache
    from nussif_data.connectors.databento import DatabentoConnector

    conn = DatabentoConnector.__new__(DatabentoConnector)
    conn.cfg = {}
    seen = {}

    def fake_one_oi(sym, date_str):
        seen["date_str"] = date_str
        return pd.DataFrame(
            {
                "symbol": ["SPY"],
                "date": [pd.Timestamp(date_str)],
                "expiration": [pd.Timestamp("2022-04-08")],
                "strike": [330.0],
                "right": ["P"],
                "open_interest": [1207.0],
            }
        )

    conn._one_oi = fake_one_oi
    conn.open_interest("SPY", date=pd.Timestamp("2022-04-07"))
    assert seen["date_str"] == "2022-04-07"  # no embedded " 00:00:00"


def test_databento_parse_osi():
    from nussif_data.connectors.databento import _parse_osi

    p = _parse_osi("SPY   220408P00330000")
    assert p == {
        "root": "SPY",
        "expiration": pd.Timestamp("2022-04-08"),
        "right": "P",
        "strike": 330.0,
    }
    assert _parse_osi("not-osi") is None  # wrong length -> defensive None, not a crash


def test_databento_assemble_oi_to_canonical():
    from nussif_data.connectors.databento import DatabentoConnector
    from nussif_data.core.schema import OPEN_INTEREST

    t0 = pd.Timestamp("2022-04-07T11:30:00Z")
    raw = pd.DataFrame(
        {
            "symbol": ["SPY   220408P00330000", "SPY   220408C00450000", "QQQ   220408C00300000"],
            "quantity": [1207, 532, 999],  # QQQ row should be dropped -- wrong root for this pull
            "instrument_id": [1, 2, 3],
            "ts_event": [t0, t0, t0],
        }
    )
    out = DatabentoConnector._assemble_oi("SPY", "2022-04-07", raw)
    OPEN_INTEREST.validate(out, where="test")
    assert len(out) == 2
    assert set(out["right"]) == {"C", "P"}
    assert out.loc[out.right == "P", "open_interest"].iloc[0] == 1207.0
    assert out.loc[out.right == "P", "strike"].iloc[0] == 330.0
    assert (out["symbol"] == "SPY").all()


def test_databento_assemble_oi_dedupes_repeated_prints():
    """The raw feed repeats the same (instrument_id, ts_event) print several
    times (a multicast artifact) -- must collapse to one row per contract, not
    sum/duplicate it."""
    from nussif_data.connectors.databento import DatabentoConnector

    raw = pd.DataFrame(
        {
            "symbol": ["SPY   220408P00330000"] * 4,
            "quantity": [1207, 1207, 1207, 1207],
            "instrument_id": [1, 1, 1, 1],
            "ts_event": [pd.Timestamp("2022-04-07T11:30:00Z")] * 4,
        }
    )
    out = DatabentoConnector._assemble_oi("SPY", "2022-04-07", raw)
    assert len(out) == 1
    assert out["open_interest"].iloc[0] == 1207.0


def test_databento_get_oi_filters_to_open_interest_stat_type():
    """`statistics` carries every stat type on a real pull -- only
    `stat_type == OPEN_INTEREST` (9) rows are real open interest; anything
    else's `quantity` is unrelated (seen in practice as an int32 sentinel,
    2147483647, once misread as an OI value of 2.1 billion contracts)."""
    from nussif_data.connectors.databento import _STAT_TYPE_OPEN_INTEREST, DatabentoConnector

    class _FakeData:
        def to_df(self):
            return pd.DataFrame(
                {
                    "stat_type": [_STAT_TYPE_OPEN_INTEREST, 5, 9],  # 5 = some other stat
                    "quantity": [1207, 2147483647, 42],
                    "symbol": ["SPY   220408P00330000"] * 3,
                    "instrument_id": [1, 2, 3],
                    "ts_event": [pd.Timestamp("2022-04-07T11:30:00Z")] * 3,
                }
            )

    class _FakeTS:
        def get_range(self, **kw):
            return _FakeData()

    class _FakeClient:
        timeseries = _FakeTS()

    conn = DatabentoConnector.__new__(DatabentoConnector)
    conn._cli = _FakeClient()
    out = conn._get_oi("SPY", "2022-04-07")
    assert (out["stat_type"] == _STAT_TYPE_OPEN_INTEREST).all()
    assert len(out) == 2
    assert 2147483647 not in out["quantity"].to_numpy()


def test_massive_intraday_parse_and_rth_filter(monkeypatch, tmp_path):
    import nussif_data as nd
    from nussif_data.connectors.massive import bars as massive_bars

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

    raw = massive_bars.fetch_intraday(nd.massive.http, nd.massive.cfg, "spy", 5, "minute")
    assert str(raw["timestamp"].dt.tz) == ny
    assert set(raw["ticker"]) == {"SPY"}
    assert {"timestamp", "ticker", "open", "close", "vwap", "trades"} <= set(raw.columns)
    assert len(raw) == 2  # de-duped across the repeated 6-month windows

    monkeypatch.setattr(massive_bars, "fetch_intraday", lambda *a, **k: raw)
    df = nd.massive.bars_intraday("SPY", start="2020-01-01", end="2020-12-31")  # rth=True default
    assert len(df) == 1 and df["timestamp"].iloc[0].hour == 10  # pre-market bar dropped
    df_all = nd.massive.bars_intraday("SPY", start="2020-01-01", end="2020-12-31", rth=False)
    assert len(df_all) == 2


# --- massive connector: option_chain (offline) -----------------------------
def test_massive_option_chain_registered():
    assert "option_chain" in [d.name for d in nd.massive.datasets()]
    assert callable(nd.massive.option_chain)
    assert "massive" in nd.catalog()["option_chain"]["providers"]


def test_massive_fetch_symbol_rejects_option_chain():
    with pytest.raises(NotImplementedError):
        nd.massive._fetch_symbol("option_chain", "SPY")


def test_massive_close_window_utc_handles_dst():
    from nussif_data.connectors.massive.options import _close_window_utc

    start_jan, end_jan = _close_window_utc(
        pd.Timestamp("2024-01-15"), "America/New_York", "16:00", 3
    )
    start_jun, end_jun = _close_window_utc(
        pd.Timestamp("2024-06-15"), "America/New_York", "16:00", 3
    )
    # 16:00 ET is 21:00 UTC in winter (EST) and 20:00 UTC in summer (EDT)
    assert start_jan.endswith("20:57:00+00:00")
    assert start_jun.endswith("19:57:00+00:00")
    assert end_jan.endswith("21:01:00+00:00")
    assert end_jun.endswith("20:01:00+00:00")


def test_massive_get_contracts_moneyness_none_drops_filter(monkeypatch):
    from nussif_data.connectors.massive.options import OptionChainFetcher

    conn = OptionChainFetcher.__new__(OptionChainFetcher)
    conn.cfg = {
        "endpoints": {"option_contracts": {"endpoint": "/v3/reference/options/contracts"}},
        "composites": {"option_chain": {}},
    }
    conn.http = type("H", (), {})()

    seen = {}

    def _fake_get_json(path, params=None):
        seen["path"] = path
        seen["params"] = params
        return {"results": [{"ticker": "O:SPY240621C00540000"}], "next_url": None}

    conn.http.get_json = _fake_get_json
    df = conn._get_contracts("SPY", "2024-06-03", 527.0, None, None, None)
    assert len(df) == 1
    assert "strike_price.gte" not in seen["params"]
    assert "expiration_date.gte" not in seen["params"]
    assert "expired" not in seen["params"]  # deliberately omitted -- see _get_contracts docstring


def test_massive_get_contracts_default_band_filters(monkeypatch):
    from nussif_data.connectors.massive.options import OptionChainFetcher

    conn = OptionChainFetcher.__new__(OptionChainFetcher)
    conn.cfg = {
        "endpoints": {"option_contracts": {"endpoint": "/v3/reference/options/contracts"}},
        "composites": {"option_chain": {}},
    }
    conn.http = type("H", (), {})()

    seen = {}

    def _fake_get_json(path, params=None):
        seen["params"] = params
        return {"results": [], "next_url": None}

    conn.http.get_json = _fake_get_json
    conn._get_contracts("SPY", "2024-06-03", 500.0, 0.25, 15, 60)
    assert seen["params"]["strike_price.gte"] == pytest.approx(375.0)
    assert seen["params"]["strike_price.lte"] == pytest.approx(625.0)
    assert seen["params"]["expiration_date.gte"] == "2024-06-18"
    assert seen["params"]["expiration_date.lte"] == "2024-08-02"


def test_massive_assemble_builds_canonical_shape():
    from nussif_data.connectors.massive.options import OptionChainFetcher

    contracts_df = pd.DataFrame(
        {
            "ticker": ["O:SPY240621C00540000", "O:SPY240621P00540000"],
            "underlying_ticker": ["SPY", "SPY"],
            "contract_type": ["call", "put"],
            "strike_price": [540.0, 540.0],
            "expiration_date": ["2024-06-21", "2024-06-21"],
        }
    )
    quotes_df = pd.DataFrame(
        {
            "ticker": ["O:SPY240621C00540000", "O:SPY240621P00540000"],
            "bid_price": [1.2, 0.9],
            "ask_price": [1.3, 1.0],
            "bid_size": [10, 5],
            "ask_size": [12, 6],
            "date": ["2024-06-03", "2024-06-03"],
            "lookback_min_used": [3, 60],
            "sip_timestamp": [1717444857505864448, 1717444857505864448],
        }
    )
    out = OptionChainFetcher._assemble(contracts_df, quotes_df)
    assert set(out.columns) == {
        "symbol",
        "date",
        "expiration",
        "strike",
        "right",
        "bid",
        "ask",
        "ticker",
        "bid_size",
        "ask_size",
        "lookback_min_used",
        "timestamp",
    }
    assert set(out["symbol"]) == {"SPY"}
    assert list(out["strike"]) == [540.0, 540.0]
    assert list(out["bid"]) == [1.2, 0.9]  # renamed from bid_price
    assert set(out["right"]) == {"C", "P"}  # derived from contract_type
    assert pd.api.types.is_datetime64_any_dtype(out["timestamp"])
    assert str(out["timestamp"].dt.tz) == "America/New_York"


def test_massive_assemble_empty_quotes_is_empty():
    from nussif_data.connectors.massive.options import OptionChainFetcher

    out = OptionChainFetcher._assemble(pd.DataFrame({"ticker": []}), pd.DataFrame())
    assert out.empty


def test_massive_option_chain_end_to_end(monkeypatch, tmp_path):
    import nussif_data as nd

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    monkeypatch.setenv("MASSIVE_API_KEY", "dummy")

    contracts_canned = {
        "results": [
            {
                "ticker": "O:SPY240621C00540000",
                "underlying_ticker": "SPY",
                "contract_type": "call",
                "strike_price": 540.0,
                "expiration_date": "2024-06-21",
            }
        ],
        "next_url": None,
    }
    monkeypatch.setattr(nd.massive.http, "get_json", lambda *a, **k: contracts_canned)
    monkeypatch.setattr(nd.massive.option_chain, "_spot_on", lambda sym, date_str: 538.5)

    quotes_df = pd.DataFrame(
        [
            {
                "ticker": "O:SPY240621C00540000",
                "bid_price": 1.2,
                "ask_price": 1.3,
                "bid_size": 10,
                "ask_size": 12,
                "date": "2024-06-03",
                "lookback_min_used": 3,
                "sip_timestamp": 1717444857505864448,
            }
        ]
    )
    monkeypatch.setattr(
        nd.massive.option_chain,
        "_get_quotes_concurrent",
        lambda tickers, date_str, api_key, max_workers: quotes_df,
    )

    out = nd.massive.option_chain("SPY", date="2024-06-03").fetch()
    assert len(out) == 1
    assert out["symbol"].iloc[0] == "SPY"
    assert out["right"].iloc[0] == "C"
    assert out["bid"].iloc[0] == 1.2
    assert "contract_type" not in out.columns
    assert "bid_price" not in out.columns


def test_massive_option_chain_fetch_out_writes_file(monkeypatch, tmp_path):
    import nussif_data as nd

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    monkeypatch.setenv("MASSIVE_API_KEY", "dummy")

    contracts_canned = {
        "results": [
            {
                "ticker": "O:SPY240621C00540000",
                "underlying_ticker": "SPY",
                "contract_type": "call",
                "strike_price": 540.0,
                "expiration_date": "2024-06-21",
            }
        ],
        "next_url": None,
    }
    monkeypatch.setattr(nd.massive.http, "get_json", lambda *a, **k: contracts_canned)
    monkeypatch.setattr(nd.massive.option_chain, "_spot_on", lambda sym, date_str: 538.5)
    quotes_df = pd.DataFrame(
        [
            {
                "ticker": "O:SPY240621C00540000",
                "bid_price": 1.2,
                "ask_price": 1.3,
                "bid_size": 10,
                "ask_size": 12,
                "date": "2024-06-03",
                "lookback_min_used": 3,
                "sip_timestamp": 1717444857505864448,
            }
        ]
    )
    monkeypatch.setattr(
        nd.massive.option_chain,
        "_get_quotes_concurrent",
        lambda tickers, date_str, api_key, max_workers: quotes_df,
    )

    out_path = tmp_path / "spy_chain.parquet"
    out = nd.massive.option_chain("SPY", date="2024-06-03").fetch(out=str(out_path))
    assert out_path.exists()
    reloaded = pd.read_parquet(out_path)
    assert len(reloaded) == len(out)
    assert reloaded["right"].iloc[0] == "C"


def test_massive_option_chain_fetch_raw_rejects_out(monkeypatch):
    monkeypatch.setattr(
        nd.massive.http, "get_json", lambda *a, **k: (_ for _ in ()).throw(AssertionError())
    )
    req = nd.massive.option_chain("SPY", date="2024-06-03", raw=True)
    with pytest.raises(ValueError):
        req.fetch(out="whatever.parquet")


def test_massive_option_chain_needs_symbol():
    with pytest.raises(ValueError):
        nd.massive.option_chain(date="2024-06-03")


def test_massive_option_chain_rejects_date_and_range_together():
    with pytest.raises(ValueError):
        nd.massive.option_chain("SPY", date="2024-06-03", start="2024-06-03", end="2024-06-05")


def test_massive_option_chain_rejects_neither_date_nor_range():
    with pytest.raises(ValueError):
        nd.massive.option_chain("SPY")


def test_massive_option_chain_rejects_spot_with_range():
    with pytest.raises(ValueError):
        nd.massive.option_chain("SPY", start="2024-06-03", end="2024-06-05", spot=500)


def test_massive_option_chain_returns_request_not_dataframe():
    from nussif_data.connectors.massive.options import OptionChainRequest

    req = nd.massive.option_chain("SPY", date="2024-06-03")
    assert isinstance(req, OptionChainRequest)
    assert not isinstance(req, pd.DataFrame)
    assert callable(req.fetch) and callable(req.estimate) and callable(req.download)


def _fake_request(symbol="SPY", date_strs=("2024-06-03", "2024-06-04", "2024-06-05"), raw=False):
    from nussif_data.connectors.massive.options import OptionChainFetcher, OptionChainRequest

    fetcher = OptionChainFetcher.__new__(OptionChainFetcher)
    fetcher.cfg = nd.massive.cfg
    fetcher.http = nd.massive.http
    return OptionChainRequest(fetcher, [symbol], list(date_strs), None, 0.25, 15, 60, 80, raw)


def test_massive_option_chain_fetch_embeds_metadata_without_out(monkeypatch, tmp_path):
    import pyarrow.parquet as pq

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    req = _fake_request(date_strs=("2024-06-03",))
    monkeypatch.setattr(
        req._fetcher,
        "_one_chain",
        lambda *a, **k: pd.DataFrame({"ticker": ["O:SPY1"], "bid_price": [1.0]}),
    )
    req.fetch()  # no out= at all
    path = tmp_path / "massive" / "option_chain" / "SPY" / "2024-06-03" / "m0.25_dte15-60.parquet"
    meta = pq.read_schema(str(path)).metadata
    assert meta[b"nd_vendor"] == b"massive"
    assert meta[b"nd_symbol"] == b"SPY"
    assert meta[b"nd_date"] == b"2024-06-03"
    assert b"nd_added_columns" in meta


def test_massive_option_chain_raw_and_default_share_one_cache_file(monkeypatch, tmp_path):
    """raw=True and raw=False produce identical content now -- they must
    share one cache key, not write the same data twice under two paths."""
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    calls = []

    def _one_chain(sym, date_str, *a, **k):
        calls.append(date_str)
        return pd.DataFrame({"ticker": ["O:SPY1"], "bid_price": [1.0]})

    req_default = _fake_request(date_strs=("2024-06-03",), raw=False)
    monkeypatch.setattr(req_default._fetcher, "_one_chain", _one_chain)
    req_default.fetch()

    req_raw = _fake_request(date_strs=("2024-06-03",), raw=True)
    monkeypatch.setattr(req_raw._fetcher, "_one_chain", _one_chain)
    req_raw.fetch()  # must hit the same cache file -- no second call

    assert calls == ["2024-06-03"]  # only fetched once, raw=True reused the cache
    assert req_default._cache_key("SPY", "2024-06-03") == req_raw._cache_key("SPY", "2024-06-03")


def test_massive_option_chain_download_returns_summary_not_data(monkeypatch, tmp_path):
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    req = _fake_request(date_strs=("2024-06-03",))
    monkeypatch.setattr(req._fetcher, "_one_chain", lambda *a, **k: pd.DataFrame({"x": [1]}))
    summary = req.download()
    assert not isinstance(summary, pd.DataFrame)
    assert summary == {"succeeded": [{"symbol": "SPY", "date": "2024-06-03"}], "failed": []}


def test_massive_option_chain_download_skips_and_continues_on_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    req = _fake_request(date_strs=("2024-06-03", "2024-06-04", "2024-06-05"))

    def _one_chain(sym, date_str, *a, **k):
        if date_str == "2024-06-04":
            raise RateLimited("massive: 429 after retries")
        return pd.DataFrame({"x": [1]})

    monkeypatch.setattr(req._fetcher, "_one_chain", _one_chain)
    summary = req.download()  # must not raise, despite the middle day failing
    assert [d["date"] for d in summary["succeeded"]] == ["2024-06-03", "2024-06-05"]
    assert len(summary["failed"]) == 1
    assert summary["failed"][0]["date"] == "2024-06-04"
    assert "429" in summary["failed"][0]["error"]


def test_massive_option_chain_download_resumes_only_failed_days(monkeypatch, tmp_path):
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    req = _fake_request(date_strs=("2024-06-03", "2024-06-04"))
    calls = []

    def _flaky_once(sym, date_str, *a, **k):
        calls.append(date_str)
        if date_str == "2024-06-04":
            raise RateLimited("massive: 429 after retries")
        return pd.DataFrame({"x": [1]})

    monkeypatch.setattr(req._fetcher, "_one_chain", _flaky_once)
    first = req.download()
    assert len(first["failed"]) == 1
    assert calls == ["2024-06-03", "2024-06-04"]

    # second run: 06-03 already cached (must not be re-fetched), retry only 06-04
    calls.clear()
    monkeypatch.setattr(
        req._fetcher,
        "_one_chain",
        lambda sym, date_str, *a, **k: calls.append(date_str) or pd.DataFrame({"x": [1]}),
    )
    second = req.download()
    assert calls == ["2024-06-04"]  # only the real fetch -- 06-03 served from disk, no call
    assert len(second["failed"]) == 0
    # both now have valid data available -- 06-03 was already cached, 06-04 just succeeded
    assert [d["date"] for d in second["succeeded"]] == ["2024-06-03", "2024-06-04"]


def test_massive_option_chain_estimate_skips_cached_day(monkeypatch, tmp_path):
    from nussif_data.cache import cached

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))

    req = nd.massive.option_chain("SPY", date="2024-06-03")
    # simulate an already-fetched day at the exact cache key .fetch()/.estimate() share
    cached(req._cache_key("SPY", "2024-06-03"), lambda: pd.DataFrame({"x": [1]}))

    def _boom(*a, **k):
        raise AssertionError("estimate() should not probe an already-cached day")

    monkeypatch.setattr(nd.massive.http, "get_json", _boom)
    est = req.estimate()
    assert est["days_cached"] == 1
    assert est["days_to_fetch"] == 0
    assert est["est_seconds"] == 0.0


def test_massive_option_chain_estimate_projects_from_contract_count(monkeypatch, tmp_path):
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    monkeypatch.setattr(nd.massive.option_chain, "_spot_on", lambda sym, date_str: 500.0)
    contracts_canned = {"results": [{"ticker": f"O:SPY{i}"} for i in range(200)], "next_url": None}
    monkeypatch.setattr(nd.massive.http, "get_json", lambda *a, **k: contracts_canned)

    req = nd.massive.option_chain("SPY", date="2024-06-03", max_workers=80)
    est = req.estimate()

    spec = nd.massive.cfg["composites"]["option_chain"]
    rate = spec.get("contracts_per_worker_second", 1.0)
    overhead = spec.get("contracts_fetch_overhead_s", 1.5)
    expected = overhead + 200 / (80 * rate)

    assert est["days_to_fetch"] == 1
    assert est["per_day"][0]["contracts"] == 200
    assert est["per_day"][0]["est_seconds"] == round(expected, 1)
    assert est["est_seconds"] == round(expected, 1)


# --- massive connector: exchanges (offline) ---------------------------------
def test_massive_exchanges_registered():
    assert "exchanges" in [d.name for d in nd.massive.datasets()]
    assert callable(nd.massive.exchanges)


def test_massive_exchanges_fetch_and_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))

    canned = {
        "results": [
            {"id": 302, "name": "Chicago Board Options Exchange", "mic": "XCBO"},
            {"id": 322, "name": "Cboe C2 Options Exchange", "mic": "C2OX"},
        ]
    }
    seen = {}

    def _fake_get_json(path, params=None):
        seen["path"] = path
        seen["params"] = params
        return canned

    monkeypatch.setattr(nd.massive.http, "get_json", _fake_get_json)
    df = nd.massive.exchanges("options")
    assert seen["params"]["asset_class"] == "options"
    assert len(df) == 2
    assert df.loc[df["id"] == 322, "name"].iloc[0] == "Cboe C2 Options Exchange"

    # second call hits the cache -- get_json must not fire again
    monkeypatch.setattr(
        nd.massive.http,
        "get_json",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should be cached")),
    )
    df2 = nd.massive.exchanges("options")
    assert len(df2) == 2


def test_massive_exchanges_needs_asset_class():
    with pytest.raises(TypeError):
        nd.massive.exchanges()


def test_massive_exchanges_empty_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    monkeypatch.setattr(nd.massive.http, "get_json", lambda *a, **k: {"results": []})
    with pytest.raises(UpstreamError):
        nd.massive.exchanges("options")


# --- massive connector: trades (offline) ------------------------------------
def test_massive_trades_registered():
    assert "trades" in [d.name for d in nd.massive.datasets()]
    assert callable(nd.massive.trades)


def test_massive_trades_needs_ticker():
    with pytest.raises(ValueError):
        nd.massive.trades(date="2024-06-03")


def test_massive_trades_fetch_paginates_and_derives_timestamp(monkeypatch, tmp_path):
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))

    page1 = {
        "results": [
            {"price": 1.2, "size": 3, "sip_timestamp": 1717412400000000000, "exchange": 322},
        ],
        "next_url": "https://api.massive.com/v3/trades/O:SPY240621C00540000?cursor=abc",
    }
    page2 = {
        "results": [
            {"price": 1.3, "size": 1, "sip_timestamp": 1717412401000000000, "exchange": 301},
        ],
        "next_url": None,
    }
    calls = []

    def _fake_get_json(path, params=None):
        calls.append(path)
        return page1 if len(calls) == 1 else page2

    monkeypatch.setattr(nd.massive.http, "get_json", _fake_get_json)
    df = nd.massive.trades("O:SPY240621C00540000", date="2024-06-03")
    assert len(calls) == 2  # paginated via next_url
    assert len(df) == 2
    assert (df["ticker"] == "O:SPY240621C00540000").all()
    assert str(df["timestamp"].dt.tz) == "America/New_York"
    assert {"price", "size", "exchange"} <= set(df.columns)  # vendor fields kept


def test_massive_trades_empty_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    monkeypatch.setattr(nd.massive.http, "get_json", lambda *a, **k: {"results": []})
    with pytest.raises(UpstreamError):
        nd.massive.trades("O:SPY240621C00540000", date="2024-06-03")


# --- cache_summary / cache_files (repl.py's `cache` command) --------------
def test_cache_summary_groups_by_vendor_dataset_and_strips_extension(monkeypatch, tmp_path):
    from nussif_data.cache import cache_summary

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    os.makedirs(tmp_path / "massive" / "option_chain" / "QQQ" / "2024-06-03", exist_ok=True)
    (
        tmp_path / "massive" / "option_chain" / "QQQ" / "2024-06-03" / "m0.25_dte15-60.parquet"
    ).write_bytes(b"x")
    (tmp_path / "cboe").mkdir()
    (tmp_path / "cboe" / "GVZ.parquet").write_bytes(b"xx")

    rows = {r["prefix"]: r for r in cache_summary()}
    assert rows["massive/option_chain"]["files"] == 1
    assert rows["massive/option_chain"]["size_bytes"] == 1
    # a 2-segment key (no distinct dataset level) must not leak the
    # ".parquet" extension into the group label
    assert "cboe/GVZ" in rows
    assert rows["cboe/GVZ"]["size_bytes"] == 2


def test_cache_files_filters_by_prefix(monkeypatch, tmp_path):
    from nussif_data.cache import cache_files

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    os.makedirs(tmp_path / "massive" / "option_chain" / "QQQ", exist_ok=True)
    os.makedirs(tmp_path / "massive" / "option_chain" / "SPY", exist_ok=True)
    (tmp_path / "massive" / "option_chain" / "QQQ" / "d.parquet").write_bytes(b"x")
    (tmp_path / "massive" / "option_chain" / "SPY" / "d.parquet").write_bytes(b"x")

    rows = cache_files("massive/option_chain/QQQ")
    assert len(rows) == 1
    assert rows[0]["key"] == "massive/option_chain/QQQ/d"


# --- option_chain .download()'s on_progress callback -----------------------
def test_massive_option_chain_download_reports_progress(monkeypatch, tmp_path):
    req = _fake_request(date_strs=("2024-06-03", "2024-06-04"))
    monkeypatch.setattr(
        req._fetcher,
        "_one_chain",
        lambda *a, **k: pd.DataFrame({"ticker": ["O:X"], "bid_price": [1.0]}),
    )
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    calls = []
    req.download(on_progress=lambda *a: calls.append(a))
    assert calls == [
        (1, 2, "SPY", "2024-06-03", True),
        (2, 2, "SPY", "2024-06-04", True),
    ]


def test_massive_option_chain_download_progress_reports_failure(monkeypatch, tmp_path):
    req = _fake_request(date_strs=("2024-06-03",))

    def _boom(*a, **k):
        raise UpstreamError("nope")

    monkeypatch.setattr(req._fetcher, "_one_chain", _boom)
    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    calls = []
    req.download(on_progress=lambda *a: calls.append(a))
    assert calls == [(1, 1, "SPY", "2024-06-03", False)]


# --- shared option_chain CLI/REPL dispatch (_option_chain_cli.py) ---------
def test_option_chain_cli_omitted_band_args_use_catalog_default(monkeypatch):
    from nussif_data._option_chain_cli import add_option_chain_args

    p = _argparse_parser()
    add_option_chain_args(p)
    a = p.parse_args(["SPY", "--date", "2024-06-03"])

    from nussif_data._option_chain_cli import build_request

    seen = {}
    monkeypatch.setattr(nd.massive, "option_chain", lambda *syms, **kw: seen.update(kw))
    build_request(a)
    assert "moneyness" not in seen
    assert "min_dte" not in seen
    assert "max_dte" not in seen


def test_option_chain_cli_explicit_none_drops_the_filter(monkeypatch):
    from nussif_data._option_chain_cli import add_option_chain_args, build_request

    p = _argparse_parser()
    add_option_chain_args(p)
    a = p.parse_args(["SPY", "--date", "2024-06-03", "--moneyness", "none", "--min-dte", "none"])

    seen = {}
    monkeypatch.setattr(nd.massive, "option_chain", lambda *syms, **kw: seen.update(kw))
    build_request(a)
    assert seen["moneyness"] is None
    assert seen["min_dte"] is None
    assert "max_dte" not in seen  # not passed at all -> catalog default


def _argparse_parser():
    import argparse

    return argparse.ArgumentParser()


# --- interactive shell (repl.py) -------------------------------------------
def test_repl_cache_command_renders_without_crashing(monkeypatch, tmp_path, capsys):
    from nussif_data.repl import NussifDataShell

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    os.makedirs(tmp_path / "massive" / "option_chain" / "QQQ", exist_ok=True)
    (tmp_path / "massive" / "option_chain" / "QQQ" / "d.parquet").write_bytes(b"x")

    shell = NussifDataShell()
    shell.onecmd("cache")
    shell.onecmd("cache massive/option_chain/QQQ")
    out = capsys.readouterr().out
    assert "massive/option_chain" in out


def test_repl_unknown_command_does_not_raise():
    from nussif_data.repl import NussifDataShell

    shell = NussifDataShell()
    shell.onecmd("this_is_not_a_command")  # must not raise / exit the process


def test_repl_bad_arguments_does_not_raise():
    from nussif_data.repl import NussifDataShell

    shell = NussifDataShell()
    # missing required positional -- argparse's default behavior is
    # sys.exit(); the shell must recover instead of dying.
    shell.onecmd("fetch massive")


def test_repl_option_chain_download_runs_in_background_job(monkeypatch, tmp_path):
    from nussif_data.repl import NussifDataShell

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))
    monkeypatch.setattr(
        nd.massive.option_chain,
        "_one_chain",
        lambda *a, **k: pd.DataFrame({"ticker": ["O:X"], "bid_price": [1.0]}),
    )
    shell = NussifDataShell()
    shell.onecmd("option_chain QQQ --date 2024-06-03 --mode download")
    assert len(shell._jobs) == 1
    job = shell._jobs[1]
    # give the background thread a moment to finish (tiny, fully-mocked fetch)
    import time as _time

    for _ in range(50):
        if job.finished_at is not None:
            break
        _time.sleep(0.05)
    assert job.finished_at is not None
    assert job.succeeded == 1
    assert job.failed == []


# ---------------------------------------------------------------- backfill
def _fake_bars(days, symbols=("SPY",)):
    def bars(*syms, start=None, end=None, field=None, **kw):
        df = pd.DataFrame({"date": pd.DatetimeIndex(days)})
        for i, s in enumerate(syms):
            df[s] = 100.0 + i
        return df.loc[(df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))]

    return bars


def test_trading_days_from_reference_bars_and_weekday(monkeypatch):
    days = pd.to_datetime(["2024-07-01", "2024-07-02", "2024-07-03", "2024-07-05", "2024-07-10"])
    monkeypatch.setattr(nd.massive, "bars", _fake_bars(days))
    got = nd.trading_days("2024-07-01", "2024-07-31")
    assert list(got) == list(days)  # July 4th absent: the calendar is the market's own
    wed = nd.trading_days("2024-07-01", "2024-07-31", weekday="wed")
    assert list(wed.strftime("%Y-%m-%d")) == ["2024-07-03", "2024-07-10"]
    with pytest.raises(ValueError, match="weekday"):
        nd.trading_days("2024-07-01", "2024-07-31", weekday="SAT")


def test_backfill_pulls_every_symbol_day_with_spot_and_band(monkeypatch):
    days = pd.to_datetime(["2024-07-01", "2024-07-02"])
    monkeypatch.setattr(nd.massive, "bars", _fake_bars(days))
    calls = []

    def chain(sym, *, date, spot=None, **band):
        calls.append((sym, date, spot, band))
        if (sym, date) == ("QQQ", "2024-07-02"):
            raise RuntimeError("no contracts in band")
        return pd.DataFrame({"x": [1, 2, 3]})

    monkeypatch.setattr(nd.databento, "option_chain", chain)
    report = nd.backfill(
        "option_chain", ["spy", "qqq"], days, moneyness=0.25, min_dte=15, progress=False
    )
    assert len(calls) == 4
    assert {c[2] for c in calls if c[0] == "SPY"} == {100.0}  # spot = that symbol's close
    assert all(c[3] == {"moneyness": 0.25, "min_dte": 15} for c in calls)
    failed = report[report["status"] != "ok"]
    assert list(zip(failed["symbol"], failed["date"].dt.strftime("%Y-%m-%d"), strict=True)) == [
        ("QQQ", "2024-07-02")
    ]
    assert report.loc[report["status"] == "ok", "rows"].eq(3).all()


def test_backfill_rejects_band_for_open_interest_and_unknown_dataset():
    with pytest.raises(ValueError, match="no band"):
        nd.backfill("open_interest", "SPY", ["2024-07-01"], min_dte=15)
    with pytest.raises(ValueError, match="dataset"):
        nd.backfill("bars", "SPY", ["2024-07-01"])


def test_cli_backfill(monkeypatch, capsys):
    from nussif_data.cli import main

    days = pd.to_datetime(["2024-07-01", "2024-07-02"])
    monkeypatch.setattr(nd.massive, "bars", _fake_bars(days))
    monkeypatch.setattr(
        nd.databento, "open_interest", lambda sym, *, date: pd.DataFrame({"x": [1]})
    )
    assert (
        main(["backfill", "open_interest", "SPY", "--start", "2024-07-01", "--end", "2024-07-31"])
        == 0
    )
    assert "2 pulls, 0 failed" in capsys.readouterr().out


def test_databento_refuses_dates_before_history_start_without_a_fetch(monkeypatch, tmp_path):
    # a date before OPRA.PILLAR's history can't have data; asking the vendor anyway was
    # a paid round-trip per call, repeated forever (failed fetches aren't cached)
    import nussif_data as nd
    from nussif_data.connectors.databento import DatabentoConnector

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))

    def no_network(*a, **k):
        raise AssertionError("fetched a date outside the dataset's history")

    monkeypatch.setattr(DatabentoConnector, "_client", no_network)
    with pytest.raises(nd.OutsideHistory, match="before 2013-04-01"):
        nd.databento.option_chain("SPY", date="2012-12-31")
    with pytest.raises(nd.OutsideHistory):
        nd.databento.open_interest("SPY", date="2010-06-01")
    assert isinstance(nd.OutsideHistory("x"), nd.NussifDataError)


def test_databento_cache_only_never_fetches(monkeypatch, tmp_path):
    # research that must not spend on vendor calls asks for cache_only=True:
    # a hit reads the cache, a miss raises NotCached -- never a paid fetch
    import nussif_data as nd
    from nussif_data.cache import cached
    from nussif_data.connectors.databento import DatabentoConnector

    monkeypatch.setenv("NUSSIF_DATA_CACHE", str(tmp_path))

    def no_network(*a, **k):
        raise AssertionError("fetched with cache_only=True")

    monkeypatch.setattr(DatabentoConnector, "_client", no_network)
    monkeypatch.setattr(DatabentoConnector, "_one", no_network)
    monkeypatch.setattr(DatabentoConnector, "_one_oi", no_network)
    with pytest.raises(nd.NotCached, match="TLT/2020-01-08"):
        nd.databento.option_chain(
            "TLT", date="2020-01-08", moneyness=0.25, min_dte=15, max_dte=60, cache_only=True
        )
    with pytest.raises(nd.NotCached):
        nd.databento.open_interest("TLT", date="2020-01-08", cache_only=True)
    with pytest.raises(ValueError, match="contradict"):
        nd.databento.option_chain("TLT", date="2020-01-08", cache_only=True, refresh=True)
    frame = pd.DataFrame(
        {
            "symbol": ["TLT"],
            "date": [pd.Timestamp("2020-01-08")],
            "expiration": [pd.Timestamp("2020-02-21")],
            "strike": [130.0],
            "right": ["P"],
            "bid": [1.0],
            "ask": [1.1],
            "bid_size": pd.array([10], dtype="uint32"),
            "ask_size": pd.array([10], dtype="uint32"),
        }
    )
    cached("databento/option_chain/TLT/2020-01-08/m0.25_d15-60", lambda: frame)
    got = nd.databento.option_chain(
        "TLT", date="2020-01-08", moneyness=0.25, min_dte=15, max_dte=60, cache_only=True
    )
    assert len(got) == 1
    assert isinstance(nd.NotCached("x"), nd.NussifDataError)
