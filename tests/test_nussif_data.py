"""Offline tests: parsing, catalog, date slicing, key errors. No network."""
import os
import tempfile

import pandas as pd
import pytest

import nussif_data as nd
from nussif_data import _util
from nussif_data.cboe import _parse_history
from nussif_data.fred import _fetch_one as _fred_fetch  # noqa: F401  (import shape check)


def test_catalog_shape():
    c = nd.catalog()
    assert "VIX" in c["cboe"]
    assert c["fred"]["baa10y"] == "BAA10Y"
    assert "SPY" in c["bars"]


def test_cboe_history_parser_handles_title_line():
    raw = (
        b"Cboe VIX Index History\n"
        b"DATE,OPEN,HIGH,LOW,CLOSE\n"
        b"01/02/2020,13.00,14.00,12.50,13.78\n"
        b"01/03/2020,13.80,13.90,13.10,13.20\n"
    )
    df = _parse_history("VIX", raw)
    assert list(df.columns) == ["date", "VIX"]
    assert df.loc[df.date == pd.Timestamp("2020-01-02"), "VIX"].iloc[0] == 13.78


def test_cboe_history_parser_single_value_column():
    raw = b"DATE,VVIX\n03/06/2006,71.73\n03/07/2006,72.10\n"
    df = _parse_history("VVIX", raw)
    assert list(df.columns) == ["date", "VVIX"]
    assert len(df) == 2


def test_slice_dates_accepts_loose_input():
    df = pd.DataFrame({"date": pd.to_datetime(["2019-06-01", "2020-06-01", "2021-06-01"]), "x": [1, 2, 3]})
    assert list(_util.slice_dates(df, start="2020")["x"]) == [2, 3]
    assert list(_util.slice_dates(df, end="2020-06-01")["x"]) == [1, 2]
    assert list(_util.slice_dates(df, start="2020", end="2020-12-31")["x"]) == [2]


def test_outer_merge_on_date():
    a = pd.DataFrame({"date": pd.to_datetime(["2020-01-01", "2020-01-02"]), "A": [1, 2]})
    b = pd.DataFrame({"date": pd.to_datetime(["2020-01-02", "2020-01-03"]), "B": [9, 8]})
    m = _util.outer_merge_on_date([a, b])
    assert list(m.columns) == ["date", "A", "B"]
    assert len(m) == 3


def test_missing_massive_key_is_clear(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    from nussif_data import _config
    monkeypatch.setattr(_config, "KEYS_FILE", os.path.join(tempfile.gettempdir(), "no_such_keys.env"))
    _config._MEM.clear()
    with pytest.raises(RuntimeError, match="no API key for 'massive'"):
        _config.get_key("massive")
