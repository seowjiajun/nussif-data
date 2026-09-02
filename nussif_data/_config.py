"""API-key resolution for vendors that need one (Massive, Alpha Vantage, Databento).

Order:  environment variable  ->  ~/.config/nussif-data/keys.env

There is deliberately **no Python setter**: a key must never be passable as a
function argument or literal, so it can't end up in a committed notebook or
script. Provide it out of band --

    export MASSIVE_API_KEY=...                       # shell / CI secret
    # or, to persist without re-exporting:
    mkdir -p ~/.config/nussif-data
    printf 'MASSIVE_API_KEY=%s\n' "$KEY" >> ~/.config/nussif-data/keys.env
    chmod 600 ~/.config/nussif-data/keys.env

`keys.env` lives under $HOME, outside any repo.
"""

from __future__ import annotations

import os

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".config", "nussif-data")
KEYS_FILE = os.path.join(CONFIG_DIR, "keys.env")

# vendor -> env var name (default is <VENDOR>_API_KEY)
_ENV = {"massive": "MASSIVE_API_KEY", "alphavantage": "ALPHAVANTAGE_API_KEY"}


def _env_name(vendor: str) -> str:
    return _ENV.get(vendor, f"{vendor.upper()}_API_KEY")


def get_key(vendor: str) -> str:
    name = _env_name(vendor)
    if os.environ.get(name):
        return os.environ[name].strip()
    if os.path.exists(KEYS_FILE):
        with open(KEYS_FILE) as fh:
            for ln in fh:
                k, _, v = ln.strip().partition("=")
                if k in (name, "API_KEY"):
                    return v.strip().strip('"').strip("'")
    raise RuntimeError(
        f"no API key for {vendor!r}. Set ${name} in your environment, or add a "
        f"line '{name}=...' to {KEYS_FILE} (chmod 600)."
    )
