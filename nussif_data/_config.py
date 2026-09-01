"""API-key resolution for vendors that need one (Massive, Alpha Vantage).

Order: environment variable  ->  ~/.config/nussif-data/keys.env  ->  set_key().
"""

from __future__ import annotations

import os

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".config", "nussif-data")
KEYS_FILE = os.path.join(CONFIG_DIR, "keys.env")

# vendor -> env var name (default is <VENDOR>_API_KEY)
_ENV = {"massive": "MASSIVE_API_KEY", "alphavantage": "ALPHAVANTAGE_API_KEY"}
_MEM: dict[str, str] = {}


def _env_name(vendor: str) -> str:
    return _ENV.get(vendor, f"{vendor.upper()}_API_KEY")


def set_key(vendor: str, value: str, persist: bool = True) -> None:
    """Set a vendor API key for this session; if persist, also write it to
    ~/.config/nussif-data/keys.env (chmod 600)."""
    _MEM[vendor] = value
    if not persist:
        return
    os.makedirs(CONFIG_DIR, exist_ok=True)
    name = _env_name(vendor)
    lines = []
    if os.path.exists(KEYS_FILE):
        with open(KEYS_FILE) as fh:
            lines = [ln for ln in fh if not ln.strip().startswith(name + "=")]
    lines.append(f"{name}={value}\n")
    with open(KEYS_FILE, "w") as fh:
        fh.writelines(lines)
    os.chmod(KEYS_FILE, 0o600)


def get_key(vendor: str) -> str:
    name = _env_name(vendor)
    if vendor in _MEM:
        return _MEM[vendor]
    if os.environ.get(name):
        return os.environ[name].strip()
    if os.path.exists(KEYS_FILE):
        with open(KEYS_FILE) as fh:
            for ln in fh:
                k, _, v = ln.strip().partition("=")
                if k in (name, "API_KEY"):
                    return v.strip().strip('"').strip("'")
    raise RuntimeError(
        f"no API key for {vendor!r}. Set ${name}, add it to {KEYS_FILE}, "
        f"or call nussif_data.set_key({vendor!r}, '...')."
    )
