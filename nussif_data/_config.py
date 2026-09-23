"""API-key resolution for vendors that need one (Massive, Alpha Vantage, Databento).

Order:  environment variable  ->  ~/.config/nussif-data/keys.env  ->  interactive
prompt (TTY only). The prompt exists so nobody has to already know this
file's exact path to get going -- it offers to persist what you type there
itself, so it's a one-time ask, not a manual dotfile-editing step.

There is deliberately **no Python setter**: a key must never be passable as a
function argument or literal, so it can't end up in a committed notebook or
script -- same reason the prompt uses getpass (input hidden, never echoed to
the terminal or left in shell scrollback) rather than a plain input().

Non-interactive to begin with (no TTY, piped stdin, CI, a test run) -> never
prompts, same RuntimeError as always. To skip the prompt yourself:

    export MASSIVE_API_KEY=...                       # shell / CI secret
    # or, to persist without re-exporting:
    mkdir -p ~/.config/nussif-data
    printf 'MASSIVE_API_KEY=%s\n' "$KEY" >> ~/.config/nussif-data/keys.env
    chmod 600 ~/.config/nussif-data/keys.env

`keys.env` lives under $HOME, outside any repo.
"""

from __future__ import annotations

import getpass
import os
import sys

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".config", "nussif-data")
KEYS_FILE = os.path.join(CONFIG_DIR, "keys.env")

# vendor -> env var name (default is <VENDOR>_API_KEY)
_ENV = {"massive": "MASSIVE_API_KEY", "alphavantage": "ALPHAVANTAGE_API_KEY"}


def _env_name(vendor: str) -> str:
    return _ENV.get(vendor, f"{vendor.upper()}_API_KEY")


def _read_keys_file(name: str) -> str | None:
    if not os.path.exists(KEYS_FILE):
        return None
    with open(KEYS_FILE) as fh:
        for ln in fh:
            k, _, v = ln.strip().partition("=")
            if k in (name, "API_KEY"):
                return v.strip().strip('"').strip("'")
    return None


def _can_prompt() -> bool:
    # PYTEST_CURRENT_TEST: pytest sets this for the duration of every test --
    # checked unconditionally first so a local `pytest -s` run (or pytest
    # invoked in-process, e.g. from a notebook cell) can never block on this.
    if "PYTEST_CURRENT_TEST" in os.environ:
        return False
    # A real terminal has isatty() == True. A Jupyter kernel (notebook,
    # JupyterLab, VS Code notebooks, Colab, ...) does NOT -- sys.stdin there
    # is a socket-backed stream, not a tty -- but ipykernel monkey-patches
    # getpass.getpass/input to route through the kernel's own secure
    # input-request protocol (a masked box in the notebook UI), so it's just
    # as interactive. Verified live in a real running kernel: isatty() is
    # False, getpass.getpass/input are both bound to Kernel.getpass/
    # Kernel.raw_input regardless -- "ipykernel" in sys.modules is the
    # reliable signal for that case, since isatty() alone would otherwise
    # silently disable this everywhere notebooks are actually used.
    return sys.stdin.isatty() or "ipykernel" in sys.modules


def _prompt_and_maybe_persist(name: str) -> str | None:
    if not _can_prompt():
        return None
    try:
        key = getpass.getpass(f"no ${name} found -- paste it here (input hidden): ").strip()
        if not key:
            return None
        save = input(f"save to {KEYS_FILE} so this isn't asked again? [Y/n] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return None
    if save in ("", "y", "yes"):
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(KEYS_FILE, "a") as fh:
            fh.write(f"{name}={key}\n")
        os.chmod(KEYS_FILE, 0o600)
        print(f"saved to {KEYS_FILE} -- future runs won't ask for {name} again.")
    return key


def get_key(vendor: str) -> str:
    name = _env_name(vendor)
    if os.environ.get(name):
        return os.environ[name].strip()
    found = _read_keys_file(name)
    if found:
        return found
    prompted = _prompt_and_maybe_persist(name)
    if prompted:
        return prompted
    raise RuntimeError(
        f"no API key for {vendor!r}. Set ${name} in your environment, or add a "
        f"line '{name}=...' to {KEYS_FILE} (chmod 600)."
    )
