"""Interactive `nussif-data` shell -- `nussif-data` with no arguments drops
into this instead of the one-shot argparse CLI (cli.py), for browsing the
catalog/cache and running fetches/downloads across a session instead of one
process per call.

Built on stdlib `cmd.Cmd` (readline history/completion for free) rather than
a new TUI framework for *navigation* -- `rich` is used only for *rendering*
(tables), matching this repo's minimal-dependency habit: reach for the
smallest thing that does the job, not the fanciest.

A `option_chain ... --mode download` here runs in a background (non-daemon)
thread so the prompt stays usable while it works -- `jobs` polls its
progress via the callback `OptionChainRequest.download()` now accepts.
Non-daemon is deliberate: exiting the shell with jobs still running doesn't
kill them, it just means the process stays alive until they finish -- the
same "runs for hours unattended" contract `.download()` already documents,
just not silently killed by leaving the shell.
"""

from __future__ import annotations

import argparse
import cmd
import itertools
import shlex
import threading
import time
from dataclasses import dataclass, field

from rich.console import Console
from rich.table import Table

import nussif_data as nd

from ._option_chain_cli import add_option_chain_args
from ._option_chain_cli import run as oc_run
from .cache import cache_dir, cache_files, cache_summary, clear_cache

console = Console()

_VENDORS = {"cboe": nd.cboe, "fred": nd.fred, "massive": nd.massive}


# --------------------------------------------------------------------------- rendering
def _human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def _human_age(ts: float) -> str:
    if not ts:
        return "-"
    s = time.time() - ts
    if s < 60:
        return f"{s:.0f}s ago"
    if s < 3600:
        return f"{s / 60:.0f}m ago"
    if s < 86400:
        return f"{s / 3600:.1f}h ago"
    return f"{s / 86400:.1f}d ago"


def _render_df(df, max_rows: int = 20) -> None:
    if df is None or len(df) == 0:
        console.print("[dim]empty result[/dim]")
        return
    t = Table()
    for col in df.columns:
        t.add_column(str(col))
    for _, row in df.head(max_rows).iterrows():
        t.add_row(*(str(v) for v in row.tolist()))
    console.print(t)
    if len(df) > max_rows:
        console.print(f"[dim]... {len(df) - max_rows} more row(s) ({len(df)} total)[/dim]")


def _render_estimate(est: dict) -> None:
    t = Table(title="option_chain estimate")
    t.add_column("field")
    t.add_column("value")
    for k in (
        "days_total",
        "days_cached",
        "days_to_fetch",
        "est_seconds",
        "est_human",
        "confidence",
    ):
        t.add_row(k, str(est.get(k)))
    console.print(t)
    per_day = est.get("per_day") or []
    if per_day:
        shown = per_day[:10]
        t2 = Table(
            title=f"{len(per_day)} day(s) to fetch" + (" (first 10)" if len(per_day) > 10 else "")
        )
        for col in shown[0]:
            t2.add_column(col)
        for row in shown:
            t2.add_row(*(str(v) for v in row.values()))
        console.print(t2)


# --------------------------------------------------------------------------- background download jobs
@dataclass
class _Job:
    id: int
    symbols: list
    total: int
    done: int = 0
    succeeded: int = 0
    failed: list = field(default_factory=list)
    started_at: float = 0.0
    finished_at: float | None = None
    error: str | None = None


class NussifDataShell(cmd.Cmd):
    prompt = "nd> "
    intro = None

    def __init__(self) -> None:
        super().__init__()
        self._jobs: dict[int, _Job] = {}
        self._job_ids = itertools.count(1)

    # -- helpers --
    def emptyline(self) -> bool:
        return False  # don't repeat the last command on a bare Enter

    def default(self, line: str) -> None:
        cmd_name = line.split()[0] if line.split() else line
        console.print(f"[red]unknown command: {cmd_name!r}[/red] (try 'help')")

    def _parse(self, prog: str, line: str, configure) -> argparse.Namespace | None:
        """shlex-split `line`, feed it to a fresh ArgumentParser `configure`
        builds -- returns None (already reported) on any parse failure
        instead of letting argparse's default sys.exit() kill the shell."""
        try:
            args = shlex.split(line)
        except ValueError as e:
            console.print(f"[red]{e}[/red]")
            return None
        p = argparse.ArgumentParser(prog=prog, add_help=False, exit_on_error=False)
        configure(p)
        try:
            return p.parse_args(args)
        except (SystemExit, argparse.ArgumentError, ValueError) as e:
            console.print(f"[red]bad arguments: {e}[/red] (try 'help {prog}')")
            return None

    # -- introspection --
    def do_catalog(self, line: str) -> None:
        """catalog -- dataset -> connector / providers / needs_key / description."""
        t = Table(title="nd.catalog()")
        for col in ("dataset", "connector", "providers", "needs_key", "description"):
            t.add_column(col)
        for name, row in sorted(nd.catalog().items()):
            t.add_row(
                name,
                row["connector"],
                ",".join(row["providers"]),
                str(row["needs_key"]),
                row["description"],
            )
        console.print(t)

    def do_connectors(self, line: str) -> None:
        """connectors -- connector -> datasets it serves."""
        t = Table(title="nd.connectors()")
        t.add_column("connector")
        t.add_column("datasets")
        for name, row in sorted(nd.connectors().items()):
            t.add_row(name, ", ".join(row["datasets"]))
        console.print(t)

    # -- cache --
    def do_cache(self, line: str) -> None:
        """cache [PREFIX] -- no arg: summary by <vendor>/<dataset>. With a
        prefix (e.g. 'massive/option_chain/QQQ'): lists matching files."""
        prefix = line.strip()
        if not prefix:
            rows = cache_summary()
            if not rows:
                console.print(f"[dim]cache is empty ({cache_dir()})[/dim]")
                return
            t = Table(title=f"cache: {cache_dir()}")
            for col in ("prefix", "files", "size", "newest"):
                t.add_column(col)
            for r in rows:
                t.add_row(
                    r["prefix"],
                    str(r["files"]),
                    _human_size(r["size_bytes"]),
                    _human_age(r["newest"]),
                )
            console.print(t)
            return
        rows = cache_files(prefix)
        if not rows:
            console.print(f"[dim]no cached files under '{prefix}'[/dim]")
            return
        shown = rows[:200]
        t = Table(
            title=f"{len(rows)} file(s) under '{prefix}'"
            + (" (first 200)" if len(rows) > 200 else "")
        )
        for col in ("key", "size", "modified"):
            t.add_column(col)
        for r in shown:
            t.add_row(r["key"], _human_size(r["size_bytes"]), _human_age(r["mtime"]))
        console.print(t)

    def do_clearcache(self, line: str) -> None:
        """clearcache PREFIX | ALL -- delete cached files under PREFIX (ALL
        clears everything). Always asks for confirmation first."""
        prefix = line.strip()
        if not prefix:
            console.print("[red]usage: clearcache PREFIX  (or: clearcache ALL)[/red]")
            return
        target = None if prefix == "ALL" else prefix
        preview = cache_files(target or "")
        if not preview:
            console.print(f"[dim]nothing cached under '{prefix}'[/dim]")
            return
        console.print(f"[yellow]about to delete {len(preview)} file(s) under '{prefix}'[/yellow]")
        if input("type 'yes' to confirm: ").strip().lower() != "yes":
            console.print("[dim]cancelled[/dim]")
            return
        n = clear_cache(target)
        console.print(f"deleted {n} file(s)")

    # -- generic vendor fetch (cboe / fred / massive bars) --
    def do_fetch(self, line: str) -> None:
        """fetch VENDOR SYMBOL [SYMBOL ...] [--start S] [--end E] [--field F]
        [--out PATH] [--raw] [--refresh]  -- VENDOR: cboe | fred | massive
        (that vendor's primary dataset)."""

        def configure(p):
            p.add_argument("vendor", choices=sorted(_VENDORS))
            p.add_argument("symbols", nargs="+")
            p.add_argument("--start")
            p.add_argument("--end")
            p.add_argument("--field")
            p.add_argument("--out")
            p.add_argument("--raw", action="store_true")
            p.add_argument("--refresh", action="store_true")

        a = self._parse("fetch", line, configure)
        if a is None:
            return
        kw = {"start": a.start, "end": a.end, "refresh": a.refresh, "raw": a.raw}
        if a.vendor == "massive" and a.field:
            kw["field"] = a.field
        if a.out and not a.raw:
            kw["out"] = a.out
        try:
            result = _VENDORS[a.vendor](*a.symbols, **kw)
        except Exception as e:
            console.print(f"[red]{type(e).__name__}: {e}[/red]")
            return
        if a.raw:
            for sym, df in result.items():
                console.print(f"[bold]{sym}[/bold]  ({len(df)} rows)")
                _render_df(df)
            return
        if a.out:
            console.print(f"wrote {a.out}  ({len(result)} rows x {result.shape[1]} cols)")
        else:
            _render_df(result)

    # -- option_chain (fetch / estimate / background download) --
    def do_option_chain(self, line: str) -> None:
        """option_chain SYMBOL... (--date D | --start S --end E)
        [--moneyness F|none] [--min-dte N|none] [--max-dte N|none]
        [--max-workers N] [--refresh] [--raw] [--mode fetch|estimate|download]
        [-o PATH] [--head N]  -- mode=download runs in the background, see 'jobs'."""
        a = self._parse("option_chain", line, add_option_chain_args)
        if a is None:
            return
        try:
            mode, result = oc_run(a)
        except Exception as e:
            console.print(f"[red]{type(e).__name__}: {e}[/red]")
            return

        if mode == "estimate":
            _render_estimate(result)
            return
        if mode == "download":
            self._start_download_job(a.symbols, result, refresh=a.refresh)
            return

        # mode == "fetch"
        if a.raw:
            for sym, df in result.items():
                console.print(f"[bold]{sym}[/bold]  ({len(df)} rows)")
                _render_df(df, max_rows=a.head or 20)
            return
        if a.out:
            console.print(f"wrote {a.out}  ({len(result)} rows x {result.shape[1]} cols)")
        else:
            _render_df(result, max_rows=a.head or 20)

    def _start_download_job(self, symbols, req, *, refresh: bool) -> None:
        total = len(req.date_strs) * len(req.syms)
        job = _Job(id=next(self._job_ids), symbols=symbols, total=total, started_at=time.time())
        self._jobs[job.id] = job

        def on_progress(done, total_, symbol, date_str, ok):
            job.done = done
            if ok:
                job.succeeded += 1
            else:
                job.failed.append({"symbol": symbol, "date": date_str})

        def run():
            try:
                req.download(refresh=refresh, on_progress=on_progress)
            except Exception as e:
                job.error = f"{type(e).__name__}: {e}"
            finally:
                job.finished_at = time.time()

        threading.Thread(target=run, name=f"nd-download-{job.id}", daemon=False).start()
        console.print(
            f"[green]started job {job.id}[/green]: {','.join(symbols)}, {total} day(s) queued "
            "-- 'jobs' to check progress"
        )

    def do_jobs(self, line: str) -> None:
        """jobs -- list background download jobs and their progress."""
        if not self._jobs:
            console.print("[dim]no jobs[/dim]")
            return
        t = Table()
        for col in ("id", "symbols", "progress", "succeeded", "failed", "status", "elapsed"):
            t.add_column(col)
        for job in self._jobs.values():
            status = "error" if job.error else ("done" if job.finished_at else "running")
            elapsed = (job.finished_at or time.time()) - job.started_at
            t.add_row(
                str(job.id),
                ",".join(job.symbols),
                f"{job.done}/{job.total}",
                str(job.succeeded),
                str(len(job.failed)),
                status,
                f"{elapsed:.0f}s",
            )
        console.print(t)

    def do_job(self, line: str) -> None:
        """job ID -- detail for one background job (failed days, error if any)."""
        try:
            jid = int(line.strip())
        except ValueError:
            console.print("[red]usage: job ID[/red]")
            return
        job = self._jobs.get(jid)
        if job is None:
            console.print(f"[red]no job {jid}[/red]")
            return
        console.print(
            f"job {job.id}: {job.done}/{job.total} done, {job.succeeded} succeeded, "
            f"{len(job.failed)} failed"
        )
        if job.error:
            console.print(f"[red]{job.error}[/red]")
        if job.failed:
            shown = job.failed[:50]
            t = Table(
                title=f"{len(job.failed)} failed" + (" (first 50)" if len(job.failed) > 50 else "")
            )
            t.add_column("symbol")
            t.add_column("date")
            for f in shown:
                t.add_row(f["symbol"], f["date"])
            console.print(t)

    # -- exit --
    def do_exit(self, line: str) -> bool:
        """exit -- leave the shell (background jobs, if any, keep running
        until they finish -- the process stays alive for them)."""
        running = [j for j in self._jobs.values() if j.finished_at is None]
        if running:
            console.print(
                f"[yellow]{len(running)} job(s) still running -- this process stays alive "
                "until they finish[/yellow]"
            )
        return True

    do_quit = do_exit

    def do_EOF(self, line: str) -> bool:
        console.print()
        return self.do_exit(line)


def run_repl() -> int:
    console.print(
        f"[bold]nussif-data[/bold] {nd.__version__} -- interactive shell. "
        "Type 'help' for commands, 'exit' to leave."
    )
    NussifDataShell().cmdloop()
    return 0
