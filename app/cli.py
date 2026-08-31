from __future__ import annotations

import asyncio
import functools
import json as _json_mod
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from viva_api.common import StrEnumBase

if TYPE_CHECKING:
    from collections.abc import Callable

    from rich.console import Console

    from viva_api.common.storage.file_service_s3 import FileServiceS3

import httpx
import typer
from typer import Argument, Option

from app.app_data_service import READ_CAPABILITIES, E2EDataService, get_data_service
from app.cli_theme import display_json, get_console, print_banner, status_border, status_style
from app.tui import AtlantisTUI


def _format_server_detail(body: str) -> str:
    """Extract the 'detail' field from a JSON error body, or return the body as-is."""
    try:
        parsed = _json_mod.loads(body)
        if isinstance(parsed, dict) and "detail" in parsed:
            return str(parsed["detail"])
    except (ValueError, TypeError):
        pass
    return body.strip()


def _handle_cli_error(e: Exception, console: Console | None = None) -> None:
    """Render a user-friendly error panel for common exception types."""
    from rich.panel import Panel

    if console is None:
        console = get_console()

    if isinstance(e, httpx.ConnectError | ConnectionError | OSError) or _is_connection_error(e):
        console.print(
            Panel(
                "[memphis.error]Could not connect to the API server.[/]\n\n"
                "Check that the server is running and the --base-url is correct.\n"
                f"Detail: {e}",
                title="Connection Error",
                border_style="memphis.border.error",
            )
        )
    elif isinstance(e, httpx.HTTPStatusError):
        detail = _format_server_detail(e.response.text)
        code = e.response.status_code
        console.print(
            Panel(
                f"[memphis.error]Server returned {code}[/]\n\n{detail}",
                title="API Error",
                border_style="memphis.border.error",
            )
        )
    elif isinstance(e, httpx.HTTPError):
        msg = str(e)
        # Extract JSON detail from messages like "Server returned 400: {"detail":"..."}"
        json_start = msg.find("{")
        if json_start >= 0:
            prefix = msg[:json_start].strip().rstrip(":")
            detail = _format_server_detail(msg[json_start:])
            label = prefix if prefix else "HTTP Error"
        else:
            detail = msg
            label = "HTTP Error"
        console.print(
            Panel(
                f"[memphis.error]{label}[/]\n\n{detail}",
                title="API Error",
                border_style="memphis.border.error",
            )
        )
    elif isinstance(e, _json_mod.JSONDecodeError):
        console.print(
            Panel(
                f"[memphis.error]Invalid JSON input.[/]\n\n{e.msg}\n  at position {e.pos}",
                title="Input Error",
                border_style="memphis.border.error",
            )
        )
    elif isinstance(e, KeyboardInterrupt):
        console.print("\n[memphis.warning]Cancelled.[/]")
    else:
        console.print(
            Panel(
                f"[memphis.error]{type(e).__name__}[/]: {e}",
                title="Error",
                border_style="memphis.border.error",
            )
        )

    if os.environ.get("ATLANTIS_DEBUG"):
        console.print_exception(show_locals=True)


def _is_connection_error(e: Exception) -> bool:
    """Check if an exception is a connection-related error."""
    err_str = str(e).lower()
    return any(s in err_str for s in ("connect", "refused", "unreachable", "timed out", "no route"))


def cli_error_handler(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that catches exceptions and renders user-friendly error messages."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception as e:
            _handle_cli_error(e)
            raise typer.Exit(1) from None

    return wrapper


def _slice_by_id(items: list[Any], n: int | None) -> list[Any]:
    """Slice a list of model objects by database_id order.

    Args:
        items: List of pydantic models with a ``database_id`` attribute.
        n: If positive, return the first N (lowest IDs). If negative, return
           the last |N| (highest IDs). If None or 0, return all.
    """
    if not n or not items:
        return items
    has_id = hasattr(items[0], "database_id")
    if has_id:
        items = sorted(items, key=lambda x: x.database_id)
    if n > 0:
        return items[:n]
    return items[n:]


def sync_sources_to_s3(
    sources: list[Path],
    bucket: str,
    prefix: str = "sources",
    delete: bool = False,
    console: Console | None = None,
) -> list[tuple[str, str]]:
    """Sync local data-source directories to S3 via the AWS CLI.

    Returns a list of ``(basename, s3_uri)`` pairs. The first entry backs
    ``ECOLI_SOURCES``; subsequent ones are overlay manifests.
    """
    if not sources:
        return []
    if shutil.which("aws") is None:
        raise RuntimeError(
            "The AWS CLI (`aws`) was not found on PATH. Install it and authenticate before using --sources."
        )
    synced: list[tuple[str, str]] = []
    for src in sources:
        src_path = src.expanduser().resolve()
        if not src_path.exists():
            raise typer.BadParameter(f"--sources path does not exist: {src_path}")
        if not src_path.is_dir():
            raise typer.BadParameter(f"--sources path is not a directory: {src_path}")
        basename = src_path.name
        if not basename:
            raise typer.BadParameter(f"Cannot derive a basename from source path: {src_path}")
        s3_dst = f"s3://{bucket}/{prefix.strip('/')}/{basename}/"
        cmd = [
            "aws",
            "s3",
            "sync",
            str(src_path),
            s3_dst,
            "--exclude",
            ".venv/*",
            "--exclude",
            "*/__pycache__/*",
            "--exclude",
            ".git/*",
            "--exclude",
            "*.pyc",
        ]
        if delete:
            cmd.append("--delete")
        if console is not None:
            console.print(f"[memphis.info]Syncing[/]  {src_path}  →  {s3_dst}")
        subprocess.run(cmd, check=True)
        synced.append((basename, s3_dst))
    return synced


class CliType(StrEnumBase):
    SIMULATOR = "simulator"
    SIMULATION = "simulation"
    PARCA = "parca"
    ANALYSIS = "analysis"
    COMPOSE = "compose"
    DEMO = "demo"
    HELP = "help"
    TUI = "tui"
    GUI = "gui"
    TKAPP = "tkapp"


class ApiBaseUrl(StrEnumBase):
    RKE_PROD = "https://sms.cam.uchc.edu"
    RKE_DEV = "https://sms-dev.cam.uchc.edu"
    LOCAL_8888 = "http://localhost:8888"
    LOCAL_8000 = "http://localhost:8000"
    LOCAL_1111 = "http://localhost:1111"
    LOCAL_62505 = "http://localhost:62505"
    LOCAL_8080 = "http://localhost:8080"


API_BASE_URL = os.getenv("API_BASE_URL", ApiBaseUrl.LOCAL_8080)


cli = typer.Typer(name="atlantis", help="SMS API CLI for managing vEcoli simulations, simulators, parca, and analyses.")
simulator_cli = typer.Typer(help="Manage simulator (vEcoli) versions and builds.")
simulation_cli = typer.Typer(help="Run and inspect simulation workflows.")
parca_cli = typer.Typer(help="Inspect parca (parameter calculator) datasets and runs.")
analysis_cli = typer.Typer(help="Inspect analysis jobs and outputs.")
compose_cli = typer.Typer(help="Compose (process-bigraph) simulation commands.")
worker_cli = typer.Typer(help="Run and call env workers (a simulator image as a live process).")
demo_cli = typer.Typer(help="Demo and utility commands.")
tui_cli = typer.Typer(help="TUI's command line interface.")
gui_cli = typer.Typer(help="GUI's command line interface.")
tkapp_cli = typer.Typer(help="Tkinter desktop GUI.")

cli.add_typer(simulator_cli, name="simulator")
cli.add_typer(simulation_cli, name="simulation")
cli.add_typer(parca_cli, name="parca")
cli.add_typer(analysis_cli, name="analysis")
cli.add_typer(compose_cli, name="compose")
cli.add_typer(worker_cli, name="worker")
cli.add_typer(demo_cli, name="demo")
cli.add_typer(tui_cli)
cli.add_typer(gui_cli)
cli.add_typer(tkapp_cli)


# ---------------------------------------------------------------------------
# Env workers (plan §C relay)
#
# An env worker is a simulator's own prebuilt image run as a LIVE process you can
# ask questions of -- "what generators does this build have?", "build this
# composite" -- without submitting a simulation. viva-api creates the Job, holds
# its socket, and forwards JSON-RPC over HTTP, which is what makes this usable
# from a laptop: the worker dials back, and an SSM tunnel has no inbound path,
# so the workbench's own transport has no address to advertise from here.
#
# These replace the hand-rolled `curl` that was the only laptop client.
# ---------------------------------------------------------------------------


@worker_cli.command("start", help="Start an env worker for a simulator commit and wait for it to connect.")
def worker_start(
    commit: str = Argument(help="Simulator commit (the prebuilt image tag) to run."),
    workspace: str = Option(
        default="", help="Workspace path inside the worker container. Default: the image's own checkout."
    ),
    session_key: str = Option(default="", help="Owning session; makes the Job name unique per session."),
    accept_timeout: float = Option(
        default=300.0, help="Seconds to wait for the worker to dial back (pod schedule + image pull)."
    ),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)
    # The wait is the interesting part -- a cold image pull dominates it -- so
    # say what is being waited FOR rather than showing a bare spinner.
    with console.status(f"[memphis.spinner]Starting env worker for {commit} (pod schedule + image pull)..."):
        try:
            result = data_service.worker_start(
                commit=commit,
                workspace=workspace or None,
                session_key=session_key or None,
                accept_timeout=accept_timeout,
            )
        except Exception as e:
            console.print(_worker_error(e, "start"))
            raise typer.Exit(1) from e
    job = result.get("job_name", "")
    console.print(Panel("[memphis.success]CONNECTED[/]", title=f"Env worker — {commit}", border_style="green"))
    # highlight=False: a Job name is an IDENTIFIER the user copies, and Rich's
    # auto-highlighter colours the digits inside it as if they were a number —
    # which makes `env-worker-f78672f-cli0smok-c41a7961` render in three colours
    # and reads as though part of it were special. Same for the copyable
    # commands below.
    console.print(f"[memphis.label]Job:[/]   {job}", highlight=False)
    console.print(f"[memphis.label]Image:[/] {result.get('image', '')}", highlight=False)
    console.print(f"\n[dim italic]Call it:[/] atlantis worker call {job} list_generators", highlight=False)
    console.print(f"[dim italic]Stop it:[/] atlantis worker stop {job}", highlight=False)


@worker_cli.command("call", help="Call one method on a running env worker.")
def worker_call(
    job_name: str = Argument(help="Job name from 'atlantis worker start'."),
    method: str = Argument(help="Worker method, e.g. list_generators."),
    params: str = Option(default="", help='JSON object of method params, e.g. \'{"ref": "pkg.composites.cell"}\'.'),
    timeout: float = Option(default=300.0, help="Seconds to wait for this call's reply."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)

    parsed: dict[str, object] = {}
    if params:
        try:
            parsed = _json_mod.loads(params)
        except _json_mod.JSONDecodeError as e:
            # Say what was wrong with THEIR json, not just that json failed --
            # this is the flag most likely to be typed by hand.
            console.print(f"[memphis.error]--params is not valid JSON:[/] {e}")
            raise typer.Exit(1) from e
        if not isinstance(parsed, dict):
            console.print("[memphis.error]--params must be a JSON object[/] (the worker takes named params)")
            raise typer.Exit(1)

    with console.status(f"[memphis.spinner]{method}..."):
        try:
            result = data_service.worker_call(job_name, method=method, params=parsed, timeout=timeout)
        except Exception as e:
            console.print(_worker_error(e, "call"))
            raise typer.Exit(1) from e
    # A worker result may legitimately be null (a method that only acts). Print
    # something rather than handing display_json a None it will not accept.
    payload = result.get("result")
    if payload is None:
        console.print("[dim](no result)[/]")
    else:
        display_json(payload, console)


@worker_cli.command("stop", help="Stop an env worker and delete its Job.")
def worker_stop(
    job_name: str = Argument(help="Job name from 'atlantis worker start'."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    try:
        result = data_service.worker_stop(job_name)
    except Exception as e:
        console.print(_worker_error(e, "stop"))
        raise typer.Exit(1) from e
    was = result.get("was_connected")
    console.print(
        f"[memphis.success]Deleted[/] {job_name}" + ("" if was else " [dim](no live connection held)[/]"),
        highlight=False,
    )


@worker_cli.command("read", help="Read one of the worker's named capabilities.")
def worker_read(
    job_name: str = Argument(help="Job name from 'atlantis worker start'."),
    capability: str = Argument(help=f"One of: {', '.join(READ_CAPABILITIES)}."),
    package_path: str = Option(default="", help="For core-snapshot: the workspace package to import."),
    include: list[str] = Option(default=[], help="For reexports: a package to scan. Repeatable."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    """Prefer this to `worker call` for reads.

    The named endpoints turn the worker's in-band failures into status codes:
    `{"__unavailable__": true}` arrives as 501 rather than as a 200 whose body
    the caller has to know to inspect. `worker call` is the raw escape hatch and
    hands those back untouched.
    """
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    if capability not in READ_CAPABILITIES:
        console.print(f"[memphis.error]unknown capability[/] {capability}")
        console.print(f"[dim]expected one of: {', '.join(READ_CAPABILITIES)}[/]")
        raise typer.Exit(1)

    params: dict[str, object] = {}
    if package_path:
        params["package_path"] = package_path
    if include:
        params["include"] = list(include)
    if capability == "core-snapshot" and not package_path:
        # Required server-side; saying so here beats a 422 the user has to decode.
        console.print("[memphis.error]core-snapshot needs --package-path[/] (the worker imports <package>.core)")
        raise typer.Exit(1)

    with console.status(f"[memphis.spinner]{capability}..."):
        try:
            payload = data_service.worker_read(job_name, capability, params or None)
        except Exception as e:
            console.print(_worker_error(e, "read"))
            raise typer.Exit(1) from e
    if isinstance(payload, dict | list | str):
        display_json(payload, console)
    else:
        # A read whose whole answer is a scalar. Rare, but printing nothing
        # because the renderer only takes containers would be worse.
        console.print(payload)


# --------------------------------------------------------------------------- #
# The task tier (plan §E option (e) step 7)
#
# `worker call` is synchronous and stays that way -- an interactive method
# answers in seconds. `run_study` and friends run a study to completion, which no
# gateway will hold a request for, so they are submitted and polled instead.
# --------------------------------------------------------------------------- #

#: Methods that MUST go through the task tier. Mirrors the workbench's
#: `env_worker_routing.JOB_CLASS_METHODS`, including its correction that
#: `run_process` is NOT one despite the prefix -- it builds one class and runs a
#: single update(), which is a probe.
JOB_CLASS_METHODS = ("run_study", "run_study_analyses", "run_investigation_analysis")

_TERMINAL = ("completed", "failed", "cancelled")


def _identity_note(identity: str) -> str:
    """Say what the identity is FOR, without implying it is a login."""
    if identity:
        return f"[dim italic]as {identity} — attribution only, not authentication[/]"
    return "[dim italic]anonymous — set --as to be able to cancel this later[/]"


@worker_cli.command("submit", help="Submit a long-running method as a task, and optionally poll it.")
def worker_submit(
    job_name: str = Argument(help="Job name from 'atlantis worker start'."),
    method: str = Argument(help=f"Worker method, e.g. {JOB_CLASS_METHODS[0]}."),
    params: str = Option(default="", help='JSON object of method params, e.g. \'{"study_slug": "s1"}\'.'),
    poll: bool = Option(default=False, help="Wait for the task to settle, printing status as it changes."),
    interval: float = Option(default=10.0, help="Seconds between polls when --poll is set."),
    identity: str = Option("", "--as", help="Caller identity to record on the task."),
    identity_header: str = Option(default="", help="Header the server reads identity from."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(
        base_url=base_url, identity=identity or None, identity_header=identity_header or None
    )

    parsed = _worker_params(console, params)
    if method not in JOB_CLASS_METHODS:
        # Not refused: the tier accepts any method, and a caller may have a
        # reason. But an interactive method submitted as a task is almost always
        # a mistake for `worker call`, and silently doing it would leave them
        # polling something that had already finished.
        console.print(
            f"[memphis.warn]{method} is not a job-class method[/] "
            f"[dim](those are: {', '.join(JOB_CLASS_METHODS)}) — 'atlantis worker call' answers directly[/]"
        )

    try:
        task = data_service.worker_submit(job_name, method=method, params=parsed)
    except Exception as e:
        console.print(_worker_error(e, "submit"))
        raise typer.Exit(1) from e

    task_id = task.get("task_id")
    console.print(
        f"[memphis.success]Queued[/] task [memphis.value]{task_id}[/]  {method}  {_identity_note(identity)}",
        highlight=False,
    )
    # Do not claim an identity the server did not accept. The header is only read
    # where a deployment names one (IDENTITY_HEADER); where it does not,
    # the row is anonymous and the caller will NOT be able to cancel this task.
    # Printing "as you@example.com" and leaving them to discover that at cancel
    # time would be the CLI lying about what happened.
    if identity and not task.get("created_by"):
        console.print(
            "[memphis.warn]The server did not record that identity[/] — it has no "
            "IDENTITY_HEADER configured, so this task is anonymous and "
            "anyone may cancel it.",
        )
    if not poll:
        console.print(f"\n[dim italic]Watch it:[/]  atlantis worker task {task_id}", highlight=False)
        console.print(f"[dim italic]Cancel it:[/] atlantis worker cancel {task_id}", highlight=False)
        return
    _poll_task(console, data_service, int(task_id or 0), interval)


@worker_cli.command("task", help="Show one task's status, and its result or error once it settles.")
def worker_task(
    task_id: int = Argument(help="Task id from 'atlantis worker submit'."),
    poll: bool = Option(default=False, help="Wait for the task to settle."),
    interval: float = Option(default=10.0, help="Seconds between polls when --poll is set."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    if poll:
        _poll_task(console, data_service, task_id, interval)
        return
    try:
        task = data_service.worker_task(task_id)
    except Exception as e:
        console.print(_worker_error(e, "task"))
        raise typer.Exit(1) from e
    _print_task(console, task)


@worker_cli.command("tasks", help="Status of several tasks at once.")
def worker_tasks(
    task_ids: list[int] = Argument(help="Task ids, space separated."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    """Results are deliberately NOT included: a handful of run_study results is
    megabytes, and a poll loop would re-download all of it every few seconds. The
    listing says whether a result is waiting; `worker task <id>` fetches it."""
    from rich.table import Table

    console = get_console()
    data_service = get_data_service(base_url=base_url)
    try:
        rows = data_service.worker_tasks(list(task_ids))
    except Exception as e:
        console.print(_worker_error(e, "tasks"))
        raise typer.Exit(1) from e

    missing = sorted(set(task_ids) - {int(r.get("task_id", -1)) for r in rows})
    table = Table(title="Env worker tasks", border_style="magenta")
    for col in ("ID", "Method", "Status", "Result", "Owner"):
        table.add_column(col)
    for row in rows:
        status = str(row.get("status", ""))
        table.add_row(
            str(row.get("task_id", "")),
            str(row.get("method", "")),
            f"[{_status_style(status)}]{status}[/]",
            "yes" if row.get("has_result") else "—",
            str(row.get("created_by") or "[dim]anonymous[/]"),
        )
    console.print(table)
    if missing:
        # Say which, rather than letting a shorter table be the only clue.
        console.print(f"[dim]not found: {', '.join(str(m) for m in missing)}[/]")


@worker_cli.command("cancel", help="Cancel a task you started.")
def worker_cancel(
    task_id: int = Argument(help="Task id from 'atlantis worker submit'."),
    identity: str = Option("", "--as", help="Caller identity; must match whoever submitted the task."),
    identity_header: str = Option(default="", help="Header the server reads identity from."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    """The one authorization rule in the API. Everything else -- start, call,
    submit, poll -- is open to anonymous callers; only DESTROYING someone else's
    work asks who you are."""
    console = get_console()
    data_service = get_data_service(
        base_url=base_url, identity=identity or None, identity_header=identity_header or None
    )
    try:
        result = data_service.worker_cancel(task_id)
    except Exception as e:
        console.print(_worker_cancel_error(e, identity))
        raise typer.Exit(1) from e
    console.print(f"[memphis.success]Cancelled[/] task [memphis.value]{task_id}[/]", highlight=False)
    if result.get("status"):
        console.print(f"[memphis.label]Status:[/] {result['status']}", highlight=False)


def _worker_cancel_error(exc: Exception, identity: str) -> str:
    """401 and 403 mean different things here and the fix differs, so say which.

    A bare "403 Forbidden" invites the user to retry with a different flag; the
    useful thing is that the task HAS an owner and it is not them.
    """
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status == 401:
        return (
            "[memphis.error]cancel needs an identity[/]\n"
            "  Cancelling destroys someone's work, so it is the one operation that asks who you are.\n"
            "  Pass [memphis.value]--as you@example.com[/] (or set ATLANTIS_IDENTITY)."
        )
    if status == 403:
        detail = _worker_detail(exc)
        who = f" ({detail})" if detail else ""
        return (
            f"[memphis.error]that task belongs to someone else{who}[/]\n"
            + (f"  You are identified as [memphis.value]{identity}[/].\n" if identity else "")
            + "  You cannot cancel a task you did not start."
        )
    return _worker_error(exc, "cancel")


def _worker_error_detail(exc: Exception) -> object:
    """The server's `detail`, whatever shape it is. Never raises."""
    response = getattr(exc, "response", None)
    if response is None:
        return None
    try:
        return response.json().get("detail")
    except Exception:
        return None


def _worker_detail(exc: Exception) -> str:
    """The server's own message, when there is one."""
    response = getattr(exc, "response", None)
    if response is None:
        return ""
    try:
        detail = response.json().get("detail")
    except Exception:
        return ""
    return detail if isinstance(detail, str) else ""


def _status_style(status: str) -> str:
    return {
        "completed": "memphis.success",
        "failed": "memphis.error",
        "cancelled": "yellow",
        "running": "memphis.spinner",
    }.get(status, "dim")


def _print_task(console: Console, task: dict[str, Any]) -> None:
    from rich.panel import Panel

    status = str(task.get("status", ""))
    console.print(
        Panel(
            f"[{_status_style(status)}]{status.upper()}[/]",
            title=f"Task {task.get('task_id')} — {task.get('method')}",
            border_style="magenta",
        )
    )
    for label, key in (("Worker", "job_name"), ("Started", "started_at"), ("Ended", "ended_at")):
        if task.get(key):
            console.print(f"[memphis.label]{label}:[/] {task[key]}", highlight=False)
    console.print(f"[memphis.label]Owner:[/]  {task.get('created_by') or '[dim]anonymous[/]'}", highlight=False)
    # A FAILED task and a task whose RESULT carries errors are different things,
    # and the distinction is the whole point of the tier -- see the plan's
    # "What a caller can actually tell apart". Show both, separately.
    if task.get("error_message"):
        console.print(f"\n[memphis.error]The job failed:[/] {task['error_message']}", highlight=False)
    result = task.get("result")
    if result is not None:
        errors = result.get("errors") if isinstance(result, dict) else None
        if errors:
            console.print(f"\n[memphis.warn]The job completed, but {len(errors)} stage(s) failed:[/]")
            for entry in errors:
                stage = entry.get("stage", "?") if isinstance(entry, dict) else "?"
                message = entry.get("error", "") if isinstance(entry, dict) else str(entry)
                console.print(f"  [memphis.label]{stage}[/] {message}", highlight=False)
        # A conclusion card sitting under a partly-failed run is the one thing
        # here a scientist might act on, so say plainly that it is not about this
        # run. The harvest already demoted `overall`; without this the demotion
        # is only visible to someone reading the raw JSON.
        verdict = result.get("verdict") if isinstance(result, dict) else None
        incomplete = verdict.get("evidence_incomplete") if isinstance(verdict, dict) else None
        if incomplete:
            console.print(
                f"\n[memphis.warn]Verdict not graded[/] [dim]— was "
                f"'{incomplete.get('overall_before')}', but "
                f"{len(incomplete.get('failed_stages') or [])} stage(s) of this run did not "
                f"happen. The card on disk is unchanged.[/]"
            )
        console.print()
        display_json(result, console)


def _poll_task(console: Console, data_service: E2EDataService, task_id: int, interval: float) -> None:
    """Poll until terminal, printing each status CHANGE rather than every tick.

    A line per poll would bury the two moments that matter (queued -> running,
    running -> settled) in a wall of identical text.
    """
    import time as _time

    seen = ""
    while True:
        try:
            task = data_service.worker_task(task_id)
        except Exception as e:
            console.print(_worker_error(e, "task"))
            raise typer.Exit(1) from e
        status = str(task.get("status", ""))
        if status != seen:
            console.print(f"  [{_status_style(status)}]{status}[/]", highlight=False)
            seen = status
        if status in _TERMINAL:
            console.print()
            _print_task(console, task)
            return
        _time.sleep(interval)


def _worker_params(console: Console, params: str) -> dict[str, Any]:
    """Parse --params, saying what was wrong with THEIR json."""
    if not params:
        return {}
    try:
        parsed = _json_mod.loads(params)
    except _json_mod.JSONDecodeError as e:
        console.print(f"[memphis.error]--params is not valid JSON:[/] {e}")
        raise typer.Exit(1) from e
    if not isinstance(parsed, dict):
        console.print("[memphis.error]--params must be a JSON object[/] (the worker takes named params)")
        raise typer.Exit(1)
    return parsed


def _worker_error(exc: Exception, verb: str) -> str:
    """Turn the relay's status codes into something actionable.

    Each of these means a specific, different thing, and a bare stack trace
    hides which -- 503 in particular is a DEPLOYMENT answer ("the relay is not
    switched on here"), not a fault the caller can retry.
    """
    status = getattr(getattr(exc, "response", None), "status_code", None)
    # A tier refusal is also a 422, and the generic hint below ("check the method
    # name and params") actively contradicts it: the method was right and the
    # params were understood -- the work was simply too big for this tier. Detect
    # it by its own field rather than by status, and say what to do instead.
    if status == 422:
        detail = _worker_error_detail(exc)
        if isinstance(detail, dict) and detail.get("declared_simulations") is not None:
            return (
                f"[memphis.error]too big for an env worker[/] "
                f"[dim](declared {detail['declared_simulations']} simulations, budget "
                f"{detail.get('budget')})[/]\n  {detail.get('hint', '')}"
            )
    hints = {
        503: "the relay is not enabled on this deployment "
        "(ENV_WORKER_RELAY_ADVERTISE_HOST is unset on the api Deployment)",
        404: "no such worker — it was never started here, or viva-api restarted and dropped its socket",
        410: "the worker's connection is gone; start a new one",
        422: "the worker ran and refused: check the method name and params",
        409: "a Job of that name already exists or is still terminating",
        504: "the worker never dialled back — check the image tag and the Job's logs",
    }
    hint = hints.get(status or 0)
    head = f"[memphis.error]worker {verb} failed"
    head += f" (HTTP {status})[/]" if status else "[/]"
    detail = ""
    try:
        body = exc.response.json()  # type: ignore[attr-defined]
        detail = body.get("detail") or body.get("error") or ""
    except Exception:
        detail = str(exc)[:200]
    out = f"{head} {detail}"
    if hint:
        out += f"\n  [dim]{hint}[/]"
    if status is None:
        # No response at all: almost always the tunnel, on this path.
        out += "\n  [dim]no response — is the tunnel up? (sms-proxy.sh -s smsvpctest)[/]"
    return out


def main() -> None:
    # Allow "help" as a trailing word at any nesting level:
    # e.g. "atlantis simulation run help" → "atlantis simulation run --help"
    if len(sys.argv) > 1 and sys.argv[-1] == "help":
        sys.argv[-1] = "--help"

    try:
        cli()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        get_console().print("\n[memphis.warning]Cancelled.[/]")
        raise SystemExit(130) from None
    except Exception as e:
        _handle_cli_error(e)
        raise SystemExit(1) from None


# -- Info/Help --


def _show_group_help(group_name: str) -> None:
    """Print the banner then display help for *group_name* (a sub-typer)."""
    import click

    console = get_console()
    print_banner(console)

    cmd = typer.main.get_command(cli)
    with click.Context(cmd) as ctx:
        sub = cmd.get_command(ctx, group_name)  # type: ignore[attr-defined]
        if sub is None:
            print(f"Unknown command: {group_name}")
            raise typer.Exit(1)
        with click.Context(sub, parent=ctx) as sub_ctx:
            print(sub.get_help(sub_ctx))


@cli.command(name="help")
def display_help(app: CliType | None = typer.Argument(default=None)) -> None:
    """Show help for a specific subcommand, or the main CLI."""
    import click

    if app is not None:
        _show_group_help(app.value)
        return

    console = get_console()
    print_banner(console)
    cmd = typer.main.get_command(cli)
    with click.Context(cmd) as ctx:
        print(cmd.get_help(ctx))


# Register a "help" command on every sub-typer so that e.g.
# `atlantis simulation help` works identically to `atlantis help simulation`.
def _register_subgroup_help(sub_typer: typer.Typer, group_name: str) -> None:
    @sub_typer.command(name="help", help=f"Show help for {group_name} commands.")
    def _help() -> None:
        _show_group_help(group_name)


for _name, _sub in [
    ("simulator", simulator_cli),
    ("simulation", simulation_cli),
    ("parca", parca_cli),
    ("analysis", analysis_cli),
    ("compose", compose_cli),
    ("demo", demo_cli),
]:
    _register_subgroup_help(_sub, _name)
# tui_cli / gui_cli are single-command typers added without a name (flattened),
# so they don't need sub-group help — use `atlantis tui --help` instead.


# -- Top-level App launch commands --


@tui_cli.command(name="tui", help="Launch the interactive terminal UI.")
def launch_tui(
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    tui = AtlantisTUI(base_url=base_url)
    tui.run()


@gui_cli.command(name="gui", help="Launch the interactive graphical user interface.")
def launch_gui(
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
    mode: str = Option(
        default="run", help="Launch the interactive graphical user interface mode in either run or edit mode."
    ),
) -> None:
    try:
        proc = subprocess.Popen(["uv", "run", "marimo", mode, "app/gui.py", "--no-token"])
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        proc.wait(timeout=5)


# -- Simulator commands --


@simulator_cli.command("latest", help="Fetch, upload, and build the latest simulator version from the default repo.")
def simulator_latest(
    repo_url: str | None = Option(default=None, help="Git repo URL. Defaults to the configured default repo."),
    branch: str | None = Option(default=None, help="Git branch. Defaults to the configured default branch."),
    force: bool = Option(default=False, help="Force rebuild even if a completed build exists."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    import time

    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)

    # 1. Get latest commit
    with console.status("[memphis.spinner]Fetching latest commit..."):
        latest = data_service.submit_get_latest_simulator(repo_url=repo_url, branch=branch)
    commit_info = f"({latest.git_repo_url} @ {latest.git_branch})"
    console.print(f"[memphis.label]Commit:[/] {latest.git_commit_hash}  [memphis.dim]{commit_info}[/]")

    # 2. Upload (triggers build if new, or force rebuild)
    with console.status("[memphis.spinner]Uploading simulator..."):
        uploaded = data_service.submit_upload_simulator(simulator=latest, force=force)
    console.print(f"[memphis.label]Simulator ID:[/] {uploaded.database_id}")

    # 3. Poll build status with live feedback
    console.print("[memphis.info]Waiting for build...[/]")
    poll_interval = 15
    elapsed = 0
    status = "running"
    while status not in ("completed", "failed", "cancelled"):
        time.sleep(poll_interval)
        elapsed += poll_interval
        status = data_service.submit_get_simulator_build_status(simulator=uploaded)
        console.print(f"  [{elapsed}s] status: [{status_style(status)}]{status}[/]")

    console.print(
        Panel(
            f"[{status_style(status)}]{status.upper()}[/]",
            title=f"Build — simulator {uploaded.database_id}",
            border_style=status_border(status),
        )
    )
    display_json(uploaded.model_dump(), console)


@simulator_cli.command("list", help="List registered simulator versions.")
def simulator_list(
    n: int | None = Option(
        default=None, help="Number of entries to show. Positive = first N, negative = last N (by ID)."
    ),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    simulators = data_service.show_simulators()
    simulators = _slice_by_id(simulators, n)
    for sim in simulators:
        display_json(sim.model_dump(), console)


@simulator_cli.command("status", help="Get the container build status for a simulator by its database ID.")
def simulator_status(
    simulator_id: int = Argument(help="Simulator database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)
    hpcrun = data_service.submit_get_simulator_build_status_full(simulator_id=simulator_id)
    s = hpcrun.status or "unknown"
    console.print(
        Panel(
            f"[{status_style(s)}]{s.upper()}[/]",
            title=f"Build — simulator {simulator_id}",
            border_style=status_border(s),
        )
    )
    if hpcrun.error_message:
        console.print(f"[memphis.error]Error:[/] {hpcrun.error_message}")
    display_json(hpcrun.model_dump(), console)


# -- Simulation commands --


@simulation_cli.command("run", help="Submit a simulation workflow (parca -> simulation -> analysis).")
def simulation_run(
    experiment_id: str = Argument(help="Unique experiment identifier."),
    simulator_id: int = Argument(help="Database ID of the simulator to use."),
    config_filename: str = Option(
        default="api_simulation_default.json",
        help="Config filename in vEcoli/configs/ on HPC. The server validates accepted values.",
    ),
    generations: int = Option(default=1, help="Number of generations to run per lineage (seed)."),
    seeds: int = Option(default=3, help="Number of lineages (seeds)."),
    description: str | None = Option(default=None, help="Custom description for this simulation run."),
    run_parca: bool = Option(
        default=False,
        help="Run the parameter calculator before simulation. Increases overall runtime.",
    ),
    observables: str | None = Option(
        default=None,
        help="Comma-separated dot-path observables to record (e.g. 'bulk,listeners.mass.cell_mass'). "
        "If omitted, all outputs are emitted.",
    ),
    analysis_options: str | None = Option(
        default=None,
        help='JSON string of vEcoli analysis module config. E.g. \'{"single": {"ptools_rna": {"n_tp": 10}}}\'.'
        " Keys are analysis categories (single, multiseed, multigeneration, etc.);"
        " values map module names to params. If omitted, defaults depend on the simulator repo.",
    ),
    sources: list[Path] = Option(
        default_factory=list,
        help="Local data-source directories to sync to S3 before the workflow. "
        "Repeat for multiple: --sources ../ecoli-sources --sources ../ecoli-sources-vegas. "
        "Requires the AWS CLI on PATH with credentials configured.",
    ),
    sources_prefix: str = Option(
        default="sources",
        help="S3 key prefix under the configured bucket for --sources sync.",
    ),
    sources_delete: bool = Option(
        default=False,
        help="Pass --delete to `aws s3 sync` (removes S3 objects not present locally).",
    ),
    sources_repo: str | None = Option(
        default=None,
        help="GitHub repo URL for ecoli-sources data (e.g. https://github.com/vivarium-collective/ecoli-sources). "
        "The server downloads and syncs to S3 automatically — no local clone or AWS CLI needed.",
    ),
    sources_ref: str | None = Option(
        default=None,
        help="Git ref (branch/tag/commit) for --sources-repo. Defaults to 'main'.",
    ),
    tag: list[str] = Option(
        default_factory=list,
        help="Free-form tag to attach for later filtering (e.g. --tag cd1). Repeat for multiple. "
        "Tags can also be added later with 'atlantis simulation tag <id> <tag>'.",
    ),
    poll: bool = Option(default=False, help="Poll simulation status until completion."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    import json as _json
    import time

    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)
    observables_list = [o.strip() for o in observables.split(",") if o.strip()] if observables else None
    analysis_opts_parsed = _json.loads(analysis_options) if analysis_options else None

    # Sync --sources directories to S3 and derive env var values
    ecoli_sources_uri: str | None = None
    ecoli_sources_overlays: str | None = None
    if sources:
        from viva_api.config import get_settings

        settings = get_settings()
        if not settings.storage_s3_bucket:
            console.print("[memphis.error]--sources requires STORAGE_S3_BUCKET to be configured.[/]")
            raise typer.Exit(1)
        synced = sync_sources_to_s3(
            sources=sources,
            bucket=settings.storage_s3_bucket,
            prefix=sources_prefix,
            delete=sources_delete,
            console=console,
        )
        if synced:
            _, primary_uri = synced[0]
            ecoli_sources_uri = primary_uri.rstrip("/")
            overlay_uris = [f"{uri.rstrip('/')}/data/manifest.tsv" for _, uri in synced[1:]]
            if overlay_uris:
                ecoli_sources_overlays = ";".join(overlay_uris)
            console.print(
                f"\n[memphis.dim]ECOLI_SOURCES={ecoli_sources_uri}[/]"
                + (
                    f"\n[memphis.dim]ECOLI_SOURCES_OVERLAYS={ecoli_sources_overlays}[/]"
                    if ecoli_sources_overlays
                    else ""
                )
            )

    with console.status("[memphis.spinner]Submitting simulation..."):
        simulation = data_service.run_workflow(
            experiment_id=experiment_id,
            simulator_id=simulator_id,
            config_filename=config_filename,
            num_generations=generations,
            num_seeds=seeds,
            description=description or f"sim{simulator_id}-{experiment_id}; {generations} Generations; {seeds} Seeds",
            run_parameter_calculator=run_parca,
            observables=observables_list,
            analysis_options=analysis_opts_parsed,
            ecoli_sources_uri=ecoli_sources_uri,
            ecoli_sources_overlays=ecoli_sources_overlays,
            ecoli_sources_repo_url=sources_repo,
            ecoli_sources_ref=sources_ref,
            tags=list(tag) or None,
        )

    console.print(f"[memphis.success]Simulation submitted![/]  ID: {simulation.database_id}")
    display_json(simulation.model_dump(), console)

    if not poll:
        sim_id = simulation.database_id
        console.print(f"\n[memphis.hint]Track progress:[/]  atlantis simulation status {sim_id}")
        console.print(f"[memphis.hint]Download data:[/]   atlantis simulation outputs {sim_id} --dest ./debug")
        return

    # Poll until done
    console.print("\n[memphis.info]Polling simulation status...[/]")
    poll_interval = 30
    elapsed = 0
    status = "running"
    run = None
    while status not in ("completed", "failed", "cancelled", "unknown"):
        time.sleep(poll_interval)
        elapsed += poll_interval
        try:
            run = data_service.get_workflow_status(simulation_id=simulation.database_id)
            status = run.status.value
        except Exception as e:
            console.print(f"  [{elapsed}s] [memphis.error]error: {e}[/]")
            continue
        console.print(f"  [{elapsed}s] status: [{status_style(status)}]{status}[/]")

    error_detail = f"\n{run.error_message}" if run and run.error_message else ""
    console.print(
        Panel(
            f"[{status_style(status)}]{status.upper()}[/]{error_detail}",
            title=f"Simulation {simulation.database_id}",
            border_style=status_border(status),
        )
    )
    if status == "completed":
        console.print(
            f"\n[memphis.hint]Download data:[/]  atlantis simulation outputs {simulation.database_id} --dest ./debug"
        )


@simulation_cli.command("get", help="Get a simulation by its database ID.")
def simulation_get(
    simulation_id: int = Argument(help="Simulation database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    simulation = data_service.get_workflow(simulation_id=simulation_id)
    display_json(simulation.model_dump(), console)


@simulation_cli.command("list", help="List simulations.")
def simulation_list(
    n: int | None = Option(
        default=None, help="Number of entries to show. Positive = first N, negative = last N (by ID)."
    ),
    experiment_id: str | None = Option(default=None, help="Comma-separated experiment IDs to filter by."),
    tag: str | None = Option(
        default=None,
        help="Comma-separated tags to filter by (e.g. 'cd1'). Use 'atlantis simulation tags' to list tags in use.",
    ),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    simulations = data_service.show_workflows(experiment_id=experiment_id, tag=tag)
    simulations = _slice_by_id(simulations, n)
    for sim in simulations:
        display_json(sim.model_dump(), console)


@simulation_cli.command("tags", help="List the tags in use and the experiment IDs carrying each.")
def simulation_tags(
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    tags = data_service.list_simulation_tags()
    if not tags:
        console.print("[dim]No simulation tags defined.[/]")
        return
    for tag_name, experiment_ids in tags.items():
        console.print(f"[memphis.info]{tag_name}[/]")
        for eid in experiment_ids:
            console.print(f"  {eid}")


@simulation_cli.command("tag", help="Attach one or more tags to an existing simulation.")
def simulation_tag(
    simulation_id: int = Argument(help="Database ID of the simulation to tag."),
    tags: list[str] = Argument(help="One or more tags to add (e.g. cd1)."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    simulation = data_service.tag_workflow(simulation_id=simulation_id, tags=list(tags))
    console.print(f"[memphis.success]Tagged simulation {simulation_id}[/]  tags: {simulation.tags}")


@simulation_cli.command("configs", help="List available config filenames for a simulator's repo.")
def simulation_configs(
    simulator_id: int = Argument(help="Simulator database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    discovery = data_service.discover_repo(simulator_id=simulator_id)
    repo = f"{discovery.git_repo_url} @ {discovery.git_commit_hash}"
    console.print(f"[memphis.info]Config files for simulator {simulator_id}[/] ({repo}):\n")
    if discovery.config_filenames:
        for name in discovery.config_filenames:
            console.print(f"  {name}")
    else:
        console.print("  [dim]No config files found (embedded default will be used)[/]")


@simulation_cli.command("analyses", help="List available analysis modules for a simulator's repo.")
def simulation_analyses(
    simulator_id: int = Argument(help="Simulator database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    discovery = data_service.discover_repo(simulator_id=simulator_id)
    repo = f"{discovery.git_repo_url} @ {discovery.git_commit_hash}"
    console.print(f"[memphis.info]Analysis modules for simulator {simulator_id}[/] ({repo}):\n")
    if discovery.analysis_modules:
        for category, modules in sorted(discovery.analysis_modules.items()):
            console.print(f"  [bold]{category}:[/]")
            for mod in modules:
                console.print(f"    {mod}")
    else:
        console.print("  [dim]No analysis modules discovered[/]")


@simulation_cli.command("status", help="Get the workflow log tail and status for a simulation.")
def simulation_status(
    simulation_id: int = Argument(help="Simulation database ID."),
    poll: bool = Option(default=False, help="Poll until simulation completes."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    import time

    from viva_api.common.handlers.simulations import workflow_log

    if not poll:
        workflow_log(simulation_id=simulation_id, base_url=base_url)
        return

    # Poll until terminal state
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    console.print("[memphis.info]Polling...[/]")
    poll_interval = 30
    elapsed = 0
    status = "running"
    while status not in ("completed", "failed", "cancelled", "unknown"):
        time.sleep(poll_interval)
        elapsed += poll_interval
        try:
            run = data_service.get_workflow_status(simulation_id=simulation_id)
            status = run.status.value
        except Exception as e:
            console.print(f"  [{elapsed}s] [memphis.error]error: {e}[/]")
            continue
        console.print(f"  [{elapsed}s] status: [{status_style(status)}]{status}[/]")

    workflow_log(simulation_id=simulation_id, base_url=base_url)


@simulation_cli.command("cancel", help="Cancel a running simulation.")
def simulation_cancel(
    simulation_id: int = Argument(help="Simulation database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    result = data_service.cancel_workflow(simulation_id=simulation_id)
    display_json(result.model_dump(), console)


@simulation_cli.command("log", help="Show the Nextflow workflow log for a simulation.")
def simulation_log(
    simulation_id: int = Argument(help="Simulation database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)
    try:
        log = data_service.get_workflow_log(simulation_id=simulation_id, truncate=False)
        console.print(Panel(log, title=f"Workflow Log (sim {simulation_id})", border_style="memphis.border.info"))
    except Exception as e:
        console.print(f"[memphis.error]Error: {e}[/]")


@simulation_cli.command("outputs", help="Download simulation output data as a tar.gz archive.")
def simulation_outputs(
    simulation_id: int = Argument(help="Simulation database ID."),
    dest: str | None = Option(default=None, help="Destination directory. Defaults to ./simulation_id_<ID>."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    outdir = Path(dest) if dest is not None else Path(f"simulation_id_{simulation_id}")
    archive_dir = asyncio.run(data_service.get_output_data(simulation_id=simulation_id, dest=outdir))
    console.print(f"[memphis.success]Saved simulation outputs to:[/] {archive_dir!s}")


@simulation_cli.command("analysis", help="Run standalone analysis on existing simulation output.")
def simulation_analysis(
    simulation_id: int = Argument(help="Simulation database ID (must be completed)."),
    modules: str | None = Option(
        default=None,
        help='JSON string of analysis modules. E.g. \'{"single": {"ptools_rna": {"n_tp": 10}}}\'.'
        " If omitted, runs default ptools modules.",
    ),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    try:
        result = data_service.run_analysis(simulation_id=simulation_id, modules=modules)
        console.print("[memphis.success]Analysis submitted![/]")
        display_json(result, console)
    except Exception as e:
        console.print(f"[memphis.error]Error: {e}[/]")


# -- Parca commands --


@parca_cli.command("list", help="List parca datasets.")
def parca_list(
    n: int | None = Option(
        default=None, help="Number of entries to show. Positive = first N, negative = last N (by ID)."
    ),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    datasets = data_service.get_parca_datasets()
    datasets = _slice_by_id(datasets, n)
    for ds in datasets:
        display_json(ds.model_dump(), console)


@parca_cli.command("status", help="Get the status of a parca run by its database ID.")
def parca_status(
    parca_id: int = Argument(help="Parca dataset database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    status = data_service.get_parca_status(parca_id=parca_id)
    display_json(status.model_dump(), console)


# -- Analysis commands --


@analysis_cli.command("get", help="Get an analysis spec by its database ID.")
def analysis_get(
    analysis_id: int = Argument(help="Analysis database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    analysis = data_service.get_analysis(analysis_id=analysis_id)
    display_json(analysis.model_dump(), console)


@analysis_cli.command("status", help="Get the status of an analysis run.")
def analysis_status(
    analysis_id: int = Argument(help="Analysis database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    status = data_service.get_analysis_status(analysis_id=analysis_id)
    display_json(status.model_dump(), console)


@analysis_cli.command("log", help="Get the log output of an analysis run.")
def analysis_log(
    analysis_id: int = Argument(help="Analysis database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)
    log = data_service.get_analysis_log(analysis_id=analysis_id)
    console.print(Panel(log, title=f"Analysis Log ({analysis_id})", border_style="memphis.border.info"))


@analysis_cli.command("plots", help="Get analysis plot outputs (HTML) for an analysis run.")
def analysis_plots(
    analysis_id: int = Argument(help="Analysis database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    plots = data_service.get_analysis_plots(analysis_id=analysis_id)
    for plot in plots:
        display_json(plot.model_dump(), console)


# -- Compose (process-bigraph) commands --


@compose_cli.command("run", help="Submit a compose simulation (OMEX/PBG/SBML upload).")
def compose_run(
    file: Path = Argument(help="Path to OMEX, PBG, or SBML file."),
    interval_time: float = Option(default=1.0, help="Simulation interval/duration."),
    batch: bool = Option(default=False, help="Use batch submission mode."),
    poll: bool = Option(default=False, help="Poll until job completes."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    import time

    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)

    if not file.exists():
        console.print(f"[memphis.error]File not found:[/] {file}")
        raise typer.Exit(1)

    with console.status("[memphis.spinner]Submitting compose simulation..."):
        result = data_service.compose_run_simulation(file_path=file, interval_time=interval_time, batch=batch)
    sim_id = result["simulation_database_id"]
    sim_ver_id = result["simulator_database_id"]
    console.print(f"[memphis.label]Simulation ID:[/] {sim_id}")
    console.print(f"[memphis.label]Simulator ID:[/] {sim_ver_id}")
    display_json(result, console)

    if poll:
        console.print("[memphis.info]Polling for completion...[/]")
        poll_interval = 10
        elapsed = 0
        status = "running"
        while status not in ("completed", "failed", "cancelled"):
            time.sleep(poll_interval)
            elapsed += poll_interval
            try:
                status_data = data_service.compose_get_simulation_status(simulation_id=sim_id)
                status = (status_data.get("status") or "unknown").lower()
            except Exception:
                status = "unknown"
            console.print(f"  [{elapsed}s] status: [{status_style(status)}]{status}[/]")
        console.print(
            Panel(
                f"[{status_style(status)}]{status.upper()}[/]",
                title=f"Compose simulation {sim_id}",
                border_style=status_border(status),
            )
        )


@compose_cli.command("status", help="Get compose simulation job status. Accepts several IDs.")
def compose_status(
    simulation_ids: list[int] = Argument(help="One or more compose simulation database IDs."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    """Status for one simulation, or a whole campaign in ONE request.

    Given several IDs this uses ``/compose/v1/simulations/status/batch`` rather
    than looping — the endpoint viva-api grew for exactly this, and which the
    workbench's job layer already polls through. Checking twenty runs should not
    be twenty round trips, and through the SSM tunnel the difference is felt.

    A single ID keeps the previous behaviour EXACTLY: same endpoint, same panel,
    same JSON. The batch path is an addition, not a replacement, so no existing
    invocation changes shape.
    """
    from rich.panel import Panel
    from rich.table import Table

    console = get_console()
    data_service = get_data_service(base_url=base_url)

    if len(simulation_ids) == 1:
        simulation_id = simulation_ids[0]
        result = data_service.compose_get_simulation_status(simulation_id=simulation_id)
        s = (result.get("status") or "unknown").lower()
        console.print(
            Panel(
                f"[{status_style(s)}]{s.upper()}[/]",
                title=f"Compose simulation {simulation_id}",
                border_style=status_border(s),
            )
        )
        display_json(result, console)
        return

    rows = data_service.compose_get_simulations_status_batch(simulation_ids=simulation_ids)
    by_id = {r.get("ref_id") or r.get("database_id") or r.get("sim_id"): r for r in rows if isinstance(r, dict)}

    table = Table(title=f"Compose simulations ({len(simulation_ids)})", border_style="magenta")
    table.add_column("ID", justify="right", style="memphis.label")
    table.add_column("Status")
    table.add_column("Error", overflow="fold")
    missing = 0
    for sid in simulation_ids:
        row = by_id.get(sid)
        if row is None:
            # An id the server did not return is NOT "unknown status" — it is an
            # id that does not exist here. Saying so beats a blank line the user
            # has to investigate.
            missing += 1
            table.add_row(str(sid), "[dim]not found[/]", "")
            continue
        st = (row.get("status") or "unknown").lower()
        table.add_row(str(sid), f"[{status_style(st)}]{st}[/]", str(row.get("error_message") or ""))
    console.print(table)

    counts: dict[str, int] = {}
    for row in by_id.values():
        st = (row.get("status") or "unknown").lower()
        counts[st] = counts.get(st, 0) + 1
    summary = "  ".join(f"[{status_style(k)}]{v} {k}[/]" for k, v in sorted(counts.items()))
    if missing:
        summary += f"  [dim]{missing} not found[/]"
    console.print(summary, highlight=False)


@compose_cli.command("results", help="Download compose simulation results as a zip file.")
def compose_results(
    simulation_id: int = Argument(help="Compose simulation database ID."),
    dest: str = Option(default="./compose_results", help="Local destination directory."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    dest_path = Path(dest)
    with console.status("[memphis.spinner]Downloading compose results..."):
        out_file = data_service.compose_get_simulation_results(simulation_id=simulation_id, dest=dest_path)
    console.print(f"[memphis.success]Results saved to:[/] {out_file}")


@compose_cli.command("doc", help="Retrieve the process-bigraph document used for a compose simulation.")
def compose_doc(
    simulation_id: int = Argument(help="Compose simulation database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    result = data_service.compose_get_simulation_document(simulation_id=simulation_id)
    display_json(result, console)


@compose_cli.command("simulators", help="List registered compose simulators.")
def compose_simulators(
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    result = data_service.compose_list_simulators()
    display_json(result, console)


@compose_cli.command("processes", help="List registered process-bigraph processes.")
def compose_processes(
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    result = data_service.compose_list_processes()
    if not result:
        console.print("[memphis.dim]No processes registered.[/]")
    else:
        display_json(list(result), console)


@compose_cli.command("steps", help="List registered process-bigraph steps.")
def compose_steps(
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    result = data_service.compose_list_steps()
    if not result:
        console.print("[memphis.dim]No steps registered.[/]")
    else:
        display_json(list(result), console)


@compose_cli.command("build-status", help="Get compose container build status.")
def compose_build_status(
    simulator_id: int = Argument(help="Compose simulator database ID."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)
    result = data_service.compose_get_build_status(simulator_id=simulator_id)
    s = (result.get("status") or "unknown").lower()
    console.print(
        Panel(
            f"[{status_style(s)}]{s.upper()}[/]",
            title=f"Build — compose simulator {simulator_id}",
            border_style=status_border(s),
        )
    )
    display_json(result, console)


@compose_cli.command("ecoli", help="Run a v2ecoli whole-cell E. coli simulation via process-bigraph.")
def compose_ecoli(
    duration: float = Option(default=60.0, help="Simulation duration in seconds."),
    seed: int = Option(default=0, help="Random seed for stochastic processes."),
    interval: float = Option(default=1.0, help="Execution interval (timestep) in seconds."),
    features: str = Option(default="[]", help="JSON list of feature modules, e.g. '[\"ppgpp_regulation\"]'"),
    cache_dir: str = Option(default="/out/cache", help="Absolute path to ParCa cache inside container."),
    poll: bool = Option(default=False, help="Poll until job completes."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    import time

    from rich.panel import Panel

    console = get_console()
    data_service = get_data_service(base_url=base_url)

    with console.status("[memphis.spinner]Submitting v2ecoli simulation..."):
        result = data_service.compose_run_v2ecoli(
            duration=duration, seed=seed, interval=interval, features=features, cache_dir=cache_dir
        )
    sim_id = result["simulation_database_id"]
    console.print(f"[memphis.label]Simulation ID:[/] {sim_id}")
    console.print(f"[memphis.label]Simulator ID:[/] {result['simulator_database_id']}")
    display_json(result, console)

    if poll:
        console.print("[memphis.info]Polling for completion...[/]")
        poll_interval = 10
        elapsed = 0
        status = "running"
        while status not in ("completed", "failed", "cancelled"):
            time.sleep(poll_interval)
            elapsed += poll_interval
            try:
                status_data = data_service.compose_get_simulation_status(simulation_id=sim_id)
                status = (status_data.get("status") or "unknown").lower()
            except Exception:
                status = "unknown"
            console.print(f"  [{elapsed}s] status: [{status_style(status)}]{status}[/]")
        console.print(
            Panel(
                f"[{status_style(status)}]{status.upper()}[/]",
                title=f"v2ecoli simulation {sim_id}",
                border_style=status_border(status),
            )
        )


@compose_cli.command("copasi", help="Run a COPASI simulation from an SBML file.")
def compose_copasi(
    sbml: Path = Argument(help="Path to SBML file."),
    start_time: float = Option(default=0.0, help="Simulation start time."),
    duration: float = Option(default=10.0, help="Simulation duration."),
    num_data_points: float = Option(default=100.0, help="Number of data points."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    with console.status("[memphis.spinner]Submitting COPASI simulation..."):
        result = data_service.compose_run_copasi(
            sbml_path=sbml, start_time=start_time, duration=duration, num_data_points=num_data_points
        )
    display_json(result, console)


@compose_cli.command("tellurium", help="Run a Tellurium simulation from an SBML file.")
def compose_tellurium(
    sbml: Path = Argument(help="Path to SBML file."),
    start_time: float = Option(default=0.0, help="Simulation start time."),
    end_time: float = Option(default=10.0, help="Simulation end time."),
    num_data_points: float = Option(default=100.0, help="Number of data points."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    with console.status("[memphis.spinner]Submitting Tellurium simulation..."):
        result = data_service.compose_run_tellurium(
            sbml_path=sbml, start_time=start_time, end_time=end_time, num_data_points=num_data_points
        )
    display_json(result, console)


@compose_cli.command("biomodels-ids", help="List BioModels database identifiers.")
def compose_biomodels_ids(
    n: int = Option(default=20, help="Max number of identifiers to return."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    with console.status("[memphis.spinner]Fetching BioModels identifiers..."):
        ids = data_service.compose_biomodels_identifiers(n=n)
    console.print(ids)


@compose_cli.command("biomodels-meta", help="Get metadata for a BioModels database entry.")
def compose_biomodels_meta(
    biomodel_id: str = Argument(help="BioModel ID (e.g. BIOMD0000000001)."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    with console.status(f"[memphis.spinner]Fetching metadata for {biomodel_id}..."):
        result = data_service.compose_biomodels_metadata(biomodel_id=biomodel_id)
    display_json(result, console)


@compose_cli.command("biomodels-run", help="Run a BioModels database model via Copasi or Tellurium.")
def compose_biomodels_run(
    biomodel_id: str = Argument(help="BioModel ID (e.g. BIOMD0000000001)."),
    simulator: str = Option(default="copasi", help="Simulator: copasi or tellurium."),
    poll: bool = Option(default=False, help="Poll for job status until complete."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    with console.status(f"[memphis.spinner]Submitting {biomodel_id} via {simulator}..."):
        result = data_service.compose_biomodels_run(biomodel_id=biomodel_id, simulator=simulator)
    display_json(result, console)
    if poll:
        sim_id = result.get("simulation_database_id")
        if sim_id is None:
            console.print("[yellow]No simulation_database_id in response; cannot poll.[/yellow]")
            return
        import time

        while True:
            time.sleep(5)
            with console.status(f"[memphis.spinner]Polling status for simulation {sim_id}..."):
                status_data = data_service.compose_get_simulation_status(simulation_id=sim_id)
            status = status_data.get("status", "unknown")
            console.print(f"  Status: {status}")
            if status in ("completed", "failed", "cancelled", "timeout"):
                break
        display_json(status_data, console)


@compose_cli.command("biomodels-batch", help="Run a batch of BioModels database models.")
def compose_biomodels_batch(
    simulator: str = Option(default="copasi", help="Simulator: copasi or tellurium."),
    n: int = Option(default=5, help="Number of models to run (ignored if --ids provided)."),
    ids: str = Option(default="", help="Comma-separated BioModel IDs to run."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    model_ids = [i.strip() for i in ids.split(",") if i.strip()] if ids else None
    with console.status("[memphis.spinner]Submitting BioModels batch..."):
        result = data_service.compose_biomodels_batch(
            simulator=simulator,
            model_ids=model_ids,
            n_models=n if model_ids is None else None,
        )
    display_json(result, console)


@compose_cli.command("biomodels-audit", help="Run a BioModel on multiple simulators for cross-validation.")
def compose_biomodels_audit(
    biomodel_id: str = Argument(help="BioModel ID (e.g. BIOMD0000000001)."),
    simulators: str = Option(default="copasi,tellurium", help="Comma-separated simulators to use."),
    poll: bool = Option(default=False, help="Poll for job status until complete."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    sim_list = [s.strip() for s in simulators.split(",") if s.strip()]
    with console.status(f"[memphis.spinner]Submitting audit for {biomodel_id} ({', '.join(sim_list)})..."):
        result = data_service.compose_biomodels_audit(biomodel_id=biomodel_id, simulators=sim_list)
    display_json(result, console)
    if poll:
        experiment = result.get("experiment", result)
        sim_id = experiment.get("simulation_database_id") if isinstance(experiment, dict) else None
        if sim_id is None:
            console.print("[yellow]No simulation_database_id in response; cannot poll.[/yellow]")
            return
        import time

        while True:
            time.sleep(5)
            with console.status(f"[memphis.spinner]Polling audit simulation {sim_id}..."):
                status_data = data_service.compose_get_simulation_status(simulation_id=sim_id)
            status = status_data.get("status", "unknown")
            console.print(f"  Status: {status}")
            if status in ("completed", "failed", "cancelled", "timeout"):
                break
        display_json(status_data, console)


@compose_cli.command("biomodels-regression", help="Run a BioModels regression suite.")
def compose_biomodels_regression(
    n: int = Option(default=10, help="Number of models to run (ignored if --ids provided)."),
    ids: str = Option(default="", help="Comma-separated BioModel IDs to run."),
    simulators: str = Option(default="copasi,tellurium", help="Comma-separated simulators to wire into each model."),
    base_url: ApiBaseUrl = Option(default=API_BASE_URL, help="API server base URL."),
) -> None:
    console = get_console()
    data_service = get_data_service(base_url=base_url)
    model_ids = [i.strip() for i in ids.split(",") if i.strip()] if ids else None
    sim_list = [s.strip() for s in simulators.split(",") if s.strip()]
    with console.status("[memphis.spinner]Submitting BioModels regression suite..."):
        result = data_service.compose_biomodels_regression(
            n_models=n,
            model_ids=model_ids,
            simulators=sim_list,
        )
    submitted = result.get("submitted", [])
    failed = result.get("failed", [])
    total = result.get("total_requested", n)
    console.print(f"[bold]Regression complete:[/bold] {len(submitted)}/{total} submitted, {len(failed)} failed")
    if failed:
        console.print(f"[yellow]Failed IDs:[/yellow] {', '.join(failed)}")
    display_json(result, console)


# -- Demo commands --


@demo_cli.command("get-data", help="Download S3 simulation outputs directly (mirrors test_outputs.py e2e test).")
def demo_get_data(
    dest: str = Option(default="./demo_outputs", help="Local destination directory for downloaded + extracted files."),
) -> None:
    """Download simulation output data directly from S3 — no running API server needed.

    Replicates the exact flow from tests/api/ecoli/test_outputs.py:
    1. Reads TEST_BUCKET_EXPERIMENT_OUTDIR from .dev_env to derive the experiment_id.
    2. Initialises a real FileServiceS3 (uses AWS creds from env).
    3. Calls the handler's _download_outputs_from_s3() to pull analyses/ + workflow_config.json.
    4. Creates a tar.gz archive and extracts it locally.

    Prerequisites:
    - AWS credentials configured (AWS_ACCESS_KEY_ID etc. in .dev_env or environment)
    - TEST_BUCKET_EXPERIMENT_OUTDIR set in .dev_env or environment
    """
    asyncio.run(_demo_get_data_async(dest))


async def _download_s3_with_progress(
    fs: FileServiceS3,
    experiment_prefix: str,
    local_cache: Path,
    console: Console,
) -> None:
    """List, filter, and download S3 objects with a Rich progress bar."""
    from viva_api.common.handlers.simulations import _ACCEPTED_ANALYSES_EXTENSIONS, _WORKFLOW_CONFIG_KEY
    from viva_api.common.storage.file_paths import S3FilePath

    # List files in S3
    with console.status("[memphis.spinner]Listing S3 objects..."):
        analyses_prefix = S3FilePath(s3_path=Path(f"{experiment_prefix}/analyses"))
        analyses_listing = await fs.get_listing(analyses_prefix)

    # Filter to accepted extensions
    download_items: list[tuple[str, Path]] = []
    for item in analyses_listing:
        if not item.Key.endswith(_ACCEPTED_ANALYSES_EXTENSIONS):
            continue
        relative = Path(item.Key).relative_to(experiment_prefix)
        local_file = local_cache / relative
        if not local_file.exists():
            download_items.append((item.Key, local_file))

    # Add workflow_config.json
    workflow_config_key = f"{experiment_prefix}/{_WORKFLOW_CONFIG_KEY}"
    local_workflow_config = local_cache / _WORKFLOW_CONFIG_KEY
    if not local_workflow_config.exists():
        download_items.append((workflow_config_key, local_workflow_config))

    # Download with progress bar
    from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

    with Progress(
        SpinnerColumn(style="memphis.spinner"),
        TextColumn("[memphis.progress]{task.description}"),
        BarColumn(bar_width=40, complete_style="bright_magenta", finished_style="bright_green"),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Downloading from S3", total=len(download_items))
        for s3_key, local_file in download_items:
            local_file.parent.mkdir(parents=True, exist_ok=True)
            progress.update(task, description=f"[memphis.progress]{Path(s3_key).name}")
            try:
                await fs.download_file(S3FilePath(s3_path=Path(s3_key)), local_file)
            except Exception:
                if _WORKFLOW_CONFIG_KEY in s3_key:
                    console.print("[memphis.dim]workflow_config.json not found, skipping[/]")
                else:
                    raise
            progress.advance(task)


async def _demo_get_data_async(dest: str) -> None:
    import os
    import tarfile
    from urllib.parse import urlparse

    from rich.panel import Panel
    from rich.tree import Tree

    console = get_console()

    # 1. Derive experiment_id from TEST_BUCKET_EXPERIMENT_OUTDIR (same as test_outputs.py)
    test_outdir = os.environ.get("TEST_BUCKET_EXPERIMENT_OUTDIR", "")
    if not test_outdir:
        console.print(
            "[memphis.error]TEST_BUCKET_EXPERIMENT_OUTDIR is not set.[/]\n"
            "[memphis.dim]Set it in assets/dev/config/.dev_env or your environment, e.g.:\n"
            "  TEST_BUCKET_EXPERIMENT_OUTDIR=s3://bucket/prefix/experiment_id/[/]"
        )
        raise typer.Exit(1)

    experiment_id = urlparse(test_outdir).path.strip("/").rsplit("/", 1)[-1]
    console.print(f"[memphis.info]Experiment ID:[/] {experiment_id}")
    console.print(f"[memphis.dim]Source: {test_outdir}[/]\n")

    # 2. Initialise FileServiceS3 and wire into global deps
    from viva_api.common.storage.file_service_s3 import FileServiceS3
    from viva_api.config import get_settings
    from viva_api.dependencies import get_file_service, set_file_service

    settings = get_settings()
    if not settings.storage_s3_bucket or not settings.storage_s3_region:
        console.print("[memphis.error]S3 settings (STORAGE_S3_BUCKET, STORAGE_S3_REGION) not configured.[/]")
        raise typer.Exit(1)

    console.print(f"[memphis.info]S3 bucket:[/] {settings.storage_s3_bucket}")
    console.print(f"[memphis.info]S3 region:[/] {settings.storage_s3_region}")
    console.print(f"[memphis.info]Output prefix:[/] {settings.s3_output_prefix}\n")

    saved_fs = get_file_service()
    fs = FileServiceS3()
    set_file_service(fs)

    try:
        dest_path = Path(dest).resolve()
        local_cache = dest_path / experiment_id
        local_cache.mkdir(parents=True, exist_ok=True)

        experiment_prefix = f"{settings.s3_output_prefix}/{experiment_id}"
        await _download_s3_with_progress(fs, experiment_prefix, local_cache, console)

        # 3. Verify what we got
        real_files = [f for f in local_cache.rglob("*") if f.is_file()]
        if not real_files:
            console.print(f"[memphis.error]No files downloaded for experiment '{experiment_id}'.[/]")
            console.print("[memphis.dim]Check that TEST_BUCKET_EXPERIMENT_OUTDIR points to valid simulation output.[/]")
            raise typer.Exit(1)

        tsv_count = sum(1 for f in real_files if f.suffix == ".tsv")
        json_count = sum(1 for f in real_files if f.suffix == ".json")

        # 4. Create tar.gz archive (same as the test's artifact saving)
        archive_path = dest_path / f"{experiment_id}.tar.gz"
        with (
            console.status("[memphis.spinner]Creating archive..."),
            tarfile.open(archive_path, "w:gz") as tar,
        ):
            tar.add(str(local_cache), arcname=experiment_id)

        # 5. Report
        tree = Tree(f"[memphis.label]{experiment_id}/[/]")
        tree.add(f"[memphis.success]{tsv_count}[/] .tsv files")
        tree.add(f"[memphis.info]{json_count}[/] .json files")
        tree.add(f"[memphis.dim]{len(real_files)} total files[/]")
        console.print(Panel(tree, title="Download Complete", border_style="memphis.border.success"))
        console.print(f"[memphis.success]Extracted to:[/]  {local_cache}")
        console.print(f"[memphis.success]Archive saved:[/] {archive_path}")

    finally:
        await fs.close()
        set_file_service(saved_fs)


def fonts(txt: str, color: str = "bold spring_green3") -> None:
    """Browse all RichFiglet fonts with a given text string."""
    import typing

    import rich_pyfiglet

    hints = typing.get_type_hints(rich_pyfiglet.RichFiglet.__init__)
    font_type = hints.get("font")
    font_names = typing.get_args(font_type)
    for font_name in font_names:
        console = get_console()
        a = rich_pyfiglet.RichFiglet("atlantis", font=font_name, colors=["purple"])
        console.print(f"[memphis.running]=== FONT: {font_name} ===[/]")
        console.print(a)
        print()


def draw_ecoli(
    width: int = 160,
    height: int = 80,
    cell_color: tuple[int, ...] = (120, 195, 130),
    show_panel: bool = True,
) -> None:
    """Draw a biologically accurate E. coli cell via rich-pixels."""
    import math
    import random

    from PIL import Image, ImageDraw, ImageFilter
    from rich.console import Console
    from rich.panel import Panel
    from rich_pixels import Pixels

    console = Console()
    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    cx = width // 2
    cy = height // 2

    # ── Proportions (all relative to canvas) ─────────────────────────────────
    body_rx = int(width * 0.38)  # half-length of cell body
    body_ry = int(height * 0.30)  # half-width of cell body
    margin_x = cx - body_rx
    margin_y = cy - body_ry

    # Color palette — biologically inspired greens
    capsule_col = (*cell_color[:3], 28)  # very faint halo
    outer_mem_col = tuple(max(0, c - 30) for c in cell_color)  # darker rim
    peri_col = tuple(min(255, c + 40) for c in cell_color)  # lighter band
    cytoplasm_col = cell_color
    nucleoid_col = (50, 120, 70)
    ribosome_col = (80, 160, 95)
    flagella_col = (60, 130, 75)
    pili_col = (90, 150, 100)

    # ── 1. Capsule (polysaccharide halo) ─────────────────────────────────────
    cap_pad = 6
    draw.ellipse(
        [margin_x - cap_pad, margin_y - cap_pad, margin_x + body_rx * 2 + cap_pad, margin_y + body_ry * 2 + cap_pad],
        fill=capsule_col,
    )

    # ── 2. Outer membrane (outermost solid layer) ─────────────────────────────
    draw.ellipse(
        [margin_x, margin_y, margin_x + body_rx * 2, margin_y + body_ry * 2],
        fill=outer_mem_col,
    )

    # ── 3. Periplasmic space (lighter band just inside outer membrane) ────────
    peri_shrink = max(3, int(body_ry * 0.18))
    draw.ellipse(
        [
            margin_x + peri_shrink,
            margin_y + peri_shrink,
            margin_x + body_rx * 2 - peri_shrink,
            margin_y + body_ry * 2 - peri_shrink,
        ],
        fill=peri_col,
    )

    # ── 4. Inner membrane + cytoplasm ─────────────────────────────────────────
    inner_shrink = peri_shrink + max(2, int(body_ry * 0.12))
    draw.ellipse(
        [
            margin_x + inner_shrink,
            margin_y + inner_shrink,
            margin_x + body_rx * 2 - inner_shrink,
            margin_y + body_ry * 2 - inner_shrink,
        ],
        fill=cytoplasm_col,
    )

    # ── 5. Nucleoid — irregular blob center (compacted DNA mass) ─────────────
    # Approximated as overlapping ellipses to look organic
    n_cx, n_cy = cx, cy
    n_rx = int(body_rx * 0.38)
    n_ry = int(body_ry * 0.52)
    for dx, dy, rx_frac, ry_frac in [
        (0, 0, 1.0, 1.0),
        (int(n_rx * 0.3), int(n_ry * 0.2), 0.7, 0.6),
        (-int(n_rx * 0.3), -int(n_ry * 0.2), 0.65, 0.55),
    ]:
        draw.ellipse(
            [
                n_cx + dx - int(n_rx * rx_frac),
                n_cy + dy - int(n_ry * ry_frac),
                n_cx + dx + int(n_rx * rx_frac),
                n_cy + dy + int(n_ry * ry_frac),
            ],
            fill=nucleoid_col,
        )

    # ── 6. Ribosomes — tiny dots scattered in cytoplasm ──────────────────────
    rng = random.Random(42)  # fixed seed → deterministic layout
    r_dot = max(1, int(min(width, height) * 0.018))
    attempts = 0
    placed = 0
    while placed < 28 and attempts < 400:
        attempts += 1
        # Sample inside the inner membrane ellipse
        angle = rng.uniform(0, 2 * math.pi)
        rad = rng.uniform(0.1, 0.82)
        rx = int((body_rx - inner_shrink - r_dot) * rad)
        ry = int((body_ry - inner_shrink - r_dot) * rad)
        px = cx + int(rx * math.cos(angle))
        py = cy + int(ry * math.sin(angle))
        # Skip if too close to nucleoid center
        dist_nuc = math.sqrt(((px - n_cx) / n_rx) ** 2 + ((py - n_cy) / n_ry) ** 2)
        if dist_nuc < 1.05:
            continue
        draw.ellipse([px - r_dot, py - r_dot, px + r_dot, py + r_dot], fill=ribosome_col)
        placed += 1

    # ── 7. Pili / fimbriae — short thin projections around perimeter ──────────
    pili_count = 14
    pili_len = max(4, int(body_ry * 0.35))
    for i in range(pili_count):
        angle = (2 * math.pi * i / pili_count) + 0.15
        # Point on the outer membrane ellipse surface
        sx = cx + int(body_rx * math.cos(angle))
        sy = cy + int(body_ry * math.sin(angle))
        ex = cx + int((body_rx + pili_len) * math.cos(angle))
        ey = cy + int((body_ry + pili_len) * math.sin(angle))
        draw.line([(sx, sy), (ex, ey)], fill=pili_col, width=1)

    # ── 8. Peritrichous flagella — long wavy filaments all around cell ────────
    flagella_specs = [
        # (start_angle_frac, wave_amp, wave_freq, length, direction)
        (0.00, 0.18, 2.8, 0.52, 1),
        (0.12, -0.16, 3.2, 0.48, -1),
        (0.25, 0.20, 2.5, 0.55, 1),
        (0.38, -0.18, 3.0, 0.50, -1),
        (0.50, 0.15, 2.7, 0.52, 1),
        (0.62, -0.20, 3.1, 0.46, -1),
        (0.75, 0.17, 2.9, 0.54, 1),
        (0.88, -0.15, 3.3, 0.48, -1),
    ]
    f_thick = max(1, int(min(width, height) * 0.015))
    for angle_frac, amp_frac, freq, length_frac, direction in flagella_specs:
        base_angle = 2 * math.pi * angle_frac
        # Start at outer membrane surface
        sx = cx + int(body_rx * math.cos(base_angle))
        sy = cy + int(body_ry * math.sin(base_angle))

        f_len = int(min(width, height) * length_frac)
        amp = int(min(width, height) * abs(amp_frac))
        steps = 32
        points = [(sx, sy)]

        # Outward direction vector (perpendicular to cell surface normal → away)
        out_dx = math.cos(base_angle)
        out_dy = math.sin(base_angle)
        # Perpendicular (tangent) for the wave oscillation
        perp_dx = -math.sin(base_angle)
        perp_dy = math.cos(base_angle)

        for s in range(1, steps + 1):
            t = s / steps
            dist = f_len * t
            wave = amp * math.sin(freq * 2 * math.pi * t) * direction
            px = sx + int(out_dx * dist + perp_dx * wave)
            py = sy + int(out_dy * dist + perp_dy * wave)
            points.append((px, py))

        # Draw as polyline segments
        for k in range(len(points) - 1):
            draw.line([points[k], points[k + 1]], fill=flagella_col, width=f_thick)

    # ── 9. Slight soft-edge on the whole image (anti-alias feel) ─────────────
    img = img.filter(ImageFilter.SMOOTH_MORE)
    img = img.filter(ImageFilter.SMOOTH)

    # ── Render ────────────────────────────────────────────────────────────────
    pixels = Pixels.from_image(img)

    if show_panel:
        console.print(
            Panel(
                pixels,
                title="[bold green]E. coli[/bold green]",
                subtitle="[dim]Escherichia coli · rod-shaped bacterium[/dim]",
                border_style="green",
                padding=(0, 1),
            )
        )
    else:
        console.print(pixels)


# atlantis simulation run test-cli-baseline-seeds1000-generations10 11 --generations 10 --seeds 1000 --base-url http://localhost:8080


if __name__ == "__main__":
    main()
