"""Atlantis TUI — Textual-based interactive terminal interface for SMS API.

Launch via:
    atlantis tui [--base-url URL]

Or directly:
    uv run python -m app.tui
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from rich.syntax import Syntax
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    RichLog,
    Select,
    Static,
)

from app.app_data_service import BaseUrl, E2EDataService, get_data_service

# ─── Constants ────────────────────────────────────────────────────────────────

_HM = "[bold magenta]"
_HC = "[bold #00ffff]"
_HG = "[bold #ffd700]"
_HR = "[/]"
BANNER = (
    f"    {_HM}╭────────────────────────────────────────────╮{_HR}\n"
    f"    [bold #00ff00]│    ▄▀▄ ▀█▀ █   ▄▀▄ █▄ █ ▀█▀ █ ▄▀▀          │[/]{_HM}∿~∿~~∿~∿~{_HR}\n"
    f"    [bold #9370db]│    █▀█  █  █▄▄ █▀█ █ ▀█  █  █ ▄██           │[/]{_HC}~∿~∿~~∿~∿{_HR}\n"
    f"    [bold white]│     ∿ whole-cell simulation platform ∿     │[/]{_HG}∿~~∿~∿~~∿{_HR}\n"
    f"    {_HM}╰────────────────────────────────────────────╯{_HR}"
)


def _animated_banner(phase: float) -> str:
    """Generate banner markup with colors phasing between green and royal purple."""

    def _lerp(t: float) -> str:
        # Green (0, 255, 0) ↔ Royal Purple (120, 81, 169)
        r = int(120 * t)
        g = int(255 - 174 * t)
        b = int(169 * t)
        return f"#{r:02x}{g:02x}{b:02x}"

    c1 = _lerp((math.sin(phase) + 1) / 2)
    c2 = _lerp((math.sin(phase + 0.7) + 1) / 2)
    c3 = _lerp((math.sin(phase + 1.4) + 1) / 2)
    c4 = _lerp((math.sin(phase + 2.1) + 1) / 2)
    c5 = _lerp((math.sin(phase + 2.8) + 1) / 2)

    return (
        f"    [bold {c1}]╭────────────────────────────────────────────╮[/]\n"
        f"    [bold {c2}]│    ▄▀▄ ▀█▀ █   ▄▀▄ █▄ █ ▀█▀ █ ▄▀▀          │[/][bold {c4}]∿~∿~~∿~∿~[/]\n"
        f"    [bold {c3}]│    █▀█  █  █▄▄ █▀█ █ ▀█  █  █ ▄██           │[/][bold {c5}]~∿~∿~~∿~∿[/]\n"
        f"    [bold {c4}]│     ∿ whole-cell simulation platform ∿     │[/][bold {c1}]∿~~∿~∿~~∿[/]\n"
        f"    [bold {c5}]╰────────────────────────────────────────────╯[/]"
    )


SERVER_OPTIONS = [(f"{u.name}  ({u.value})", u.value) for u in BaseUrl]


def _status_color(status: str) -> str:
    s = status.lower()
    if s in ("completed", "complete"):
        return "ansi_green"
    if s in ("running", "active"):
        return "ansi_cyan"
    if s in ("failed", "error"):
        return "ansi_red"
    if s in ("cancelled", "canceled"):
        return "ansi_yellow"
    return "ansi_magenta"


def _json_markup(data: Any) -> Syntax:
    return Syntax(json.dumps(data, indent=2, default=str), "json", theme="native", word_wrap=True)


# ─── Modal Screens ────────────────────────────────────────────────────────────


class IdInputScreen(ModalScreen[int | None]):
    """Modal that prompts for a numeric ID."""

    BINDINGS = [("escape", "cancel", "Cancel")]
    CSS = """
    IdInputScreen {
        align: center middle;
    }
    #modal-container {
        width: 50;
        height: auto;
        max-height: 12;
        border: thick ansi_cyan;
        padding: 1 2;
    }
    #modal-container Input {
        margin-top: 1;
    }
    #modal-container .modal-buttons {
        margin-top: 1;
        height: 3;
    }
    """

    def __init__(self, prompt: str = "Enter ID") -> None:
        super().__init__()
        self.prompt = prompt

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-container"):
            yield Label(self.prompt)
            yield Input(placeholder="numeric ID...", id="modal-input", type="integer")
            with Horizontal(classes="modal-buttons"):
                yield Button("OK", variant="primary", id="modal-ok")
                yield Button("Cancel", id="modal-cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "modal-ok":
            self._submit()
        else:
            self.dismiss(None)

    def on_input_submitted(self, _event: Input.Submitted) -> None:
        self._submit()

    def _submit(self) -> None:
        val = self.query_one("#modal-input", Input).value.strip()
        if val.isdigit():
            self.dismiss(int(val))
        else:
            self.notify("Please enter a valid number", severity="error")

    def action_cancel(self) -> None:
        self.dismiss(None)


class TextInputScreen(ModalScreen[str | None]):
    """Modal that prompts for one line of free text.

    IdInputScreen next door only accepts digits, which is right for a database
    id and wrong for a Job name, an identity or a JSON blob -- the three things
    the worker actions need.
    """

    BINDINGS = [("escape", "cancel", "Cancel")]
    CSS = """
    TextInputScreen {
        align: center middle;
    }
    #modal-container {
        width: 72;
        height: auto;
        max-height: 12;
        border: thick ansi_cyan;
        padding: 1 2;
    }
    #modal-container Input {
        margin-top: 1;
    }
    #modal-container .modal-buttons {
        margin-top: 1;
        height: 3;
    }
    """

    def __init__(self, prompt: str, placeholder: str = "", initial: str = "") -> None:
        super().__init__()
        self.prompt = prompt
        self.placeholder = placeholder
        self.initial = initial

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-container"):
            yield Label(self.prompt)
            yield Input(placeholder=self.placeholder, id="modal-input", value=self.initial)
            with Horizontal(classes="modal-buttons"):
                yield Button("OK", variant="primary", id="modal-ok")
                yield Button("Cancel", id="modal-cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "modal-ok":
            self._submit()
        else:
            self.dismiss(None)

    def on_input_submitted(self, _event: Input.Submitted) -> None:
        self._submit()

    def _submit(self) -> None:
        # Empty is a legitimate answer (clearing an identity), so dismiss with it
        # rather than refusing -- the caller decides what "" means.
        self.dismiss(self.query_one("#modal-input", Input).value.strip())

    def action_cancel(self) -> None:
        self.dismiss(None)


class RunSimulationScreen(ModalScreen[dict[str, Any] | None]):
    """Modal form for submitting a new simulation."""

    BINDINGS = [("escape", "cancel", "Cancel")]
    CSS = """
    RunSimulationScreen {
        align: center middle;
    }
    #run-container {
        width: 70;
        height: auto;
        max-height: 30;
        border: thick ansi_cyan;
        padding: 1 2;
    }
    #run-container Input, #run-container Select {
        margin-top: 1;
    }
    #run-container .run-buttons {
        margin-top: 1;
        height: 3;
    }
    """

    def compose(self) -> ComposeResult:
        from viva_api.common.simulator_defaults import SimulationConfigFilename as SCF

        config_options = [(f"{c.name} ({c.value})", c.value) for c in SCF]

        with Vertical(id="run-container"):
            yield Label("[bold]Submit Simulation[/bold]")
            yield Input(placeholder="Experiment ID", id="run-exp-id")
            yield Input(placeholder="Simulator ID (number)", id="run-sim-id", type="integer")
            yield Label("Config:")
            yield Select(config_options, id="run-config", allow_blank=False)
            with Horizontal():
                with Vertical():
                    yield Label("Generations:")
                    yield Input(value="1", id="run-gens", type="integer")
                with Vertical():
                    yield Label("Seeds:")
                    yield Input(value="3", id="run-seeds", type="integer")
            yield Input(placeholder="Description (optional)", id="run-desc")
            yield Input(placeholder="Observables (comma-sep dot-paths, optional)", id="run-observables")
            yield Input(
                placeholder='Analysis options JSON (e.g. {"multiseed": {"ptools_rna": {"n_tp": 10}}})',
                id="run-analysis-opts",
            )
            with Horizontal(classes="run-buttons"):
                yield Button("Submit", variant="primary", id="run-submit")
                yield Button("Cancel", id="run-cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run-submit":
            self._submit()
        else:
            self.dismiss(None)

    def _submit(self) -> None:
        exp_id = self.query_one("#run-exp-id", Input).value.strip()
        sim_id = self.query_one("#run-sim-id", Input).value.strip()
        if not exp_id or not sim_id.isdigit():
            self.notify("Experiment ID and numeric Simulator ID are required", severity="error")
            return
        config_select = self.query_one("#run-config", Select)
        obs_raw = self.query_one("#run-observables", Input).value.strip()
        obs_list = [o.strip() for o in obs_raw.split(",") if o.strip()] if obs_raw else None
        analysis_raw = self.query_one("#run-analysis-opts", Input).value.strip()
        analysis_opts = None
        if analysis_raw:
            try:
                analysis_opts = json.loads(analysis_raw)
            except json.JSONDecodeError:
                self.notify("Invalid JSON in analysis options", severity="error")
                return
        self.dismiss({
            "experiment_id": exp_id,
            "simulator_id": int(sim_id),
            "config_filename": config_select.value,
            "num_generations": int(self.query_one("#run-gens", Input).value or "1"),
            "num_seeds": int(self.query_one("#run-seeds", Input).value or "3"),
            "description": self.query_one("#run-desc", Input).value.strip() or None,
            "observables": obs_list,
            "analysis_options": analysis_opts,
        })

    def action_cancel(self) -> None:
        self.dismiss(None)


class FileBrowserScreen(ModalScreen[None]):
    """Full-screen file browser with directory tree + content viewer."""

    BINDINGS = [("escape", "close", "Close")]
    CSS = """
    FileBrowserScreen {
        align: center middle;
    }
    #browser-container {
        width: 95%;
        height: 90%;
        border: thick ansi_cyan;
        padding: 0;
    }
    #browser-tree {
        width: 35;
        border-right: solid ansi_bright_black;
    }
    #browser-viewer {
        padding: 0 1;
    }
    #viewer-title {
        height: 1;
        text-style: bold;
        color: ansi_cyan;
        padding: 0 1;
    }
    #viewer-log {
        height: 1fr;
    }
    #viewer-table {
        height: 1fr;
    }
    #browser-status {
        dock: bottom;
        height: 1;
        color: ansi_bright_black;
        padding: 0 1;
    }
    """

    def __init__(self, root_path: str | Path) -> None:
        super().__init__()
        self.root_path = Path(root_path)

    def compose(self) -> ComposeResult:
        with Horizontal(id="browser-container"):
            yield DirectoryTree(str(self.root_path), id="browser-tree")
            with Vertical(id="browser-viewer"):
                yield Label("Select a file to view", id="viewer-title")
                yield RichLog(id="viewer-log", highlight=True, markup=True, wrap=True)
                yield DataTable(id="viewer-table")
        yield Label(
            f"  [dim]{self.root_path}  |  click a file to view  |  ESC to close[/dim]",
            id="browser-status",
        )

    def on_mount(self) -> None:
        # Start with table hidden, log visible
        self.query_one("#viewer-table", DataTable).display = False

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        """Handle file selection — render TSV as table, JSON as highlighted syntax, others as text."""
        file_path = Path(event.path)
        suffix = file_path.suffix.lower()
        title_label = self.query_one("#viewer-title", Label)
        viewer_log = self.query_one("#viewer-log", RichLog)
        viewer_table = self.query_one("#viewer-table", DataTable)

        title_label.update(f"[bold cyan]{file_path.name}[/]  [dim]({file_path.parent})[/dim]")

        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            viewer_log.clear()
            viewer_log.write(f"[red]Could not read file: {e}[/red]")
            viewer_log.display = True
            viewer_table.display = False
            return

        if suffix in (".tsv", ".csv"):
            sep = "\t" if suffix == ".tsv" else ","
            self._render_tabular(content, sep, viewer_table, viewer_log)
        elif suffix == ".json":
            self._render_json(content, viewer_log, viewer_table)
        elif suffix in (".html", ".htm"):
            self._render_html(content, viewer_log, viewer_table)
        else:
            self._render_text(content, viewer_log, viewer_table)

    @staticmethod
    def _render_tabular(content: str, sep: str, table: DataTable[str], log: RichLog) -> None:
        """Parse TSV/CSV and display as a DataTable."""
        log.display = False
        table.display = True
        table.clear(columns=True)

        lines = [line for line in content.splitlines() if line.strip()]
        if not lines:
            log.display = True
            table.display = False
            log.clear()
            log.write("[yellow]Empty file[/yellow]")
            return

        # First line is header
        headers = lines[0].split(sep)
        for h in headers:
            table.add_column(h.strip() or "—")

        for line in lines[1:]:
            cells = line.split(sep)
            while len(cells) < len(headers):
                cells.append("")
            table.add_row(*[c.strip() for c in cells[: len(headers)]])

    @staticmethod
    def _render_json(content: str, log: RichLog, table: DataTable[str]) -> None:
        """Parse and pretty-print JSON with syntax highlighting."""
        table.display = False
        log.display = True
        log.clear()
        try:
            parsed = json.loads(content)
            formatted = json.dumps(parsed, indent=2)
            log.write(Syntax(formatted, "json", theme="native", word_wrap=True))
        except json.JSONDecodeError:
            # Not valid JSON — show as plain text
            log.write(content)

    @staticmethod
    def _render_text(content: str, log: RichLog, table: DataTable[str]) -> None:
        """Show plain text content."""
        table.display = False
        log.display = True
        log.clear()
        log.write(content)

    @staticmethod
    def _render_html(content: str, log: RichLog, table: DataTable[str]) -> None:
        """Render HTML with syntax highlighting."""
        table.display = False
        log.display = True
        log.clear()
        log.write(Syntax(content, "html", theme="native", word_wrap=True, line_numbers=True))

    def action_close(self) -> None:
        self.dismiss(None)


# ─── Main App ─────────────────────────────────────────────────────────────────


class AtlantisTUI(App[None]):
    """Textual app for SMS API."""

    TITLE = "Atlantis"
    SUB_TITLE = "Simulating Microbial Systems"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("d", "toggle_dark", "Toggle Dark"),
    ]

    # Use ANSI colors exclusively so every terminal renders cleanly.
    # No truecolor backgrounds — the terminal's own bg always shows through.
    CSS = """
    #server-bar {
        dock: bottom;
        height: 3;
        padding: 0 2;
        border-top: solid ansi_magenta;
    }
    #server-bar Label {
        padding-top: 1;
        color: ansi_cyan;
        text-style: bold;
    }
    #server-bar Select {
        width: 1fr;
    }

    /* ── Sidebar ── */
    #sidebar {
        width: 22;
        border-right: solid ansi_magenta;
        padding: 1;
    }
    #sidebar Button {
        width: 100%;
    }
    .nav-section-label {
        text-style: bold;
        color: ansi_magenta;
        margin: 1 0 0 0;
    }

    /* ── Animated banner ── */
    #banner {
        height: auto;
        width: 100%;
        padding: 0 1;
    }

    /* ── Main content ── */
    #main-content {
        padding: 1 2;
    }
    #data-table {
        height: 2fr;
        min-height: 6;
        border: round ansi_cyan;
    }
    .action-bar {
        height: auto;
        padding: 1 0 0 0;
    }
    .action-bar Button {
        margin: 0 1 0 0;
    }
    #result-log {
        height: 3fr;
        border: round ansi_cyan;
        margin-top: 1;
    }
    """

    def __init__(self, base_url: BaseUrl | str = BaseUrl.STANFORD_DEV_FORWARDED) -> None:
        super().__init__()
        # textual-ansi uses ONLY the terminal's native 16 ANSI colors — no truecolor.
        # This renders cleanly in every terminal (Terminal.app, iTerm2, PyCharm, etc.)
        # because it inherits the terminal's own color scheme instead of fighting it.
        self.theme = "textual-ansi"
        self.base_url = BaseUrl(base_url) if isinstance(base_url, str) else base_url
        self.svc: E2EDataService = get_data_service(base_url=self.base_url)
        self._active_domain: str = ""
        self._temp_dirs: list[tempfile.TemporaryDirectory[str]] = []
        # Env-worker session state. There is deliberately NO "list every task"
        # endpoint -- a shared deployment's task table is not one user's business
        # -- so the TUI shows what THIS session submitted, and the batch-status
        # endpoint refreshes exactly those.
        self._task_ids: list[int] = []
        self._worker_job: str = ""
        self._identity: str = ""

    def compose(self) -> ComposeResult:
        yield Header()

        with Horizontal():
            with VerticalScroll(id="sidebar"):
                yield Button("Simulations", id="nav-simulations", variant="primary")
                yield Button("Simulators", id="nav-simulators", variant="primary")
                yield Button("Analyses", id="nav-analyses", variant="primary")
                yield Button("Env Workers", id="nav-workers", variant="primary")

                yield Label("UTILITIES", classes="nav-section-label")
                yield Button("Browse Files", id="browse-files")
                yield Button("Demo S3", id="demo-s3", variant="warning")

            with Vertical(id="main-content"):
                yield Static(id="banner")
                yield DataTable(id="data-table", cursor_type="row")

                # Domain action bars (shown/hidden based on nav selection)
                with Horizontal(id="actions-simulations", classes="action-bar"):
                    yield Button("Run New", id="sim-run", variant="success")
                    yield Button("Status", id="sim-status")
                    yield Button("Log", id="sim-log")
                    yield Button("Get by ID", id="sim-get")
                    yield Button("Cancel", id="sim-cancel", variant="error")
                    yield Button("Download", id="sim-outputs")
                    yield Button("Configs", id="sim-configs")
                    yield Button("Analyses", id="sim-analyses-discover")
                    yield Button("Run Analysis", id="sim-analysis")
                with Horizontal(id="actions-simulators", classes="action-bar"):
                    yield Button("Build Latest", id="ver-latest", variant="success")
                    yield Button("Check Build", id="ver-status")
                with Horizontal(id="actions-analyses", classes="action-bar"):
                    yield Button("Get Spec", id="ana-get")
                    yield Button("Status", id="ana-status")
                    yield Button("View Log", id="ana-log")
                    yield Button("Plots", id="ana-plots")
                with Horizontal(id="actions-workers", classes="action-bar"):
                    yield Button("Start Worker", id="wrk-start", variant="success")
                    yield Button("Submit Task", id="wrk-submit", variant="success")
                    yield Button("Refresh", id="wrk-refresh")
                    yield Button("Task Detail", id="wrk-detail")
                    yield Button("Cancel Task", id="wrk-cancel", variant="error")
                    yield Button("Stop Worker", id="wrk-stop", variant="error")
                    yield Button("Identity", id="wrk-identity", variant="warning")

                yield RichLog(id="result-log", highlight=True, markup=True, wrap=True)

        with Horizontal(id="server-bar"):
            yield Label("Server: ", id="server-label")
            yield Select(SERVER_OPTIONS, value=self.base_url.value, id="server-select", allow_blank=False)

        yield Footer()

    def on_mount(self) -> None:
        self._banner_phase = 0.0
        self._animate_banner()
        self.set_interval(0.1, self._animate_banner)
        # Hide all action bars initially
        for domain in ("simulations", "simulators", "analyses", "workers"):
            self.query_one(f"#actions-{domain}").display = False
        self.write_log("[dim]Simulating Microbial Systems — Interactive Terminal[/dim]")
        self.write_log(f"[dim]Server: {self.base_url.name} ({self.base_url.value})[/dim]")
        self.write_log("")
        self.write_log("[bold]Select a domain in the sidebar.  [dim]q[/dim]=quit  [dim]d[/dim]=theme[/bold]\n")

    def _show_domain(self, domain: str) -> None:
        """Show the action bar for *domain* and hide the others."""
        self._active_domain = domain
        for d in ("simulations", "simulators", "analyses", "workers"):
            self.query_one(f"#actions-{d}").display = d == domain

    def _animate_banner(self) -> None:
        self._banner_phase += 0.15
        self.query_one("#banner", Static).update(_animated_banner(self._banner_phase))

    # ── Helpers ───────────────────────────────────────────────────────────

    def write_log(self, msg: str | Text | Syntax) -> None:
        self.query_one("#result-log", RichLog).write(msg)

    def _clear_log(self) -> None:
        self.query_one("#result-log", RichLog).clear()

    def _show_json(self, data: Any, title: str = "") -> None:
        if title:
            self.write_log(f"[bold cyan]{title}[/]")
        self.write_log(_json_markup(data))
        self.write_log("")

    def _populate_table(self, columns: list[str], rows: list[list[str]]) -> None:
        table = self.query_one("#data-table", DataTable)
        table.clear(columns=True)
        for col in columns:
            table.add_column(col)
        for row in rows:
            table.add_row(*row)

    # ── Server change ─────────────────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "server-select" and event.value is not None and isinstance(event.value, str):
            self.base_url = BaseUrl(event.value)
            self.svc = get_data_service(base_url=self.base_url)
            self.write_log(f"[green]Switched to {self.base_url.name} ({self.base_url.value})[/green]\n")

    # ── Button dispatch ───────────────────────────────────────────────────

    def on_button_pressed(self, event: Button.Pressed) -> None:  # noqa: C901
        bid = event.button.id
        # ── Nav buttons (auto-load listing + show domain actions) ──
        if bid == "nav-simulations":
            self._show_domain("simulations")
            self._do_sim_list()
        elif bid == "nav-simulators":
            self._show_domain("simulators")
            self._do_ver_list()
        elif bid == "nav-analyses":
            self._show_domain("analyses")
            self._do_ana_list()
        elif bid == "nav-workers":
            self._show_domain("workers")
            self._do_wrk_refresh()
        # ── Simulation actions ──
        elif bid == "sim-get":
            self._ask_id_then("Simulation ID", self._do_sim_get)
        elif bid == "sim-run":
            self._do_sim_run()
        elif bid == "sim-status":
            self._ask_id_then("Simulation ID", self._do_sim_status)
        elif bid == "sim-log":
            self._ask_id_then("Simulation ID", self._do_sim_log)
        elif bid == "sim-cancel":
            self._ask_id_then("Simulation ID to cancel", self._do_sim_cancel)
        elif bid == "sim-outputs":
            self._ask_id_then("Simulation ID", self._do_sim_outputs)
        elif bid == "sim-configs":
            self._ask_id_then("Simulator ID", self._do_sim_configs)
        elif bid == "sim-analyses-discover":
            self._ask_id_then("Simulator ID", self._do_sim_analyses_discover)
        elif bid == "sim-analysis":
            self._ask_id_then("Simulation ID", self._do_sim_standalone_analysis)
        # ── Simulator actions ──
        elif bid == "ver-status":
            self._ask_id_then("Simulator ID", self._do_ver_status)
        elif bid == "ver-latest":
            self._do_ver_latest()
        # ── Analysis actions ──
        elif bid == "ana-get":
            self._ask_id_then("Analysis ID", self._do_ana_get)
        elif bid == "ana-status":
            self._ask_id_then("Analysis ID", self._do_ana_status)
        elif bid == "ana-log":
            self._ask_id_then("Analysis ID", self._do_ana_log)
        elif bid == "ana-plots":
            self._ask_id_then("Analysis ID", self._do_ana_plots)
        # ── Env worker actions ──
        elif bid == "wrk-start":
            self._ask_text_then("Simulator commit (image tag)", self._do_wrk_start, placeholder="e.g. f78672f")
        elif bid == "wrk-submit":
            self._do_wrk_submit()
        elif bid == "wrk-refresh":
            self._do_wrk_refresh()
        elif bid == "wrk-detail":
            self._ask_id_then("Task ID", self._do_wrk_detail)
        elif bid == "wrk-cancel":
            self._ask_id_then("Task ID to cancel", self._do_wrk_cancel)
        elif bid == "wrk-stop":
            self._do_wrk_stop()
        elif bid == "wrk-identity":
            self._do_wrk_identity()
        # ── Utilities ──
        elif bid == "demo-s3":
            self._do_demo_s3()
        elif bid == "browse-files":
            self._do_browse_files()

    # ── Table row selection (double-click) ──────────────────────────────

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Double-click a completed simulation row → download outputs and open file explorer."""
        if self._active_domain != "simulations":
            return
        table = self.query_one("#data-table", DataTable)
        row_data = table.get_row(event.row_key)
        if not row_data:
            return
        # Status is the last column (index 5)
        status = str(row_data[-1]).strip().upper()
        if status != "COMPLETED":
            self.write_log(
                f"[yellow]Simulation {row_data[0]} is {status} — only COMPLETED simulations can be explored.[/]\n"
            )
            return
        try:
            sim_id = int(row_data[0])
        except (ValueError, IndexError):
            return
        self._do_sim_explore(sim_id)

    @work(thread=True)
    def _do_sim_explore(self, sid: int) -> None:
        """Download outputs to a temp dir and open the file browser."""
        self.write_log(f"[cyan]Downloading outputs for simulation {sid}...[/]")
        try:
            tmp = tempfile.TemporaryDirectory(prefix=f"atlantis_sim{sid}_")
            self._temp_dirs.append(tmp)
            dest_path = Path(tmp.name)
            result = self.svc.get_output_data_sync(simulation_id=sid, dest=dest_path)
            self.write_log("[green]Outputs ready — opening explorer...[/green]\n")
            self.app.call_from_thread(self.push_screen, FileBrowserScreen(result))
        except Exception as e:
            self.write_log(f"[red]Error downloading outputs: {e}[/red]\n")

    # ── ID prompt helper ──────────────────────────────────────────────────

    def _ask_id_then(self, prompt: str, callback: Any) -> None:
        def _on_dismiss(result: int | None) -> None:
            if result is not None:
                callback(result)

        self.push_screen(IdInputScreen(prompt), _on_dismiss)

    def _ask_text_then(self, prompt: str, callback: Any, placeholder: str = "", initial: str = "") -> None:
        def _on_dismiss(result: str | None) -> None:
            if result is not None:
                callback(result)

        self.push_screen(TextInputScreen(prompt, placeholder, initial), _on_dismiss)

    # ── Env workers: the task tier ────────────────────────────────────────
    #
    # Parity with `atlantis worker` (plan §E option (e) step 7). The service
    # layer is shared, so what is here is the presentation of the same calls --
    # notably the ONE distinction the tier exists to draw: a task that FAILED
    # (the job died) versus a task that COMPLETED carrying stage errors (the
    # science failed). Collapsing those two into "error" would throw away the
    # thing this whole arc was built to tell apart.

    def _svc_as_me(self) -> Any:
        """A service carrying this session's identity, if one has been set.

        Rebuilt per call rather than stored, because the identity and the server
        can both change from the UI and a stale client would silently keep using
        the old one.
        """
        return get_data_service(base_url=self.base_url, identity=self._identity or None)

    def _do_wrk_identity(self) -> None:
        def _set(value: str) -> None:
            self._identity = value
            if value:
                self.write_log(f"[green]Identity set:[/green] {value}")
                self.write_log(
                    "[dim]Attribution only — this is not authentication. It lets you cancel "
                    "your own tasks, and stops you cancelling someone else's.[/dim]\n"
                )
            else:
                self.write_log("[yellow]Identity cleared[/yellow] [dim]— tasks will be anonymous[/dim]\n")

        self._ask_text_then(
            "Caller identity (e.g. you@example.com)", _set, placeholder="blank = anonymous", initial=self._identity
        )

    @work(thread=True)
    def _do_wrk_start(self, commit: str) -> None:
        if not commit:
            return
        self._clear_log()
        self.write_log(f"[bold cyan]Starting env worker for {commit}...[/]")
        self.write_log("[dim]This waits for pod scheduling and an image pull; it can take minutes.[/dim]")
        try:
            result = self.svc.worker_start(commit=commit, accept_timeout=420.0)
        except Exception as e:
            self.write_log(f"[red]Could not start worker: {e}[/red]\n")
            return
        self._worker_job = str(result.get("job_name", ""))
        self.write_log(f"[green]CONNECTED[/green]  {self._worker_job}")
        self.write_log(f"[dim]{result.get('image', '')}[/dim]\n")

    def _do_wrk_submit(self) -> None:
        if not self._worker_job:
            self.write_log("[yellow]No worker yet — press 'Start Worker' first.[/yellow]\n")
            return

        def _got_method(method: str) -> None:
            if not method:
                return

            def _got_params(params: str) -> None:
                self._submit_task(method, params)

            self._ask_text_then(
                f"Params for {method} (JSON object, blank for none)",
                _got_params,
                placeholder='{"workspace": "/app/v2ecoli", "study_slug": "..."}',
            )

        self._ask_text_then("Worker method", _got_method, placeholder="run_study", initial="run_study")

    @work(thread=True)
    def _submit_task(self, method: str, params_text: str) -> None:
        parsed: dict[str, Any] = {}
        if params_text:
            try:
                parsed = json.loads(params_text)
            except json.JSONDecodeError as e:
                self.write_log(f"[red]Params are not valid JSON: {e}[/red]\n")
                return
            if not isinstance(parsed, dict):
                self.write_log("[red]Params must be a JSON object[/red] [dim](the worker takes named params)[/dim]\n")
                return
        try:
            task = self._svc_as_me().worker_submit(self._worker_job, method=method, params=parsed)
        except Exception as e:
            self.write_log(f"[red]Submit failed: {e}[/red]\n")
            return
        task_id = int(task.get("task_id", 0))
        self._task_ids.append(task_id)
        self.write_log(f"[green]Queued[/green] task {task_id}  [dim]{method}[/dim]")
        if self._identity and not task.get("created_by"):
            # Same honesty the CLI owes: do not claim an identity the server did
            # not record, or the user finds out at cancel time.
            self.write_log(
                "[yellow]The server did not record that identity[/yellow] [dim]— it has no "
                "IDENTITY_HEADER configured, so this task is anonymous and anyone may cancel it.[/dim]"
            )
        self.write_log("")
        self._refresh_task_table()

    @work(thread=True)
    def _do_wrk_refresh(self) -> None:
        self._refresh_task_table()

    def _refresh_task_table(self) -> None:
        if not self._task_ids:
            self._populate_table(["ID", "Method", "Status", "Result", "Owner"], [])
            self.write_log(
                "[dim]No tasks submitted in this session yet. There is no server-side "
                "listing of everyone's tasks, so this table shows yours.[/dim]\n"
            )
            return
        try:
            rows = self._svc_as_me().worker_tasks(self._task_ids)
        except Exception as e:
            self.write_log(f"[red]Could not read tasks: {e}[/red]\n")
            return
        self._populate_table(
            ["ID", "Method", "Status", "Result", "Owner"],
            [
                [
                    str(r.get("task_id", "")),
                    str(r.get("method", "")),
                    str(r.get("status", "")),
                    "yes" if r.get("has_result") else "-",
                    str(r.get("created_by") or "anonymous"),
                ]
                for r in rows
            ],
        )

    @work(thread=True)
    def _do_wrk_detail(self, task_id: int) -> None:
        try:
            task = self._svc_as_me().worker_task(task_id)
        except Exception as e:
            self.write_log(f"[red]Could not read task {task_id}: {e}[/red]\n")
            return
        status = str(task.get("status", ""))
        color = _status_color(status)
        self.write_log(f"[bold]Task {task_id}[/] — {task.get('method', '')}  [{color}]{status.upper()}[/{color}]")
        self.write_log(f"  [dim]worker:[/dim] {task.get('job_name', '')}")
        self.write_log(f"  [dim]owner:[/dim]  {task.get('created_by') or 'anonymous'}")
        # THE distinction. A dead worker and a study whose variants failed are
        # different events with different fixes, and only one of them is a bug in
        # the caller's science.
        if task.get("error_message"):
            self.write_log(f"[red]The job failed:[/red] {task['error_message']}")
        result = task.get("result")
        if isinstance(result, dict) and result.get("errors"):
            errors = result["errors"]
            self.write_log(f"[yellow]The job completed, but {len(errors)} stage(s) failed:[/yellow]")
            for entry in errors:
                stage = entry.get("stage", "?") if isinstance(entry, dict) else "?"
                message = entry.get("error", "") if isinstance(entry, dict) else str(entry)
                self.write_log(f"  [bold]{stage}[/] {message}")
        if result is not None:
            self._show_json(result, "Result")
        else:
            self.write_log("")

    @work(thread=True)
    def _do_wrk_cancel(self, task_id: int) -> None:
        try:
            self._svc_as_me().worker_cancel(task_id)
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 401:
                self.write_log(
                    "[red]Cancel needs an identity.[/red] [dim]Cancelling destroys someone's work, so "
                    "it is the one operation that asks who you are — press 'Identity'.[/dim]\n"
                )
            elif status == 403:
                self.write_log(
                    f"[red]Task {task_id} belongs to someone else.[/red] "
                    f"[dim]You are '{self._identity}'. You cannot cancel a task you did not start.[/dim]\n"
                )
            else:
                self.write_log(f"[red]Cancel failed: {e}[/red]\n")
            return
        self.write_log(f"[green]Cancelled[/green] task {task_id}\n")
        self._refresh_task_table()

    @work(thread=True)
    def _do_wrk_stop(self) -> None:
        if not self._worker_job:
            self.write_log("[yellow]No worker to stop.[/yellow]\n")
            return
        job = self._worker_job
        try:
            result = self.svc.worker_stop(job)
        except Exception as e:
            self.write_log(f"[red]Stop failed: {e}[/red]\n")
            return
        self._worker_job = ""
        held = " (no live connection held)" if not result.get("was_connected") else ""
        settled = result.get("tasks_settled") or 0
        self.write_log(f"[green]Deleted[/green] {job}{held}")
        if settled:
            # Stopping a worker kills its tasks. Saying how many were settled is
            # the explanation owed to whoever submitted them.
            self.write_log(f"[yellow]{settled} unfinished task(s) were settled as failed.[/yellow]")
        self.write_log("")
        self._refresh_task_table()

    # ── Simulations ───────────────────────────────────────────────────────

    @work(thread=True)
    def _do_sim_list(self) -> None:
        self._clear_log()
        self.write_log("[bold cyan]Loading simulations...[/]")
        try:
            sims = self.svc.show_workflows()
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]")
            return
        if not sims:
            self.write_log("[yellow]No simulations found.[/yellow]\n")
            return
        # Enrich each simulation with its workflow status
        rows: list[list[str]] = []
        for s in sims:
            try:
                run = self.svc.get_workflow_status(simulation_id=s.database_id)
                status = run.status.value.upper()
            except Exception:
                status = "UNKNOWN"
            rows.append([
                str(s.database_id),
                s.experiment_id,
                str(s.simulator_id),
                s.simulation_config_filename,
                str(s.config.generations),
                status,
            ])
        self._populate_table(["ID", "Experiment", "Simulator", "Config", "Gens", "Status"], rows)
        self.write_log(f"[green]Loaded {len(sims)} simulations[/green]\n")

    @work(thread=True)
    def _do_sim_get(self, sid: int) -> None:
        self.write_log(f"[cyan]Fetching simulation {sid}...[/]")
        try:
            sim = self.svc.get_workflow(simulation_id=sid)
            self._show_json(sim.model_dump(), title=f"Simulation {sid}")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_sim_status(self, sid: int) -> None:
        self.write_log(f"[cyan]Checking status for simulation {sid}...[/]")
        try:
            run = self.svc.get_workflow_status(simulation_id=sid)
            status = run.status.value
            color = _status_color(status)
            self.write_log(f"  Status: [{color}]{status.upper()}[/{color}]")
            if run.error_message:
                self.write_log(f"  [red]Error: {run.error_message}[/red]")

            if status in ("completed", "failed", "cancelled"):
                # Terminal state: show simulation details instead of (unavailable) log
                try:
                    sim = self.svc.get_workflow(simulation_id=sid)
                    self._show_json(sim.model_dump(), title=f"Simulation {sid}")
                except Exception as e:
                    self.write_log(f"[dim]Details not available: {e}[/dim]")
                if status == "completed":
                    self.write_log(f"[dim]Download: click 'Download Outputs' with ID {sid}[/dim]\n")
            else:
                # Still running: show live Nextflow log
                try:
                    log = self.svc.get_workflow_log(simulation_id=sid)
                    self.write_log(f"\n[dim]─── Log ───[/dim]\n{log}\n")
                except Exception:
                    self.write_log("[dim]Log not yet available[/dim]\n")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    def _do_sim_run(self) -> None:
        def _on_dismiss(result: dict[str, Any] | None) -> None:
            if result is not None:
                self._submit_sim_run(result)

        self.push_screen(RunSimulationScreen(), _on_dismiss)

    @work(thread=True)
    def _submit_sim_run(self, params: dict[str, Any]) -> None:
        self.write_log("[cyan]Submitting simulation...[/]")
        try:
            sim = self.svc.run_workflow(**params)
            self.write_log("[green]Simulation submitted![/green]")
            self._show_json(sim.model_dump(), title="New Simulation")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_sim_cancel(self, sid: int) -> None:
        self.write_log(f"[yellow]Cancelling simulation {sid}...[/]")
        try:
            result = self.svc.cancel_workflow(simulation_id=sid)
            color = _status_color(result.status.value)
            self.write_log(f"  [{color}]{result.status.value.upper()}[/{color}]\n")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_sim_outputs(self, sid: int) -> None:
        self.write_log(f"[cyan]Downloading outputs for simulation {sid}...[/]")
        try:
            dest_path = Path(f"./simulation_id_{sid}").resolve()
            dest_path.mkdir(parents=True, exist_ok=True)
            result = asyncio.run(self.svc.get_output_data(simulation_id=sid, dest=dest_path))
            self.write_log(f"[green]Saved to: {result}[/green]\n")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_sim_log(self, sid: int) -> None:
        self.write_log(f"[cyan]Fetching full log for simulation {sid}...[/]")
        try:
            log = self.svc.get_workflow_log(simulation_id=sid, truncate=False)
            self.write_log(f"[dim]─── Simulation {sid} Log ───[/dim]\n{log}\n")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_sim_configs(self, simulator_id: int) -> None:
        self.write_log(f"[cyan]Discovering configs for simulator {simulator_id}...[/]")
        try:
            discovery = self.svc.discover_repo(simulator_id=simulator_id)
            repo = f"{discovery.git_repo_url} @ {discovery.git_commit_hash}"
            self.write_log(f"[bold]Config files[/bold] ({repo}):")
            if discovery.config_filenames:
                for name in discovery.config_filenames:
                    self.write_log(f"  {name}")
            else:
                self.write_log("  [dim]No config files found (embedded default will be used)[/dim]")
            self.write_log("")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_sim_analyses_discover(self, simulator_id: int) -> None:
        self.write_log(f"[cyan]Discovering analysis modules for simulator {simulator_id}...[/]")
        try:
            discovery = self.svc.discover_repo(simulator_id=simulator_id)
            repo = f"{discovery.git_repo_url} @ {discovery.git_commit_hash}"
            self.write_log(f"[bold]Analysis modules[/bold] ({repo}):")
            if discovery.analysis_modules:
                for category, modules in sorted(discovery.analysis_modules.items()):
                    self.write_log(f"  [bold magenta]{category}:[/]")
                    for mod in modules:
                        self.write_log(f"    {mod}")
            else:
                self.write_log("  [dim]No analysis modules discovered[/dim]")
            self.write_log("")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_sim_standalone_analysis(self, sid: int) -> None:
        self.write_log(f"[cyan]Running standalone analysis on simulation {sid}...[/]")
        try:
            result = self.svc.run_analysis(simulation_id=sid)
            self.write_log("[green]Analysis submitted![/green]")
            self._show_json(result, title=f"Standalone Analysis — Simulation {sid}")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    # ── Simulators ────────────────────────────────────────────────────────

    @work(thread=True)
    def _do_ver_list(self) -> None:
        self._clear_log()
        self.write_log("[cyan]Loading simulator versions...[/]")
        try:
            versions = self.svc.show_simulators()
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]")
            return
        if not versions:
            self.write_log("[yellow]No simulators found.[/yellow]\n")
            return
        # Enrich each simulator with its build status
        rows: list[list[str]] = []
        for sv in versions:
            try:
                status = self.svc.get_simulator_status(simulator_id=sv.database_id).upper()
            except Exception:
                status = "UNKNOWN"
            rows.append([
                str(sv.database_id),
                sv.git_commit_hash[:12],
                sv.git_branch,
                sv.git_repo_url.rsplit("/", 1)[-1] if "/" in sv.git_repo_url else sv.git_repo_url,
                str(sv.created_at)[:19] if sv.created_at else "-",
                status,
            ])
        self._populate_table(["ID", "Commit", "Branch", "Repo", "Created", "Status"], rows)
        self.write_log(f"[green]Loaded {len(versions)} simulators[/green]\n")

    @work(thread=True)
    def _do_ver_status(self, sid: int) -> None:
        self.write_log(f"[cyan]Checking build status for simulator {sid}...[/]")
        try:
            status = self.svc.get_simulator_status(simulator_id=sid)
            color = _status_color(status)
            self.write_log(f"  Build status: [{color}]{status.upper()}[/{color}]\n")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_ver_latest(self) -> None:
        self.write_log("[cyan]Building latest simulator (this may take a while)...[/]")
        try:
            sv = self.svc.get_simulator()
            self.write_log("[green]Simulator ready![/green]")
            self._show_json(sv.model_dump(), title="Built Simulator")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    # ── Parca ─────────────────────────────────────────────────────────────

    @work(thread=True)
    def _do_parca_list(self) -> None:
        self.write_log("[cyan]Loading parca datasets...[/]")
        try:
            datasets = self.svc.get_parca_datasets()
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]")
            return
        if not datasets:
            self.write_log("[yellow]No parca datasets found.[/yellow]\n")
            return
        self._populate_table(
            ["ID", "Simulator ID", "Archive Path"],
            [
                [
                    str(ds.database_id),
                    str(ds.parca_dataset_request.simulator_version.database_id),
                    ds.remote_archive_path or "-",
                ]
                for ds in datasets
            ],
        )
        self.write_log(f"[green]Loaded {len(datasets)} datasets (see Table tab)[/green]\n")

    @work(thread=True)
    def _do_parca_status(self, pid: int) -> None:
        self.write_log(f"[cyan]Checking parca status for {pid}...[/]")
        try:
            hpc_run = self.svc.get_parca_status(parca_id=pid)
            self._show_json(hpc_run.model_dump(), title=f"Parca Run {pid}")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    # ── Analyses ──────────────────────────────────────────────────────────

    @work(thread=True)
    def _do_ana_list(self) -> None:
        self._clear_log()
        self.write_log("[cyan]Analyses — use the action buttons to inspect by ID.[/]")
        self.write_log("[dim]No listing endpoint available; enter an analysis ID below.[/dim]\n")
        # Clear the table since there's no list endpoint
        table = self.query_one("#data-table", DataTable)
        table.clear(columns=True)

    @work(thread=True)
    def _do_ana_get(self, aid: int) -> None:
        self.write_log(f"[cyan]Fetching analysis {aid}...[/]")
        try:
            analysis = self.svc.get_analysis(analysis_id=aid)
            self._show_json(analysis.model_dump(), title=f"Analysis {aid}")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_ana_status(self, aid: int) -> None:
        self.write_log(f"[cyan]Checking analysis status for {aid}...[/]")
        try:
            status = self.svc.get_analysis_status(analysis_id=aid)
            color = _status_color(status.status.value)
            self.write_log(f"  Analysis {aid}: [{color}]{status.status.value.upper()}[/{color}]")
            if status.error_log:
                self.write_log(f"[red]Error log:[/red]\n{status.error_log}")
            self.write_log("")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_ana_log(self, aid: int) -> None:
        self.write_log(f"[cyan]Fetching analysis log for {aid}...[/]")
        try:
            log = self.svc.get_analysis_log(analysis_id=aid)
            self.write_log(f"[dim]─── Analysis {aid} Log ───[/dim]\n{log}\n")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    @work(thread=True)
    def _do_ana_plots(self, aid: int) -> None:
        self.write_log(f"[cyan]Fetching plots for analysis {aid}...[/]")
        try:
            plots = self.svc.get_analysis_plots(analysis_id=aid)
            if not plots:
                self.write_log("[yellow]No plots found.[/yellow]\n")
                return
            for p in plots:
                self.write_log(f"  [magenta]{p.name}[/magenta]")
                preview = p.content[:200].replace("\n", " ")
                if len(p.content) > 200:
                    preview += "..."
                self.write_log(f"  [dim]{preview}[/dim]")
            self.write_log("")
        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")

    # ── Demo S3 Download ──────────────────────────────────────────────────

    @work(thread=True)
    def _do_demo_s3(self) -> None:
        self.write_log("[bold cyan]Demo: Download S3 Data[/]")

        test_outdir = os.environ.get("TEST_BUCKET_EXPERIMENT_OUTDIR", "")
        if not test_outdir:
            self.write_log("[red]TEST_BUCKET_EXPERIMENT_OUTDIR is not set.[/red]")
            self.write_log("[dim]Set it in assets/dev/config/.dev_env or environment.[/dim]\n")
            return

        from urllib.parse import urlparse

        experiment_id = urlparse(test_outdir).path.strip("/").rsplit("/", 1)[-1]
        self.write_log(f"  Experiment: [bold]{experiment_id}[/bold]")
        self.write_log(f"  [dim]Source: {test_outdir}[/dim]")

        from viva_api.common.storage.file_service_s3 import FileServiceS3
        from viva_api.config import get_settings
        from viva_api.dependencies import get_file_service, set_file_service

        settings = get_settings()
        if not settings.storage_s3_bucket or not settings.storage_s3_region:
            self.write_log("[red]S3 settings not configured.[/red]\n")
            return

        self.write_log(f"  [dim]Bucket: {settings.storage_s3_bucket}  Region: {settings.storage_s3_region}[/dim]")
        self.write_log("[cyan]Downloading...[/]")

        saved_fs = get_file_service()
        fs = FileServiceS3()
        set_file_service(fs)

        try:
            from viva_api.common.handlers.simulations import _download_outputs_from_s3

            dest_path = Path("./demo_outputs").resolve()
            local_cache = dest_path / experiment_id
            local_cache.mkdir(parents=True, exist_ok=True)

            asyncio.run(_download_outputs_from_s3(experiment_id, local_cache))

            real_files = [f for f in local_cache.rglob("*") if f.is_file()]
            if not real_files:
                self.write_log("[red]No files downloaded.[/red]\n")
                return

            import tarfile

            tsv_count = sum(1 for f in real_files if f.suffix == ".tsv")
            json_count = sum(1 for f in real_files if f.suffix == ".json")
            archive_path = dest_path / f"{experiment_id}.tar.gz"
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(str(local_cache), arcname=experiment_id)

            self.write_log("[green]Download complete![/green]")
            self.write_log(f"  {tsv_count} .tsv files, {json_count} .json files, {len(real_files)} total")
            self.write_log(f"  [green]Files:[/green]   {local_cache}")
            self.write_log(f"  [green]Archive:[/green] {archive_path}")
            self.write_log("[bold]Opening file browser...[/bold]\n")
            self.call_from_thread(self.push_screen, FileBrowserScreen(local_cache))

        except Exception as e:
            self.write_log(f"[red]Error: {e}[/red]\n")
        finally:
            asyncio.run(fs.close())
            set_file_service(saved_fs)

    # ── File Browser ──────────────────────────────────────────────────────

    def _do_browse_files(self) -> None:
        """Open file browser rooted at ./demo_outputs or cwd."""
        demo_path = Path("./demo_outputs").resolve()
        root = demo_path if demo_path.is_dir() else Path.cwd()
        self.push_screen(FileBrowserScreen(root))

    # ── Actions ───────────────────────────────────────────────────────────

    def action_toggle_dark(self) -> None:
        # Cycle: textual-ansi → textual-dark → textual-light → textual-ansi
        cycle = ["textual-ansi", "textual-dark", "textual-light"]
        idx = cycle.index(self.theme) if self.theme in cycle else 0
        self.theme = cycle[(idx + 1) % len(cycle)]
        self.write_log(f"[dim]Theme: {self.theme}[/dim]")

    def on_unmount(self) -> None:
        """Clean up temporary directories created for simulation output browsing."""
        import contextlib

        for tmp in self._temp_dirs:
            with contextlib.suppress(Exception):
                tmp.cleanup()
        self._temp_dirs.clear()


# ── Direct invocation ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else BaseUrl.STANFORD_DEV_FORWARDED
    app = AtlantisTUI(base_url=base)
    app.run()
