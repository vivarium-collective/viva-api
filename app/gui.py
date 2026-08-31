import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium", layout_file="layouts/gui.grid.json")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import json
    import time
    import traceback
    from pathlib import Path

    return Path, json, time, traceback


@app.cell
def _(mo):
    # DAW-inspired color palette (dark theme, neon accents)
    # Sourced from uqEcoli/app/dashboard.py
    MAGENTA = "#ff3366"
    CYAN = "#00f0ff"
    YELLOW = "#ffaa00"
    GREEN = "#33ff99"
    RED = "#ff3131"
    CORAL = "#ff6f61"
    PURPLE = "#aa66ff"
    TEAL = "#00bfa5"
    BG_DARK = "#0d0d0d"
    BG_CARD = "#1a1a2e"
    BORDER = "#2a2a4a"
    TEXT_LIGHT = "#e0e0e0"
    TEXT_DIM = "#888"

    # DAW-inspired stylesheet (from uqEcoli/app/dashboard.py)
    _memphis_css = mo.Html(f"""<style>
    :root {{
        --daw-bg: {BG_DARK};
        --daw-panel: {BG_CARD};
        --daw-border: {BORDER};
        --daw-text: {TEXT_LIGHT};
        --daw-dim: {TEXT_DIM};
        --daw-accent1: {CYAN};
        --daw-accent2: {MAGENTA};
        --daw-accent3: {GREEN};
        --daw-accent4: {YELLOW};
    }}
    .memphis-banner {{
        background: linear-gradient(90deg, {MAGENTA}, {CYAN}, {YELLOW}, {MAGENTA});
        background-size: 300% 100%;
        animation: memphis-scroll 8s linear infinite;
        height: 4px;
        border-radius: 2px;
        margin: 0 0 1rem 0;
    }}
    @keyframes memphis-scroll {{
        0% {{ background-position: 0% 50%; }}
        100% {{ background-position: 300% 50%; }}
    }}
    .memphis-title {{
        font-family: JetBrains Mono, SF Mono, Fira Code, monospace;
        font-weight: 900;
        font-size: 1.6rem;
        background: linear-gradient(90deg, {CYAN}, {MAGENTA});
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: 0.08em;
    }}
    .memphis-subtitle {{
        font-family: JetBrains Mono, SF Mono, Fira Code, monospace;
        font-size: 0.75rem;
        color: {TEXT_DIM};
        letter-spacing: 0.15em;
        text-transform: uppercase;
    }}
    .memphis-status-completed {{ color: {GREEN}; font-weight: bold; }}
    .memphis-status-running {{ color: {YELLOW}; font-weight: bold; }}
    .memphis-status-failed {{ color: {RED}; font-weight: bold; }}
    .memphis-status-pending {{ color: {CYAN}; font-weight: bold; }}
    .memphis-status-cancelled {{ color: {CORAL}; font-weight: bold; }}
    .memphis-status-unknown {{ color: {PURPLE}; font-weight: bold; }}

    /* ── DAW-style panel cards ─────────────────────────────── */
    .tb-card {{
        background: {BG_CARD};
        border: 1px solid {BORDER};
        border-radius: 8px;
        padding: 0;
        margin: 0.4rem 0;
        overflow: hidden;
    }}
    .tb-card:hover {{
        border-color: {CYAN}60;
    }}
    .tb-card-header {{
        padding: 0.5rem 1rem;
        font-family: JetBrains Mono, SF Mono, Fira Code, monospace;
        font-weight: 700;
        font-size: 0.85rem;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        color: {TEXT_LIGHT};
        border-bottom: 1px solid {BORDER};
        background: {BG_DARK};
    }}
    .tb-card-header-magenta {{ color: {MAGENTA}; }}
    .tb-card-header-cyan {{ color: {CYAN}; }}
    .tb-card-header-green {{ color: {GREEN}; }}
    .tb-card-header-purple {{ color: {PURPLE}; }}
    .tb-card-header-yellow {{ color: {YELLOW}; }}
    .tb-card-body {{
        padding: 0.8rem 1rem 1rem 1rem;
    }}
    .tb-card-footer {{
        padding: 0.4rem 1rem;
        border-top: 1px solid {BORDER};
    }}
    </style>""")
    _memphis_css

    # DAW accent color map (matches uqEcoli channel strip colors)
    _ACCENT = {
        "green": GREEN,
        "cyan": CYAN,
        "magenta": MAGENTA,
        "purple": PURPLE,
        "yellow": YELLOW,
        "red": RED,
    }

    def card(title: str, icon: str, body: str, color: str = "green") -> str:
        """Wrap HTML content in a DAW-style panel (matches uqEcoli render_panel)."""
        accent = _ACCENT.get(color, CYAN)
        return (
            f'<div style="background:{BG_CARD}; border:1px solid {BORDER}; border-left:3px solid {accent}; '
            f'border-radius:8px; padding:0; overflow:hidden;">'
            f'<div style="padding:10px 16px; border-bottom:1px solid {BORDER};">'
            f'<span style="color:{accent}; font-family:JetBrains Mono, SF Mono, Fira Code, monospace; '
            f'font-size:12px; font-weight:700; letter-spacing:2px; text-transform:uppercase;">'
            f"{icon} {title}</span></div>"
            f'<div style="padding:12px 16px;">{body}</div>'
            f"</div>"
        )

    def card_wrap(title: str, icon: str, *children, color: str = "green", action=None):
        """Wrap marimo renderables in a DAW-style panel card."""
        accent = _ACCENT.get(color, CYAN)
        children_html = "".join(c.text if hasattr(c, "text") else str(c) for c in children)
        footer = ""
        if action is not None:
            action_html = action.text if hasattr(action, "text") else str(action)
            footer = f'<div style="padding:6px 16px; border-top:1px solid {BORDER};">{action_html}</div>'
        return mo.Html(
            f'<div style="background:{BG_CARD}; border:1px solid {BORDER}; border-left:3px solid {accent}; '
            f'border-radius:8px; padding:0; overflow:hidden;">'
            f'<div style="padding:10px 16px; border-bottom:1px solid {BORDER};">'
            f'<span style="color:{accent}; font-family:JetBrains Mono, SF Mono, Fira Code, monospace; '
            f'font-size:12px; font-weight:700; letter-spacing:2px; text-transform:uppercase;">'
            f"{icon} {title}</span></div>"
            f'<div style="padding:12px 16px;">{children_html}</div>'
            f"{footer}"
            f"</div>"
        )

    return CYAN, MAGENTA, card, card_wrap


@app.cell
def _(mo):
    # Thematic iconify icons — bio-sci Memphis Atlantis device aesthetic
    ICO_DNA = mo.icon("twemoji:dna", size=18)
    ICO_DNA_SM = mo.icon("twemoji:dna", size=14)
    ICO_MICROBE = mo.icon("twemoji:microbe", size=18)
    ICO_MICROSCOPE = mo.icon("twemoji:microscope", size=20)
    ICO_TEST_TUBE = mo.icon("twemoji:test-tube", size=20)
    ICO_PETRI_DISH = mo.icon("twemoji:petri-dish", size=20)
    ICO_BAR_CHART = mo.icon("twemoji:bar-chart", size=20)
    ICO_FILE_FOLDER = mo.icon("twemoji:open-file-folder", size=20)
    ICO_GEAR = mo.icon("twemoji:gear", size=16)
    ICO_CHECK = mo.icon("twemoji:check-mark-button", size=16)
    ICO_CROSS = mo.icon("twemoji:cross-mark", size=16)
    ICO_ROCKET = mo.icon("twemoji:rocket", size=16)
    ICO_HOURGLASS = mo.icon("twemoji:hourglass-not-done", size=16)
    ICO_STOP = mo.icon("twemoji:stop-sign", size=16)
    ICO_DOWN_ARROW = mo.icon("twemoji:down-arrow", size=16)
    ICO_LINK = mo.icon("twemoji:link", size=14)
    return (
        ICO_CHECK,
        ICO_CROSS,
        ICO_DNA_SM,
        ICO_DOWN_ARROW,
        ICO_GEAR,
        ICO_HOURGLASS,
        ICO_LINK,
        ICO_MICROBE,
        ICO_ROCKET,
        ICO_STOP,
    )


@app.cell
def _(ICO_CHECK, ICO_CROSS, ICO_HOURGLASS, ICO_ROCKET, ICO_STOP, mo):
    def status_badge(status: str) -> "mo.Html":
        """Return an HTML badge with a thematic icon for a given job status."""
        _css_class = f"memphis-status-{status}" if status else "memphis-status-unknown"
        _icon = {
            "completed": ICO_CHECK.text,
            "running": ICO_ROCKET.text,
            "failed": ICO_CROSS.text,
            "pending": ICO_HOURGLASS.text,
            "cancelled": ICO_STOP.text,
        }.get(status, "")
        return mo.Html(f'{_icon} <span class="{_css_class}">{(status or "unknown").upper()}</span>')

    return (status_badge,)


@app.cell
def _(CYAN, ICO_DNA_SM, ICO_MICROBE, MAGENTA, mo):
    _dna = ICO_DNA_SM.text
    _microbe = ICO_MICROBE.text

    # E. coli rod-cell shape: rounded capsule with flagella trailing right.
    # Uses CSS border-radius (pill shape) + wavy tail via SVG path.
    _banner = mo.Html(f"""
    <div style="text-align: center; margin: 0.5rem 0;">
      <div style="display: inline-flex; align-items: center; gap: 0;">
        <!-- Rod-cell body (rounded capsule) -->
        <div style="
          border: 2px solid {MAGENTA};
          border-radius: 50px;
          padding: 0.8rem 2.2rem;
          background: linear-gradient(135deg, rgba(233,30,144,0.08), rgba(0,229,255,0.05));
          text-align: center;
          min-width: 340px;
        ">
          <div style="
            font-family: 'Courier New', monospace;
            font-weight: 900;
            font-size: 1.4rem;
            letter-spacing: 0.15em;
            background: linear-gradient(90deg, {MAGENTA}, {CYAN});
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
          ">
            {_dna} ATLANTIS {_dna}
          </div>
          <div style="
            font-family: 'Courier New', monospace;
            font-size: 0.7rem;
            color: {CYAN};
            letter-spacing: 0.1em;
            margin-top: 0.2rem;
          ">
            whole-cell simulation platform
          </div>
        </div>
        <!-- Flagella tail (SVG wavy lines) -->
        <svg width="90" height="60" viewBox="0 0 90 60" style="margin-left: -2px;">
          <path d="M0,30 C15,15 25,45 40,30 S60,15 75,30 S85,42 90,35"
                stroke="{MAGENTA}" fill="none" stroke-width="2" opacity="0.7"/>
          <path d="M0,25 C12,10 22,40 35,25 S55,10 70,25 S82,38 88,30"
                stroke="{CYAN}" fill="none" stroke-width="1.5" opacity="0.5"/>
          <path d="M0,35 C18,20 28,50 45,35 S65,20 80,35 S88,48 90,40"
                stroke="#ffd700" fill="none" stroke-width="1.5" opacity="0.5"/>
        </svg>
      </div>
      <p class="memphis-subtitle" style="margin-top: 0.5rem;">
        {_dna} sms-api {_microbe}
      </p>
    </div>
    <div class="memphis-banner" style="margin-top: 0.4rem;"></div>
    """)
    _banner
    return


@app.cell
def _():
    return


@app.cell
def _(ICO_GEAR, mo):
    from app.app_data_service import BaseUrl

    # Derive the dropdown directly from BaseUrl so CLI/TUI/GUI all expose the
    # same set of deployment targets (e.g. RKE_PROD / CCAM).
    _base_url_options = {f"{u.name} ({u.value})": u.value for u in BaseUrl}
    base_url_dropdown = mo.ui.dropdown(
        options=_base_url_options,
        value=f"{BaseUrl.RKE_PROD.name} ({BaseUrl.RKE_PROD.value})",
        label=f"{ICO_GEAR.text} API base URL",
    )
    base_url_dropdown
    return (base_url_dropdown,)


@app.cell
def _(base_url_dropdown):
    from app.app_data_service import E2EDataService

    _svc = E2EDataService(base_url=base_url_dropdown.value, timeout=600)

    def get_svc() -> E2EDataService:
        return _svc

    return (get_svc,)


@app.cell(hide_code=True)
def _():
    return


@app.cell
def _(mo):
    get_built_sim_id, set_built_sim_id = mo.state(0)
    get_running_sim_id, set_running_sim_id = mo.state(0)
    return (
        get_built_sim_id,
        get_running_sim_id,
        set_built_sim_id,
        set_running_sim_id,
    )


@app.cell
def _(mo):
    # ── Build inputs (embedded in simulators table panel) ──
    repo_url_input = mo.ui.dropdown(
        options={
            "Public (CovertLab/vEcoli)": "https://github.com/CovertLab/vEcoli",
            "Private (vEcoli-private)": "https://github.com/CovertLabEcoli/vEcoli-private",
            "Fork (vivarium-collective/vEcoli)": "https://github.com/vivarium-collective/vEcoli",
        },
        value="Private (vEcoli-private)",
        label="Repository",
    )
    branch_input = mo.ui.text(value="master", label="Branch")
    force_rebuild = mo.ui.checkbox(label="Force rebuild", value=False)
    build_button = mo.ui.run_button(label=f"{mo.icon('gravity-ui:function')} Build", kind="success")
    sim_status_button = mo.ui.run_button(label="Check Status")

    build_form = mo.vstack([
        mo.hstack([repo_url_input, branch_input, force_rebuild], justify="start", gap=1),
        mo.hstack([build_button, sim_status_button], justify="start", gap=1),
    ])
    return (
        branch_input,
        build_button,
        build_form,
        force_rebuild,
        repo_url_input,
        sim_status_button,
    )


@app.cell
def _(
    ICO_DNA_SM,
    ICO_LINK,
    branch_input,
    build_button,
    card,
    force_rebuild,
    get_svc,
    json,
    mo,
    repo_url_input,
    set_built_sim_id,
    status_badge,
    time,
    traceback,
):
    # Reactive: only runs when build_button is clicked
    _sim_output = mo.Html("")

    if build_button.value:
        _svc = get_svc()
        try:
            # Step 1: fetch latest
            _latest = _svc.submit_get_latest_simulator(
                repo_url=repo_url_input.value or None,
                branch=branch_input.value or None,
            )
            # Step 2: upload
            _uploaded = _svc.submit_upload_simulator(simulator=_latest, force=force_rebuild.value)
            set_built_sim_id(_uploaded.database_id)

            # Step 3: poll build status
            _status = "pending"
            _polls = []
            while _status not in ("completed", "failed", "cancelled"):
                time.sleep(5)
                _status = _svc.submit_get_simulator_build_status(simulator=_uploaded)
                _polls.append(_status)

            _details = json.dumps(_uploaded.model_dump(), indent=2, default=str)
            _badge = status_badge(_status)
            _dna = ICO_DNA_SM.text
            _link = ICO_LINK.text
            _sim_output = mo.Html(
                card(
                    "Build Result",
                    _dna,
                    f"<strong>Simulator ID:</strong> {_uploaded.database_id}<br>"
                    f"<strong>Status:</strong> {_badge.text}<br>"
                    f"{_link} <strong>Commit:</strong> {_uploaded.git_commit_hash}<br>"
                    f"<pre style='font-size:0.75rem; max-height:200px; overflow:auto;'>{_details}</pre>",
                    color="green",
                )
            )
        except Exception:
            _sim_output = mo.Html(
                card(
                    "Error",
                    "\u26a0\ufe0f",
                    f"<span class='memphis-status-failed'>ERROR</span><br>"
                    f"<pre style='font-size:0.75rem;'>{traceback.format_exc()}</pre>",
                    color="magenta",
                )
            )

    _sim_output
    return


@app.cell
def _(mo):
    get_selected_sim_id, set_selected_sim_id = mo.state(1)
    return get_selected_sim_id, set_selected_sim_id


@app.cell
def _(build_form, get_svc, mo, set_selected_sim_id):
    sims_table = mo.Html("")
    _svc = get_svc()
    try:
        _sims = _svc.show_simulators()
        _rows = [s.model_dump() for s in _sims]
        if _rows:
            _table = mo.ui.table(
                data=_rows,
                selection="single",
                on_change=lambda rows: set_selected_sim_id(int(rows[0]["database_id"]) if rows else 1),
            )
        else:
            _table = mo.Html("<em>No simulators found.</em>")
    except Exception as e:
        _table = mo.Html(f"<span class='memphis-status-failed'>Error: {e}</span>")

    # + button expands build form inline
    _plus_btn = (
        '<details style="display:inline;"><summary style="display:inline-block; cursor:pointer; '
        "background:#33ff99; color:#0d0d0d; border:none; border-radius:50%; "
        "width:24px; height:24px; text-align:center; line-height:24px; "
        'font-size:16px; font-weight:bold; list-style:none;">+</summary>'
        f'<div style="margin-top:8px; padding:10px; border-top:1px solid #2a2a4a;">{build_form}</div>'
        "</details>"
    )

    sims_table = mo.Html(
        '<div style="background:#1a1a2e; border:1px solid #2a2a4a; border-left:3px solid #00f0ff; '
        'border-radius:8px; padding:0; overflow:hidden;">'
        '<div style="padding:10px 16px; border-bottom:1px solid #2a2a4a; '
        'display:flex; justify-content:space-between; align-items:center;">'
        '<span style="color:#00f0ff; font-family:JetBrains Mono, SF Mono, Fira Code, monospace; '
        'font-size:12px; font-weight:700; letter-spacing:2px; text-transform:uppercase;">'
        f"{mo.icon('gravity-ui:function')} Simulators</span>"
        f"{_plus_btn}</div>"
        f'<div style="padding:12px 16px;">{_table}</div>'
        "</div>"
    )
    sims_table
    return (sims_table,)


@app.cell
def _(get_selected_sim_id, get_svc):
    """Discover configs and analysis modules when simulator selection changes."""
    import json as _json

    discovered_configs = {"api_simulation_default.json": "api_simulation_default.json"}
    discovered_analysis_opts = {}

    _sid = get_selected_sim_id()
    if _sid and _sid > 0:
        try:
            _discovery = get_svc().discover_repo(simulator_id=_sid)
            if _discovery.config_filenames:
                discovered_configs = {c: c for c in _discovery.config_filenames}
            if _discovery.analysis_modules:
                for _cat, _mods in _discovery.analysis_modules.items():
                    for _m in _mods:
                        discovered_analysis_opts[f"{_cat}/{_m}"] = _json.dumps({_cat: {_m: {}}})
        except Exception:
            pass
    return discovered_analysis_opts, discovered_configs


@app.cell
def _(
    card,
    get_built_sim_id,
    get_svc,
    json,
    mo,
    sim_status_button,
    status_badge,
    traceback,
):
    _build_status_output = mo.Html("")
    if sim_status_button.value:
        _sid = get_built_sim_id()
        if not _sid or _sid == 0:
            _build_status_output = mo.Html(
                card(
                    "Build Status", "\U0001f9ec", "<em>No simulator built yet — click Build first.</em>", color="yellow"
                )
            )
        else:
            _svc = get_svc()
            try:
                _hpcrun = _svc.submit_get_simulator_build_status_full(simulator_id=_sid)
                _s = _hpcrun.status.value if _hpcrun.status else "unknown"
                _badge = status_badge(_s)
                _details = json.dumps(_hpcrun.model_dump(), indent=2, default=str)
                _err = (
                    f"<br><span class='memphis-status-failed'>Error: {_hpcrun.error_message}</span>"
                    if _hpcrun.error_message
                    else ""
                )
                _build_status_output = mo.Html(
                    card(
                        "Build Status",
                        "\U0001f9ec",
                        f"<strong>Status:</strong> {_badge.text}{_err}<br>"
                        f"<pre style='font-size:0.75rem; max-height:200px; overflow:auto;'>{_details}</pre>",
                        color="cyan",
                    )
                )
            except Exception:
                _build_status_output = mo.Html(
                    card(
                        "Error",
                        "\u26a0\ufe0f",
                        f"<span class='memphis-status-failed'>ERROR</span><br>"
                        f"<pre style='font-size:0.75rem;'>{traceback.format_exc()}</pre>",
                        color="magenta",
                    )
                )
    _build_status_output
    return


@app.cell
def _(
    card_wrap,
    discovered_analysis_opts,
    discovered_configs,
    get_selected_sim_id,
    mo,
    sims_table,
):
    # ── Run inputs ──
    exp_id_input = mo.ui.text(value="", label="Experiment ID", full_width=True)
    sim_id_input = mo.ui.number(label="Simulator ID", start=1, stop=99999, value=get_selected_sim_id())
    _cfg = discovered_configs if discovered_configs else {"api_simulation_default.json": "api_simulation_default.json"}
    config_dropdown = mo.ui.dropdown(
        options=_cfg,
        value=list(_cfg.keys())[0],
        label="Config (discovered)",
    )
    analysis_picker = (
        mo.ui.multiselect(
            options=discovered_analysis_opts,
            label="Analysis modules (select to include)",
        )
        if discovered_analysis_opts
        else None
    )
    gens_input = mo.ui.number(label="Generations", start=1, stop=40, step=1, value=1)
    seeds_input = mo.ui.number(label="Seeds", start=1, stop=100, step=1, value=1)
    run_parca_checkbox = mo.ui.checkbox(label="ParCa", value=False)
    description_input = mo.ui.text(value="", label="Description (optional)", full_width=True)
    observables_input = mo.ui.text(value="", label="Observables (comma-sep dot-paths, optional)", full_width=True)
    run_sim_button = mo.ui.run_button(label=f"{mo.icon('hugeicons:ai-dna')} Submit", kind="success")

    # ── Actions (shared sim ID) ──
    action_sim_id = mo.ui.number(label="Simulation ID", start=1, stop=99999, value=1)
    poll_sim_button = mo.ui.run_button(label="Status")
    cancel_button = mo.ui.run_button(label="Cancel", kind="danger")
    dl_dest = mo.ui.text(value="./debug", label="Dest", full_width=False)
    dl_button = mo.ui.run_button(label="\U0001f4e6 Download", kind="success")
    list_workflows_button = mo.ui.run_button(label="\U0001f4cb List")

    _div = '<div style="border-top:1px solid rgba(255,255,255,0.08); margin:0.6rem 0;"></div>'

    def _lbl(text: str) -> str:
        return (
            f'<div style="color:#888; font-size:11px; text-transform:uppercase; '
            f"letter-spacing:2px; margin-bottom:0.3rem; "
            f'font-family:JetBrains Mono, SF Mono, Fira Code, monospace;">{text}</div>'
        )

    simulation_card = card_wrap(
        "Simulation",
        "\U0001f52c",
        mo.vstack([
            mo.Html(_lbl("Submit Workflow")),
            mo.hstack([exp_id_input], justify="start"),
            mo.hstack(
                [
                    sims_table,
                    mo.vstack([config_dropdown, gens_input, seeds_input, run_parca_checkbox], justify="start"),
                ],
                justify="start",
                gap=1,
            ),
            description_input,
            observables_input,
            analysis_picker if analysis_picker is not None else mo.Html(""),
            run_sim_button,
            mo.Html(_div + _lbl("Actions")),
            mo.hstack(
                [action_sim_id, poll_sim_button, dl_dest, dl_button, cancel_button, list_workflows_button],
                justify="start",
                gap=1,
            ),
        ]),
        color="green",
    )
    simulation_card
    return (
        action_sim_id,
        analysis_picker,
        cancel_button,
        config_dropdown,
        description_input,
        dl_button,
        dl_dest,
        exp_id_input,
        gens_input,
        list_workflows_button,
        observables_input,
        poll_sim_button,
        run_parca_checkbox,
        run_sim_button,
        seeds_input,
        sim_id_input,
        simulation_card,
    )


@app.cell
def _(
    ICO_DNA_SM,
    ICO_ROCKET,
    analysis_picker,
    card,
    config_dropdown,
    description_input,
    exp_id_input,
    gens_input,
    get_svc,
    json,
    mo,
    observables_input,
    run_parca_checkbox,
    run_sim_button,
    seeds_input,
    set_running_sim_id,
    sim_id_input,
    traceback,
):
    _run_output = mo.Html("")
    if run_sim_button.value:
        _svc = get_svc()
        _exp_id = exp_id_input.value.strip()
        if not _exp_id:
            _run_output = mo.Html("<span class='memphis-status-failed'>Experiment ID is required.</span>")
        else:
            try:
                _desc = description_input.value.strip() or (
                    f"sim{int(sim_id_input.value)}-{_exp_id}; "
                    f"{int(gens_input.value)} Generations; {int(seeds_input.value)} Seeds"
                )
                _obs_raw = observables_input.value.strip()
                _obs_list = [o.strip() for o in _obs_raw.split(",") if o.strip()] if _obs_raw else None
                # Build analysis_options from picker selections
                _ao_parsed = None
                if analysis_picker is not None and hasattr(analysis_picker, "value") and analysis_picker.value:
                    _ao_parsed = {}
                    for _v in analysis_picker.value:
                        _picked = json.loads(_v)
                        for _cat, _mods in _picked.items():
                            _ao_parsed.setdefault(_cat, {}).update(_mods)
                _simulation = _svc.run_workflow(
                    experiment_id=_exp_id,
                    simulator_id=int(sim_id_input.value),
                    config_filename=config_dropdown.value,
                    num_generations=int(gens_input.value),
                    num_seeds=int(seeds_input.value),
                    description=_desc,
                    run_parameter_calculator=run_parca_checkbox.value,
                    observables=_obs_list,
                    analysis_options=_ao_parsed,
                )
                set_running_sim_id(_simulation.database_id)
                _details = json.dumps(_simulation.model_dump(), indent=2, default=str)
                _dna = ICO_DNA_SM.text
                _rocket = ICO_ROCKET.text
                _run_output = mo.Html(
                    card(
                        "Simulation Submitted",
                        _rocket,
                        f"<span class='memphis-status-completed'>SUBMITTED</span><br>"
                        f"{_dna} <strong>Simulation ID:</strong> {_simulation.database_id}<br>"
                        f"{_dna} <strong>Experiment:</strong> {_simulation.experiment_id}<br>"
                        f"<pre style='font-size:0.75rem; max-height:200px; overflow:auto;'>{_details}</pre>",
                        color="green",
                    )
                )
            except Exception:
                _run_output = mo.Html(
                    card(
                        "Error",
                        "\u26a0\ufe0f",
                        f"<span class='memphis-status-failed'>ERROR</span><br>"
                        f"<pre style='font-size:0.75rem;'>{traceback.format_exc()}</pre>",
                        color="magenta",
                    )
                )
    run_output = _run_output
    return (run_output,)


@app.cell
def _(
    action_sim_id,
    card,
    get_svc,
    mo,
    poll_sim_button,
    set_running_sim_id,
    status_badge,
    traceback,
):
    _poll_output = mo.Html("")
    if poll_sim_button.value:
        _svc = get_svc()
        _sid = int(action_sim_id.value)
        try:
            _run = _svc.get_workflow_status(simulation_id=_sid)
            _status = _run.status.value
            _badge = status_badge(_status)
            _err = (
                f"<br><span class='memphis-status-failed'>Error: {_run.error_message}</span>"
                if _run.error_message
                else ""
            )
            try:
                _log = _svc.get_workflow_log(simulation_id=_sid, truncate=True)
                _log_html = (
                    f"<pre style='font-size:0.7rem; max-height:300px; overflow:auto; "
                    f"background:#0d1117; padding:0.5rem; border-radius:6px; color:#c9d1d9;'>{_log}</pre>"
                )
            except Exception:
                _log_html = "<em style='color:#666;'>Log not yet available</em>"

            _poll_output = mo.Html(
                card(
                    f"Simulation {_sid}",
                    "\U0001f52c",
                    f"<strong>Status:</strong> {_badge.text}{_err}<br>{_log_html}",
                    color="cyan",
                )
            )
            # Start auto-refresh for this sim if it's still running
            if _status in ("running", "pending"):
                set_running_sim_id(_sid)
        except Exception:
            _poll_output = mo.Html(
                card(
                    "Error",
                    "\u26a0\ufe0f",
                    f"<span class='memphis-status-failed'>ERROR</span><br>"
                    f"<pre style='font-size:0.75rem;'>{traceback.format_exc()}</pre>",
                    color="magenta",
                )
            )
    _poll_output
    return


@app.cell
def _(mo):
    status_refresh = mo.ui.refresh(default_interval="30s", label="Auto-refresh")
    return (status_refresh,)


@app.cell
def _(
    card,
    get_running_sim_id,
    get_svc,
    mo,
    set_running_sim_id,
    status_badge,
    status_refresh,
    traceback,
):
    """Auto-refresh status for the most recently submitted simulation."""
    _auto_status = mo.Html("")
    _ = status_refresh.value  # subscribe to refresh ticks
    _sid = get_running_sim_id()
    if _sid and _sid > 0:
        try:
            _svc = get_svc()
            _run = _svc.get_workflow_status(simulation_id=_sid)
            _status = _run.status.value
            _badge = status_badge(_status)
            _err = (
                f"<br><span class='memphis-status-failed'>Error: {_run.error_message}</span>"
                if _run.error_message
                else ""
            )
            try:
                _log = _svc.get_workflow_log(simulation_id=_sid, truncate=True)
                _log_html = (
                    f"<pre style='font-size:0.7rem; max-height:300px; overflow:auto; "
                    f"background:#0d1117; padding:0.5rem; border-radius:6px; color:#c9d1d9;'>{_log}</pre>"
                )
            except Exception:
                _log_html = ""
            _auto_status = mo.Html(
                card(
                    f"Simulation {_sid} (live)",
                    "\U0001f4e1",
                    f"<strong>Status:</strong> {_badge.text}{_err}<br>{_log_html}",
                    color="cyan",
                )
            )
            # Stop refreshing once terminal
            if _status in ("completed", "failed", "cancelled"):
                set_running_sim_id(0)
        except Exception:
            _auto_status = mo.Html(
                card(
                    "Status Error",
                    "\u26a0\ufe0f",
                    f"<pre style='font-size:0.75rem;'>{traceback.format_exc()}</pre>",
                    color="magenta",
                )
            )
    log_status = _auto_status
    _auto_status
    return (log_status,)


@app.cell
def _(log_status, mo, run_output, simulation_card, status_refresh):
    _right_col = mo.vstack([status_refresh, log_status, run_output])
    simulations_component = mo.hstack([simulation_card, _right_col], widths=[3, 2], align="start")
    return (simulations_component,)


@app.cell
def _(get_svc, list_workflows_button, mo):
    _wf_table = mo.Html("")
    if list_workflows_button.value:
        _svc = get_svc()
        try:
            _workflows = _svc.show_workflows()
            _rows = [w.model_dump() for w in _workflows]
            if _rows:
                _wf_table = mo.ui.table(data=_rows, label="Simulations")
            else:
                _wf_table = mo.Html("<em>No simulations found.</em>")
        except Exception as e:
            _wf_table = mo.Html(f"<span class='memphis-status-failed'>Error: {e}</span>")
    _wf_table
    return


@app.cell
def _(
    action_sim_id,
    cancel_button,
    card,
    get_svc,
    json,
    mo,
    status_badge,
    traceback,
):
    _cancel_output = mo.Html("")
    if cancel_button.value:
        _svc = get_svc()
        try:
            _result = _svc.cancel_workflow(simulation_id=int(action_sim_id.value))
            _badge = status_badge(_result.status.value)
            _cancel_output = mo.Html(
                card(
                    "Cancel Result",
                    "\U0001f6d1",
                    f"<strong>Status:</strong> {_badge.text}<br>"
                    f"<pre style='font-size:0.75rem;'>{json.dumps(_result.model_dump(), indent=2, default=str)}</pre>",
                    color="yellow",
                )
            )
        except Exception:
            _cancel_output = mo.Html(
                card(
                    "Error",
                    "\u26a0\ufe0f",
                    f"<span class='memphis-status-failed'>ERROR</span><br>"
                    f"<pre style='font-size:0.75rem;'>{traceback.format_exc()}</pre>",
                    color="magenta",
                )
            )
    _cancel_output
    return


@app.cell
def _(
    ICO_DNA_SM,
    ICO_DOWN_ARROW,
    Path,
    action_sim_id,
    card,
    dl_button,
    dl_dest,
    get_svc,
    mo,
    traceback,
):
    import asyncio as _asyncio

    _dl_output = mo.Html("")
    if dl_button.value:
        _svc = get_svc()
        _sid = int(action_sim_id.value)
        _dest = Path(dl_dest.value.strip() or f"simulation_id_{_sid}")
        try:
            _extracted = _asyncio.run(_svc.get_output_data(simulation_id=_sid, dest=_dest))
            # List downloaded files
            _files = sorted(str(f.relative_to(_extracted)) for f in _extracted.rglob("*") if f.is_file())
            _file_count = len(_files)
            _file_list = "\n".join(_files[:50])
            _truncated = f"\n... and {_file_count - 50} more" if _file_count > 50 else ""
            _dna = ICO_DNA_SM.text
            _arrow = ICO_DOWN_ARROW.text
            _dl_output = mo.Html(
                card(
                    "Download Complete",
                    _arrow,
                    f"<span class='memphis-status-completed'>DONE</span><br>"
                    f"{_dna} <strong>Extracted to:</strong> {_extracted}<br>"
                    f"{_dna} <strong>Files:</strong> {_file_count}<br>"
                    f"<pre style='font-size:0.7rem; max-height:300px; overflow:auto;'>{_file_list}{_truncated}</pre>",
                    color="green",
                )
            )
        except Exception:
            _dl_output = mo.Html(
                card(
                    "Error",
                    "\u26a0\ufe0f",
                    f"<span class='memphis-status-failed'>ERROR</span><br>"
                    f"<pre style='font-size:0.75rem;'>{traceback.format_exc()}</pre>",
                    color="magenta",
                )
            )
    _dl_output
    return


@app.cell
def _(CYAN, ICO_DNA_SM, ICO_MICROBE, MAGENTA, mo):
    _dna = ICO_DNA_SM.text
    _microbe = ICO_MICROBE.text
    mo.Html(
        '<div class="memphis-banner" style="margin-top:2rem;"></div>'
        f'<p style="text-align:center; font-size:0.7rem; font-family: monospace; '
        f'color: {CYAN};">'
        f"{_dna} simulating {_dna} microbial {_dna} systems {_dna}"
        f"</p>"
        f'<p style="text-align:center; font-size:0.6rem; color: {MAGENTA};">'
        f"{_microbe} team stanford \u2014 whole-cell E. coli platform \u2014 UCONN CCAM {_microbe}</p>"
    )
    return


@app.cell
def _():
    # mo.ui.tabs({
    #     f"{mo.icon('gravity-ui:function')} Simulator": simulator_card,
    #     f"{mo.icon('healthicons:biomarker-24px')} Simulation": simulation_card,
    # })
    return


@app.cell
def _(mo):
    # ── Env workers: the task tier (plan §E option (e) step 7) ──────────────
    #
    # Parity with `atlantis worker` and the TUI's Env Workers panel. The service
    # layer is shared, so what differs is only the medium; the substance kept
    # identical across all three is the distinction the tier exists to draw --
    # a task that FAILED (the job died) versus one that COMPLETED carrying stage
    # errors (the science failed).
    wrk_identity = mo.ui.text(label="Identity", placeholder="you@example.com — blank = anonymous", full_width=False)
    wrk_commit = mo.ui.text(value="f78672f", label="Commit", full_width=False)
    wrk_start_button = mo.ui.run_button(label="\u25b6 Start worker", kind="success")
    wrk_job = mo.ui.text(label="Job", placeholder="filled in by Start, or paste one", full_width=True)
    wrk_method = mo.ui.text(value="run_study", label="Method", full_width=False)
    wrk_params = mo.ui.text_area(
        value='{"workspace": "/app/v2ecoli", "study_slug": "true-partial-probe", "steps": 3}',
        label="Params (JSON object)",
        rows=3,
    )
    wrk_submit_button = mo.ui.run_button(label="\u2b06 Submit task", kind="success")
    wrk_task_id = mo.ui.number(label="Task ID", start=1, stop=999999, value=1)
    wrk_detail_button = mo.ui.run_button(label="\U0001f50e Detail")
    wrk_cancel_button = mo.ui.run_button(label="\u2716 Cancel task", kind="danger")
    wrk_stop_button = mo.ui.run_button(label="\u23f9 Stop worker", kind="danger")
    return (
        wrk_cancel_button,
        wrk_commit,
        wrk_detail_button,
        wrk_identity,
        wrk_job,
        wrk_method,
        wrk_params,
        wrk_start_button,
        wrk_stop_button,
        wrk_submit_button,
        wrk_task_id,
    )


@app.cell
def _(
    mo,
    wrk_cancel_button,
    wrk_commit,
    wrk_detail_button,
    wrk_identity,
    wrk_job,
    wrk_method,
    wrk_params,
    wrk_start_button,
    wrk_stop_button,
    wrk_submit_button,
    wrk_task_id,
):
    wrk_panel = mo.vstack([
        mo.md("### Env workers"),
        mo.md(
            "_Identity is **attribution, not authentication**. It lets you cancel your own "
            "tasks and stops you cancelling someone else's; where the server has no "
            "`IDENTITY_HEADER` configured it is ignored and everything stays anonymous._"
        ),
        mo.hstack([wrk_identity, wrk_commit, wrk_start_button], justify="start"),
        wrk_job,
        mo.hstack([wrk_method, wrk_submit_button], justify="start"),
        wrk_params,
        mo.hstack([wrk_task_id, wrk_detail_button, wrk_cancel_button, wrk_stop_button], justify="start"),
    ])
    wrk_panel
    return


@app.cell
def _(base_url_dropdown, wrk_identity):
    from app.app_data_service import get_data_service as _get_data_service

    def get_worker_svc():
        """A service carrying the panel's identity.

        Rebuilt on every change rather than cached: both the identity and the
        base URL are live UI inputs, and a stale client would quietly keep using
        the previous one -- which for identity means submitting as the wrong
        person.
        """
        return _get_data_service(
            base_url=base_url_dropdown.value, timeout=600, identity=wrk_identity.value.strip() or None
        )

    return (get_worker_svc,)


@app.cell
def _(card, get_worker_svc, mo, traceback, wrk_commit, wrk_start_button):
    _start_out = mo.Html("")
    if wrk_start_button.value:
        try:
            _res = get_worker_svc().worker_start(commit=wrk_commit.value.strip(), accept_timeout=420.0)
            _start_out = mo.Html(
                card(
                    "Worker connected",
                    "\U0001f50c",
                    f"<strong>Job:</strong> <code>{_res.get('job_name', '')}</code><br>"
                    f"<span style='opacity:0.7'>{_res.get('image', '')}</span><br><br>"
                    "<em>Copy the job name into the Job field above.</em>",
                    color="green",
                )
            )
        except Exception:
            _start_out = mo.Html(
                card("Start failed", "\u26a0\ufe0f", f"<pre>{traceback.format_exc()}</pre>", color="magenta")
            )
    _start_out
    return


@app.cell
def _(card, get_worker_svc, json, mo, traceback, wrk_job, wrk_method, wrk_params, wrk_submit_button):
    _submit_out = mo.Html("")
    if wrk_submit_button.value:
        try:
            _raw = wrk_params.value.strip()
            _parsed = json.loads(_raw) if _raw else {}
            # A JSON array or scalar parses fine and then means nothing to the
            # worker, which takes NAMED params -- so reject it here with a
            # message about the shape rather than letting it 422 downstream.
            if isinstance(_parsed, dict):
                _task = get_worker_svc().worker_submit(
                    wrk_job.value.strip(), method=wrk_method.value.strip(), params=_parsed
                )
            else:
                _task = {"task_id": None, "method": wrk_method.value.strip(), "created_by": None}
            # Do not claim an identity the server did not record -- the user
            # would only find out at cancel time, when their own task refuses
            # them. Same warning the CLI and TUI give.
            _warn = ""
            if _task.get("task_id") is None:
                _warn = (
                    "<br><span class='memphis-status-failed'>Not submitted</span> &mdash; params must be a "
                    "JSON <em>object</em>; the worker takes named params."
                )
            elif not _task.get("created_by"):
                _warn = (
                    "<br><span class='memphis-status-failed'>Recorded as anonymous</span> "
                    "&mdash; the server has no <code>IDENTITY_HEADER</code> configured, so anyone may cancel this."
                )
            _submit_out = mo.Html(
                card(
                    "Task queued",
                    "\u2b06",
                    f"<strong>Task {_task.get('task_id')}</strong> &mdash; {_task.get('method', '')}<br>"
                    f"<strong>Owner:</strong> {_task.get('created_by') or 'anonymous'}{_warn}",
                    color="green",
                )
            )
        except Exception:
            _submit_out = mo.Html(
                card("Submit failed", "\u26a0\ufe0f", f"<pre>{traceback.format_exc()}</pre>", color="magenta")
            )
    _submit_out
    return


@app.cell
def _(card, get_worker_svc, json, mo, traceback, wrk_detail_button, wrk_task_id):
    _detail_out = mo.Html("")
    if wrk_detail_button.value:
        try:
            _t = get_worker_svc().worker_task(int(wrk_task_id.value))
            # THE distinction this whole tier exists for. A dead worker and a
            # study whose variants failed are different events with different
            # fixes; only one is a fault in the caller's science.
            _lines = [
                f"<strong>Task {_t.get('task_id')}</strong> &mdash; {_t.get('method', '')} "
                f"&mdash; <strong>{str(_t.get('status', '')).upper()}</strong>",
                f"<strong>Worker:</strong> <code>{_t.get('job_name', '')}</code>",
                f"<strong>Owner:</strong> {_t.get('created_by') or 'anonymous'}",
            ]
            if _t.get("error_message"):
                _lines.append(f"<span class='memphis-status-failed'>The job failed:</span> {_t['error_message']}")
            _result = _t.get("result")
            if isinstance(_result, dict) and _result.get("errors"):
                _errs = _result["errors"]
                _lines.append(f"<strong>The job completed, but {len(_errs)} stage(s) failed:</strong>")
                for _e in _errs:
                    _stage = _e.get("stage", "?") if isinstance(_e, dict) else "?"
                    _msg = _e.get("error", "") if isinstance(_e, dict) else str(_e)
                    _lines.append(f"&nbsp;&nbsp;<code>{_stage}</code> {_msg}")
            # The verdict is the one thing here a scientist might act on.
            _v = _result.get("verdict") if isinstance(_result, dict) else None
            _inc = _v.get("evidence_incomplete") if isinstance(_v, dict) else None
            if _inc:
                _lines.append(
                    f"<span class='memphis-status-failed'>Verdict not graded</span> &mdash; was "
                    f"<code>{_inc.get('overall_before')}</code>, but "
                    f"{len(_inc.get('failed_stages') or [])} stage(s) of this run did not happen. "
                    "The card on disk is unchanged."
                )
            if _result is not None:
                _lines.append(
                    f"<pre style='font-size:0.7rem; max-height:22rem; overflow:auto;'>"
                    f"{json.dumps(_result, indent=2, default=str)}</pre>"
                )
            _detail_out = mo.Html(card("Task detail", "\U0001f50e", "<br>".join(_lines), color="cyan"))
        except Exception:
            _detail_out = mo.Html(
                card("Read failed", "\u26a0\ufe0f", f"<pre>{traceback.format_exc()}</pre>", color="magenta")
            )
    _detail_out
    return


@app.cell
def _(card, get_worker_svc, mo, traceback, wrk_cancel_button, wrk_identity, wrk_task_id):
    _cancel_out = mo.Html("")
    if wrk_cancel_button.value:
        _tid = int(wrk_task_id.value)
        try:
            _res = get_worker_svc().worker_cancel(_tid)
            _cancel_out = mo.Html(
                card(
                    "Task cancelled",
                    "\u2716",
                    f"<strong>Task {_tid}</strong> &mdash; {_res.get('status', '')}",
                    color="yellow",
                )
            )
        except Exception as _e:
            # 401 and 403 mean different things and the fix differs, so say which
            # rather than dumping a traceback the user has to decode.
            _status = getattr(getattr(_e, "response", None), "status_code", None)
            if _status == 401:
                _body = (
                    "Cancelling destroys someone's work, so it is the one operation that asks who you are.<br>"
                    "Put an address in the <strong>Identity</strong> field above."
                )
            elif _status == 403:
                _body = (
                    f"That task belongs to someone else. You are "
                    f"<code>{wrk_identity.value.strip() or 'anonymous'}</code>.<br>"
                    "You cannot cancel a task you did not start."
                )
            else:
                _body = f"<pre>{traceback.format_exc()}</pre>"
            _cancel_out = mo.Html(card("Cancel refused", "\u26a0\ufe0f", _body, color="magenta"))
    _cancel_out
    return


@app.cell
def _(card, get_worker_svc, mo, traceback, wrk_job, wrk_stop_button):
    _stop_out = mo.Html("")
    if wrk_stop_button.value:
        try:
            _res = get_worker_svc().worker_stop(wrk_job.value.strip())
            _settled = _res.get("tasks_settled") or 0
            # Stopping a worker kills its tasks. How many were settled is the
            # explanation owed to whoever submitted them.
            _extra = f"<br><strong>{_settled}</strong> unfinished task(s) were settled as failed." if _settled else ""
            _stop_out = mo.Html(
                card("Worker stopped", "\u23f9", f"<code>{wrk_job.value.strip()}</code>{_extra}", color="yellow")
            )
        except Exception:
            _stop_out = mo.Html(
                card("Stop failed", "\u26a0\ufe0f", f"<pre>{traceback.format_exc()}</pre>", color="magenta")
            )
    _stop_out
    return


@app.cell
def _(simulations_component):
    simulations_component
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
