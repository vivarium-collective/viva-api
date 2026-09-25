import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium", css_file="style.css")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import json
    import os
    import time

    return json, os, time


@app.cell
def _(mo):
    # Memphis DAW color palette (matches app/ui/dashboard.py theme)
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

    _ACCENT = {
        "green": GREEN,
        "cyan": CYAN,
        "magenta": MAGENTA,
        "purple": PURPLE,
        "yellow": YELLOW,
        "red": RED,
        "coral": CORAL,
        "teal": TEAL,
    }

    # Force dark mode + inject Memphis CSS
    _dark_mode_css = mo.Html(f"""<style>
    html, body {{
        background: {BG_DARK} !important;
        color: {TEXT_LIGHT} !important;
    }}
    .marimo-output {{
        background: {BG_DARK} !important;
    }}
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
        color-scheme: dark;
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
    .composer-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1rem;
    }}
    @media (max-width: 768px) {{
        .composer-grid {{ grid-template-columns: 1fr; }}
    }}
    </style>""")
    _dark_mode_css  # noqa: B018

    def card(title, icon, body, color="cyan"):
        accent = _ACCENT.get(color, CYAN)
        return (
            f'<div style="background:{BG_CARD}; border:1px solid {BORDER}; border-left:3px solid {accent}; '
            f'border-radius:8px; padding:0; overflow:hidden; margin-bottom:0.8rem;">'
            f'<div style="padding:10px 16px; border-bottom:1px solid {BORDER}; background:{BG_DARK};">'
            f'<span style="color:{accent}; font-family:JetBrains Mono, SF Mono, Fira Code, monospace; '
            f'font-size:12px; font-weight:700; letter-spacing:2px; text-transform:uppercase;">'
            f"{icon} {title}</span></div>"
            f'<div style="padding:12px 16px; color:{TEXT_LIGHT};">{body}</div>'
            f"</div>"
        )

    def status_color(status):
        s = (status or "unknown").lower()
        return {
            "completed": GREEN,
            "running": YELLOW,
            "failed": RED,
            "pending": CYAN,
            "cancelled": CORAL,
        }.get(s, PURPLE)

    return (
        MAGENTA,
        CYAN,
        YELLOW,
        GREEN,
        RED,
        CORAL,
        PURPLE,
        TEAL,
        BG_DARK,
        BG_CARD,
        BORDER,
        TEXT_LIGHT,
        TEXT_DIM,
        _ACCENT,
        card,
        status_color,
    )


@app.cell
def _(mo, MAGENTA, CYAN, TEXT_DIM):
    # Header
    mo.output.replace(
        mo.Html(f"""
        <div class="memphis-banner"></div>
        <div style="text-align:center; margin-bottom:1.5rem;">
            <div style="font-family:JetBrains Mono, SF Mono, Fira Code, monospace;
                        font-weight:900; font-size:2rem;
                        background:linear-gradient(90deg, {CYAN}, {MAGENTA});
                        -webkit-background-clip:text; -webkit-text-fill-color:transparent;
                        letter-spacing:0.08em;">
                COMPOSER
            </div>
            <div style="font-family:JetBrains Mono, SF Mono, Fira Code, monospace;
                        font-size:0.7rem; color:{TEXT_DIM}; letter-spacing:0.2em;
                        text-transform:uppercase; margin-top:0.3rem;">
                v2ecoli &bull; process-bigraph &bull; colony simulation builder
            </div>
        </div>
        <div class="memphis-banner"></div>
        """)
    )

    return


@app.cell
def _(mo, os):
    # API base URL
    _default_url = os.environ.get("MARIMO_API_SERVER", "https://sms.cam.uchc.edu")
    api_url = mo.ui.text(
        value=_default_url,
        label="API Base URL",
        full_width=True,
    )
    api_url  # noqa: B018

    return (api_url,)


@app.cell
def _(mo, card):
    # Simulation parameters
    mo.output.replace(
        mo.Html(
            card(
                "Simulation Parameters",
                "&#9881;",
                '<span style="font-size:0.8rem; opacity:0.7;">'
                "Configure the v2ecoli whole-cell simulation below.</span>",
                color="cyan",
            )
        )
    )
    return


@app.cell
def _(mo):
    duration = mo.ui.slider(
        start=10,
        stop=600,
        step=10,
        value=60,
        label="Duration (seconds of biological time)",
        full_width=True,
    )
    seed = mo.ui.number(
        start=0,
        stop=9999,
        value=0,
        label="Random seed",
    )
    interval = mo.ui.slider(
        start=0.5,
        stop=10.0,
        step=0.5,
        value=1.0,
        label="Timestep interval (seconds)",
        full_width=True,
    )
    mo.vstack([duration, mo.hstack([seed, interval], justify="start", gap=1)])

    return duration, seed, interval


@app.cell
def _(mo):
    # Feature modules
    feat_ppgpp = mo.ui.switch(value=False, label="ppGpp regulation")
    feat_supercoiling = mo.ui.switch(value=False, label="DNA supercoiling")
    feat_trna = mo.ui.switch(value=False, label="tRNA attenuation")

    mo.hstack(
        [feat_ppgpp, feat_supercoiling, feat_trna],
        justify="start",
        gap=1.5,
    )

    return feat_ppgpp, feat_supercoiling, feat_trna


@app.cell
def _(mo, card, GREEN, YELLOW, PURPLE, TEXT_DIM):
    # Colony configuration
    mo.output.replace(
        mo.Html(
            card(
                "Colony Configuration",
                "&#127981;",
                f'<span style="font-size:0.8rem; opacity:0.7;">'
                f"Define the colony: how many cells to simulate in parallel with different seeds.</span>"
                f'<div style="margin-top:0.5rem; font-size:0.72rem; color:{TEXT_DIM};">'
                f"Each cell runs as a separate v2ecoli simulation with a unique seed. "
                f"Colony behavior emerges from population-level analysis of the results.</div>",
                color="purple",
            )
        )
    )
    return


@app.cell
def _(mo):
    num_cells = mo.ui.slider(
        start=1,
        stop=32,
        step=1,
        value=4,
        label="Number of cells in colony",
        full_width=True,
    )
    num_cells  # noqa: B018

    return (num_cells,)


@app.cell
def _(
    mo,
    json,
    card,
    duration,
    seed,
    interval,
    feat_ppgpp,
    feat_supercoiling,
    feat_trna,
    num_cells,
    CYAN,
    BG_DARK,
    BORDER,
    TEXT_LIGHT,
):
    # Build features list from toggles
    features = []
    if feat_ppgpp.value:
        features.append("ppgpp_regulation")
    if feat_supercoiling.value:
        features.append("supercoiling")
    if feat_trna.value:
        features.append("trna_attenuation")

    # Build colony spec
    colony_spec = {
        "cells": num_cells.value,
        "duration": duration.value,
        "interval": interval.value,
        "base_seed": seed.value,
        "features": features,
        "seeds": list(range(seed.value, seed.value + num_cells.value)),
    }

    spec_json = json.dumps(colony_spec, indent=2)

    mo.output.replace(
        mo.Html(
            card(
                "Colony Specification",
                "&#128196;",
                f'<pre style="background:{BG_DARK}; border:1px solid {BORDER}; border-radius:6px; '
                f"padding:0.7rem; font-size:0.75rem; color:{TEXT_LIGHT}; overflow-x:auto; "
                f'line-height:1.5;">{spec_json}</pre>',
                color="yellow",
            )
        )
    )

    return features, colony_spec


@app.cell
def _(mo):
    submit_btn = mo.ui.run_button(label="Submit Colony Simulation")
    submit_btn  # noqa: B018

    return (submit_btn,)


@app.cell
def _(
    mo,
    submit_btn,
    colony_spec,
    api_url,
    json,
    time,
    card,
    status_color,
    GREEN,
    RED,
    YELLOW,
    BG_DARK,
    BORDER,
    TEXT_LIGHT,
    TEXT_DIM,
):
    mo.stop(not submit_btn.value, "")

    import httpx

    base = api_url.value.rstrip("/")
    results = []
    errors = []

    for cell_seed in colony_spec["seeds"]:
        try:
            resp = httpx.post(
                f"{base}/compose/v1/curated/ecoli",
                params={
                    "duration": colony_spec["duration"],
                    "seed": cell_seed,
                    "interval": colony_spec["interval"],
                    "features": json.dumps(colony_spec["features"]),
                    "cache_dir": "/out/cache",
                },
                timeout=30.0,
            )
            resp.raise_for_status()
            data = resp.json()
            results.append({"seed": cell_seed, "sim_id": data["simulation_database_id"], "status": "submitted"})
        except Exception as e:
            errors.append({"seed": cell_seed, "error": str(e)})

    # Build results display
    rows = ""
    for r in results:
        rows += (
            f"<tr>"
            f'<td style="padding:6px 12px; border-bottom:1px solid {BORDER};">{r["seed"]}</td>'
            f'<td style="padding:6px 12px; border-bottom:1px solid {BORDER}; '
            f'color:{GREEN}; font-weight:bold;">{r["sim_id"]}</td>'
            f'<td style="padding:6px 12px; border-bottom:1px solid {BORDER}; color:{YELLOW};">submitted</td>'
            f"</tr>"
        )
    for e in errors:
        rows += (
            f"<tr>"
            f'<td style="padding:6px 12px; border-bottom:1px solid {BORDER};">{e["seed"]}</td>'
            f'<td style="padding:6px 12px; border-bottom:1px solid {BORDER}; '
            f'color:{RED};" colspan="2">{e["error"][:80]}</td>'
            f"</tr>"
        )

    table_html = (
        f'<table style="width:100%; border-collapse:collapse; font-size:0.8rem;">'
        f'<tr style="color:{TEXT_DIM}; text-transform:uppercase; font-size:0.7rem; letter-spacing:1px;">'
        f'<th style="padding:8px 12px; border-bottom:1px solid {BORDER}; text-align:left;">Seed</th>'
        f'<th style="padding:8px 12px; border-bottom:1px solid {BORDER}; text-align:left;">Sim ID</th>'
        f'<th style="padding:8px 12px; border-bottom:1px solid {BORDER}; text-align:left;">Status</th>'
        f"</tr>{rows}</table>"
    )

    summary = f"{len(results)} submitted, {len(errors)} failed" if errors else f"{len(results)} cells submitted"
    mo.output.replace(
        mo.Html(
            card(
                f"Colony Submitted &mdash; {summary}",
                "&#128640;",
                table_html,
                color="green" if not errors else "red",
            )
        )
    )

    return results, errors


@app.cell
def _(mo, card, TEXT_DIM, CYAN, MAGENTA, GREEN, PURPLE, YELLOW, BG_CARD, BORDER):
    # Process-bigraph architecture info panel
    processes = [
        ("Equilibrium", "cyan"),
        ("Two-Component System", "cyan"),
        ("RNA Maturation", "cyan"),
        ("TF Binding", "magenta"),
        ("TF Unbinding", "magenta"),
        ("Complexation", "green"),
        ("Protein Degradation", "green"),
        ("Transcript Initiation", "purple"),
        ("Transcript Elongation", "purple"),
        ("Polypeptide Initiation", "yellow"),
        ("Polypeptide Elongation", "yellow"),
        ("Chromosome Replication", "cyan"),
        ("Chromosome Structure", "cyan"),
        ("RNA Degradation", "magenta"),
        ("Metabolism", "green"),
        ("Division", "purple"),
    ]

    pills = ""
    accent_map = {"cyan": CYAN, "magenta": MAGENTA, "green": GREEN, "purple": PURPLE, "yellow": YELLOW}
    for name, color in processes:
        c = accent_map.get(color, CYAN)
        pills += (
            f'<span style="display:inline-block; padding:3px 10px; margin:3px; '
            f"border:1px solid {c}40; border-radius:12px; font-size:0.7rem; "
            f'color:{c}; background:{c}10;">{name}</span>'
        )

    mo.output.replace(
        mo.Html(
            card(
                "v2ecoli Biological Processes",
                "&#129516;",
                f'<div style="margin-bottom:0.5rem; font-size:0.78rem; color:{TEXT_DIM};">'
                f"These 16 biological processes are composed via process-bigraph into a whole-cell simulation. "
                f"Colors indicate execution layer grouping.</div>"
                f"<div>{pills}</div>",
                color="teal",
            )
        )
    )

    return


@app.cell
def _(mo, card, TEXT_DIM):
    mo.output.replace(
        mo.Html(
            card(
                "How It Works",
                "&#128218;",
                f'<div style="font-size:0.78rem; color:{TEXT_DIM}; line-height:1.6;">'
                f'<b style="color:#00f0ff;">1.</b> Each cell in the colony runs as an independent v2ecoli simulation '
                f"with a unique random seed, producing stochastic variation in gene expression, "
                f"growth rate, and division timing.<br>"
                f'<b style="color:#ff3366;">2.</b> The simulations are submitted to HPC via SLURM, each running inside '
                f"a Singularity container with the v2ecoli process-bigraph model (~55 processes wired together).<br>"
                f'<b style="color:#33ff99;">3.</b> Results are collected as JSON state snapshots — '
                f"bulk molecule counts, "
                f"unique molecules (ribosomes, RNAPs, replication forks), mass, volume, and growth rate.<br>"
                f'<b style="color:#ffaa00;">4.</b> Colony-level analysis emerges from comparing cell trajectories: '
                f"population growth curves, division time distributions, metabolic heterogeneity, "
                f"and stochastic gene expression noise.</div>",
                color="magenta",
            )
        )
    )
    return


if __name__ == "__main__":
    app.run()
