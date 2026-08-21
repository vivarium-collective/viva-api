#!/usr/bin/env python3
"""Render the SMS-API continuous health monitor HTML report.

Reads probe outputs from environment variables (set by `.github/workflows/ci.yml`)
and writes a self-contained static HTML report to stdout. Style matches
`compose_cli_verification_report.html` — cyber/cyberpunk monospace, no JavaScript.
"""

from __future__ import annotations

import datetime
import html
import os


def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default) or default


def latency_class(ms: str, warn: int = 1000, err: int = 5000) -> str:
    try:
        v = int(ms)
    except (TypeError, ValueError):
        return "warn"
    if v >= err:
        return "err"
    if v >= warn:
        return "warn"
    return "ok"


def status_badge(status: str) -> str:
    s = (status or "").upper()
    cls = {"PASS": "b-pass", "WARN": "b-warn", "FAIL": "b-fail"}.get(s, "b-warn")
    label = s or "—"
    return f'<span class="badge {cls}">{label}</span>'


def overall_pill(overall: str) -> str:
    o = (overall or "").lower()
    cls = {"green": "pill-ok", "yellow": "pill-warn", "red": "pill-err"}.get(o, "pill-warn")
    label = {"green": "HEALTHY", "yellow": "DEGRADED", "red": "DOWN"}.get(o, o.upper() or "UNKNOWN")
    return f'<span class="pill {cls}">{label}</span>'


def starvation_pill(starved: str) -> str:
    s = (starved or "").lower()
    if s == "true":
        return '<span class="pill pill-err">DETECTED</span>'
    if s == "false":
        return '<span class="pill pill-ok">CLEAR</span>'
    return '<span class="pill pill-warn">UNKNOWN</span>'


def fmt_ms(ms: str) -> str:
    try:
        v = int(ms)
    except (TypeError, ValueError):
        return "—"
    if v >= 60_000:
        return f"{v / 1000:.1f}s"
    if v >= 1000:
        return f"{v / 1000:.2f}s"
    return f"{v}ms"


def safe(value: str, fallback: str = "—") -> str:
    return html.escape(value) if value else fallback


def card_pipeline_row(label: str, endpoint: str, code: str, ms: str, status: str) -> str:
    return f"""
  <tr>
    <td>{html.escape(label)}</td>
    <td><code>{html.escape(endpoint)}</code></td>
    <td class="num">{safe(code, "000")}</td>
    <td class="num"><span class="lat {latency_class(ms)}">{fmt_ms(ms)}</span></td>
    <td>{status_badge(status)}</td>
  </tr>"""


def env_section(prefix: str, label: str) -> str:
    overall = env(f"{prefix}_OVERALL")
    starved = env(f"{prefix}_STARVED")
    rows = [
        card_pipeline_row(
            "Health",
            "GET /health",
            "200" if env(f"{prefix}_HEALTH_STATUS") == "PASS" else "—",
            env(f"{prefix}_HEALTH_TIME_MS"),
            env(f"{prefix}_HEALTH_STATUS"),
        ),
        card_pipeline_row(
            "Version",
            "GET /version",
            "—",
            "0",
            "PASS" if env(f"{prefix}_VERSION") else "FAIL",
        ),
        card_pipeline_row(
            "Simulator records",
            "GET /core/v1/simulator/versions",
            "—",
            env(f"{prefix}_SIM_TIME_MS"),
            env(f"{prefix}_SIM_STATUS"),
        ),
        card_pipeline_row(
            "Discovery",
            "GET /api/v1/simulations/discovery",
            "—",
            env(f"{prefix}_DISC_TIME_MS"),
            env(f"{prefix}_DISC_STATUS"),
        ),
        card_pipeline_row(
            "Analyses canary",
            "POST /api/v1/analyses",
            "—",
            env(f"{prefix}_ANA_TIME_MS"),
            env(f"{prefix}_ANA_STATUS"),
        ),
    ]
    return f"""
<div class="card">
  <div class="ch">
    <span class="cmd">{html.escape(label)}</span>
    {overall_pill(overall)}
    <span class="badge b-info">Starvation: {starvation_pill(starved)}</span>
  </div>
  <div class="cb">
    <table class="check-table">
      <thead>
        <tr><th>Probe</th><th>Endpoint</th><th>HTTP</th><th>Wall</th><th>Result</th></tr>
      </thead>
      <tbody>{"".join(rows)}</tbody>
    </table>
  </div>
</div>"""


def comparison_table() -> str:
    """Side-by-side prod vs dev — rendered only if both probed."""
    target = env("TARGET", "both")
    if target != "both":
        return ""
    prod_result = env("PROD_RESULT")
    dev_result = env("DEV_RESULT")
    if prod_result == "skipped" or dev_result == "skipped":
        return ""

    probes = [
        ("Health", "HEALTH_STATUS", "HEALTH_TIME_MS"),
        ("Simulator", "SIM_STATUS", "SIM_TIME_MS"),
        ("Discovery", "DISC_STATUS", "DISC_TIME_MS"),
        ("Analyses", "ANA_STATUS", "ANA_TIME_MS"),
    ]

    body_rows: list[str] = []
    for label, status_key, time_key in probes:
        body_rows.append(f"""
      <tr>
        <td>{html.escape(label)}</td>
        <td>{status_badge(env(f"PROD_{status_key}"))}</td>
        <td class="num"><span class="lat {latency_class(env(f"PROD_{time_key}"))}">{fmt_ms(env(f"PROD_{time_key}"))}</span></td>
        <td>{status_badge(env(f"DEV_{status_key}"))}</td>
        <td class="num"><span class="lat {latency_class(env(f"DEV_{time_key}"))}">{fmt_ms(env(f"DEV_{time_key}"))}</span></td>
      </tr>""")

    return f"""
<div class="section-title">Environment Comparison</div>
<table class="check-table">
  <thead>
    <tr><th>Probe</th><th>Prod Result</th><th>Prod Wall</th><th>Dev Result</th><th>Dev Wall</th></tr>
  </thead>
  <tbody>{"".join(body_rows)}</tbody>
</table>"""


def main() -> None:
    now_utc = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    trigger = env("TRIGGER", "unknown")
    run_id = env("RUN_ID", "0")
    repo = env("REPO", "vivarium-collective/sms-api")
    threshold_min = env("THRESHOLD_MIN", "10")
    target = env("TARGET", "both")
    run_url = f"https://github.com/{repo}/actions/runs/{run_id}"
    pages_url = "https://vivarium-collective.github.io/sms-api/"

    # Prod stat cards — drive overall pill from probe-prod outputs.
    prod_overall = env("PROD_OVERALL")
    prod_starved = env("PROD_STARVED")
    prod_health_ms = env("PROD_HEALTH_TIME_MS")
    prod_version = env("PROD_VERSION") or "unknown"
    prod_sim_count = env("PROD_SIM_COUNT") or "0"
    prod_ana_ms = env("PROD_ANA_TIME_MS")

    # Build the prod / dev panels.
    show_prod = target != "dev" and env("PROD_RESULT") != "skipped"
    show_dev = target != "prod" and env("DEV_RESULT") != "skipped"

    prod_panel = env_section("PROD", "Production — sms.cam.uchc.edu") if show_prod else ""
    dev_panel = env_section("DEV", "Dev — sms-dev.cam.uchc.edu") if show_dev else ""

    # Stat-card row uses the prod numbers as the headline (prod is the SLO target).
    stat_html = f"""
<div class="summary">
  <div class="stat s1">
    <div class="number">{html.escape(prod_overall.upper() or "—")}</div>
    <div class="label">Prod Status</div>
  </div>
  <div class="stat s2">
    <div class="number"><span class="lat {latency_class(prod_health_ms)}">{fmt_ms(prod_health_ms)}</span></div>
    <div class="label">Health Latency</div>
  </div>
  <div class="stat s3">
    <div class="number">{html.escape(prod_version)}</div>
    <div class="label">Deployed Version</div>
  </div>
  <div class="stat s4">
    <div class="number">{html.escape(prod_sim_count)}</div>
    <div class="label">Simulator Records</div>
  </div>
  <div class="stat s5">
    <div class="number"><span class="lat {latency_class(prod_ana_ms, warn=int(threshold_min) * 60 * 500, err=int(threshold_min) * 60 * 1000)}">{fmt_ms(prod_ana_ms)}</span></div>
    <div class="label">Analyses Wall Time</div>
  </div>
  <div class="stat s6">
    <div class="number">{starvation_pill(prod_starved)}</div>
    <div class="label">Starvation Alert</div>
  </div>
</div>"""

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SMS-API — Continuous Health Monitor</title>
<style>
  :root {{
    --bg: #0d0d0d; --surface: #1a1a2e; --border: #2a2a4a; --text: #e0e0e0;
    --dim: #888; --accent: #00e5ff; --success: #00e676; --warning: #ffab00;
    --error: #ff3366; --pink: #ff3366; --purple: #7c4dff; --code-bg: #12121e;
    --gold: #ffd700;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'SF Mono','Fira Code','JetBrains Mono',monospace; background: var(--bg); color: var(--text); padding: 2rem; line-height: 1.6; }}
  .header {{ text-align: center; margin-bottom: 2.5rem; padding: 2rem; border: 1px solid var(--border); border-radius: 12px; background: linear-gradient(135deg, rgba(124,77,255,0.08), rgba(0,229,255,0.05)); }}
  .header h1 {{ font-size: 1.8rem; background: linear-gradient(90deg, var(--pink), var(--accent)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.3rem; }}
  .header .meta {{ color: var(--dim); font-size: 0.82rem; }}
  .header .version {{ display: inline-block; margin-top: 0.5rem; padding: 0.2rem 0.8rem; border: 1px solid var(--accent); border-radius: 20px; color: var(--accent); font-size: 0.78rem; }}
  .summary {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 0.8rem; margin-bottom: 2rem; }}
  @media (max-width: 1100px) {{ .summary {{ grid-template-columns: repeat(3, 1fr); }} }}
  .stat {{ text-align: center; padding: 1rem; border: 1px solid var(--border); border-radius: 8px; background: var(--surface); }}
  .stat .number {{ font-size: 1.4rem; font-weight: bold; }}
  .stat .label {{ color: var(--dim); font-size: 0.72rem; text-transform: uppercase; margin-top: 0.3rem; }}
  .stat.s1 .number {{ color: var(--accent); }} .stat.s2 .number {{ color: var(--purple); }}
  .stat.s3 .number {{ color: var(--gold); }}    .stat.s4 .number {{ color: var(--success); }}
  .stat.s5 .number {{ color: var(--pink); }}    .stat.s6 .number {{ color: var(--accent); }}
  .section-title {{ font-size: 1rem; color: var(--accent); margin: 2rem 0 0.8rem; padding-bottom: 0.4rem; border-bottom: 1px solid var(--border); letter-spacing: 0.5px; }}
  .card {{ border: 1px solid var(--border); border-radius: 10px; margin-bottom: 1.2rem; overflow: hidden; background: var(--surface); }}
  .card .ch {{ display: flex; align-items: center; gap: 0.8rem; padding: 0.8rem 1.1rem; border-bottom: 1px solid var(--border); background: rgba(0,0,0,0.3); }}
  .card .ch .cmd {{ font-weight: bold; color: var(--accent); font-size: 0.9rem; }}
  .card .ch .badge {{ margin-left: auto; padding: 0.12rem 0.55rem; border-radius: 12px; font-size: 0.68rem; font-weight: bold; text-transform: uppercase; }}
  .badge {{ padding: 0.12rem 0.55rem; border-radius: 12px; font-size: 0.68rem; font-weight: bold; text-transform: uppercase; }}
  .b-pass {{ background: rgba(0,230,118,0.15); color: var(--success); border: 1px solid var(--success); }}
  .b-warn {{ background: rgba(255,171,0,0.15); color: var(--warning); border: 1px solid var(--warning); }}
  .b-fail {{ background: rgba(255,51,102,0.15); color: var(--error); border: 1px solid var(--error); }}
  .b-info {{ background: rgba(0,229,255,0.10); color: var(--accent); border: 1px solid var(--accent); margin-left: 0.4rem; }}
  .card .cb {{ padding: 0.8rem 1.1rem; }}
  .pill {{ padding: 0.18rem 0.7rem; border-radius: 14px; font-size: 0.72rem; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px; }}
  .pill-ok {{ background: rgba(0,230,118,0.18); color: var(--success); border: 1px solid var(--success); }}
  .pill-warn {{ background: rgba(255,171,0,0.18); color: var(--warning); border: 1px solid var(--warning); }}
  .pill-err {{ background: rgba(255,51,102,0.18); color: var(--error); border: 1px solid var(--error); }}
  .check-table {{ width: 100%; border-collapse: collapse; margin: 0.6rem 0; font-size: 0.8rem; }}
  .check-table th, .check-table td {{ padding: 0.5rem 0.8rem; border: 1px solid var(--border); text-align: left; }}
  .check-table th {{ background: rgba(0,0,0,0.3); color: var(--accent); font-size: 0.72rem; text-transform: uppercase; }}
  .check-table td.num {{ font-variant-numeric: tabular-nums; }}
  .lat.ok  {{ color: var(--success); }}
  .lat.warn {{ color: var(--warning); }}
  .lat.err {{ color: var(--error); }}
  .footer {{ text-align: center; margin-top: 2.5rem; padding: 1.2rem; color: var(--dim); font-size: 0.7rem; border-top: 1px solid var(--border); }}
  .footer a {{ color: var(--accent); text-decoration: none; }}
</style>
</head>
<body>

<div class="header">
  <h1>SMS-API · CONTINUOUS HEALTH MONITOR</h1>
  <div class="meta">{html.escape(now_utc)} · trigger: <code>{html.escape(trigger)}</code> · target: <code>{html.escape(target)}</code> · starvation threshold: {html.escape(threshold_min)} min</div>
  <div class="meta"><a href="{html.escape(run_url)}" style="color:var(--accent);text-decoration:none;">Run #{html.escape(run_id)}</a></div>
  <span class="version">{overall_pill(prod_overall)}</span>
</div>

{stat_html}

<div class="section-title">Probe Results</div>
{prod_panel}
{dev_panel}

{comparison_table()}

<div class="footer">
  Report generated by <code>.github/workflows/ci.yml</code> ·
  <a href="{html.escape(pages_url)}">{html.escape(pages_url)}</a> ·
  <a href="{html.escape(run_url)}">Run logs</a>
</div>

</body>
</html>
"""

    print(page)


if __name__ == "__main__":
    main()
