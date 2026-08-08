#!/usr/bin/env python3
"""Render the SMS-API continuous deployment HTML report.

Reads job results + outputs from environment variables (set by
`.github/workflows/cd.yml`) and writes a self-contained static HTML report
to stdout. Style matches `compose_cli_verification_report.html`.
"""

from __future__ import annotations

import datetime
import html
import os


def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default) or default


def badge(status: str) -> str:
    s = (status or "").upper()
    cls = {
        "PASS": "b-pass",
        "SUCCESS": "b-pass",
        "WARN": "b-warn",
        "FAIL": "b-fail",
        "FAILURE": "b-fail",
        "SKIPPED": "b-skip",
        "CANCELLED": "b-skip",
        "": "b-skip",
    }.get(s, "b-warn")
    label = s or "—"
    return f'<span class="badge {cls}">{html.escape(label)}</span>'


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


def derive_namespace_result(
    *,
    requested: str,
    deploy_result: str,
    apply_status: str,
    rollout_status: str,
    smoke_result: str,
) -> str:
    """Compute a single PASS/FAIL/SKIPPED label for a namespace."""
    if requested != "true":
        return "SKIPPED"
    if deploy_result in ("", "skipped"):
        return "SKIPPED"
    if deploy_result == "failure" or apply_status == "FAIL" or rollout_status == "FAIL":
        return "FAIL"
    if smoke_result == "failure":
        return "FAIL"
    if smoke_result in ("", "skipped"):
        # Deploy passed but smoke didn't run (skip_smoke_test=true).
        return "PASS"
    if smoke_result == "success":
        return "PASS"
    return "WARN"


def main() -> None:
    now_utc = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    run_id = env("RUN_ID", "0")
    repo = env("REPO", "vivarium-collective/sms-api")
    trigger = env("TRIGGER", "unknown")
    target_version = env("TARGET_VERSION", "unknown")
    run_url = f"https://github.com/{repo}/actions/runs/{run_id}"
    pages_url = "https://vivarium-collective.github.io/sms-api/"

    dev_requested = env("DEPLOY_DEV_REQUESTED")
    prod_requested = env("DEPLOY_PROD_REQUESTED")

    dev_deploy_result = env("DEV_DEPLOY_RESULT")
    dev_apply = env("DEV_APPLY_STATUS")
    dev_rollout = env("DEV_ROLLOUT_STATUS")
    dev_deploy_ms = env("DEV_DEPLOY_WALL_MS")

    dev_smoke_result = env("DEV_SMOKE_RESULT")
    dev_smoke_health = env("DEV_SMOKE_HEALTH")
    dev_smoke_version = env("DEV_SMOKE_VERSION")
    dev_smoke_ana = env("DEV_SMOKE_ANALYSES")
    dev_smoke_ms = env("DEV_SMOKE_WALL_MS")

    prod_deploy_result = env("PROD_DEPLOY_RESULT")
    prod_apply = env("PROD_APPLY_STATUS")
    prod_rollout = env("PROD_ROLLOUT_STATUS")
    prod_deploy_ms = env("PROD_DEPLOY_WALL_MS")

    prod_smoke_result = env("PROD_SMOKE_RESULT")
    prod_smoke_health = env("PROD_SMOKE_HEALTH")
    prod_smoke_version = env("PROD_SMOKE_VERSION")
    prod_smoke_db = env("PROD_SMOKE_DB")
    prod_smoke_ms = env("PROD_SMOKE_WALL_MS")

    dev_result = derive_namespace_result(
        requested=dev_requested,
        deploy_result=dev_deploy_result,
        apply_status=dev_apply,
        rollout_status=dev_rollout,
        smoke_result=dev_smoke_result,
    )
    prod_result = derive_namespace_result(
        requested=prod_requested,
        deploy_result=prod_deploy_result,
        apply_status=prod_apply,
        rollout_status=prod_rollout,
        smoke_result=prod_smoke_result,
    )

    deployed_count = sum(1 for r in (dev_result, prod_result) if r == "PASS")
    requested_count = sum(1 for r in (dev_requested, prod_requested) if r == "true")
    overall = (
        "PASS"
        if requested_count > 0 and deployed_count == requested_count
        else ("FAIL" if any(r == "FAIL" for r in (dev_result, prod_result)) else "PARTIAL")
    )

    # Partial-failure banner: dev passed, prod requested but didn't pass.
    partial_banner = ""
    if dev_result == "PASS" and prod_requested == "true" and prod_result != "PASS":
        partial_banner = """
<div class="banner banner-warn">
  <strong>Partial deploy:</strong> rke-dev succeeded but rke-prod did not roll
  (smoke gate held, or prod step failed). Inspect the run logs and re-trigger
  with <code>namespace=rke-prod</code> once the issue is understood.
</div>"""

    # Total wall time = sum of available wall_ms values
    def total_ms() -> str:
        total = 0
        for v in (dev_deploy_ms, dev_smoke_ms, prod_deploy_ms, prod_smoke_ms):
            try:
                total += int(v)
            except (TypeError, ValueError):
                continue
        return str(total)

    def namespace_row(
        *,
        name: str,
        url: str,
        requested: str,
        apply_status: str,
        rollout_status: str,
        deploy_ms: str,
        smoke_result: str,
        smoke_checks: list[tuple[str, str]],
        smoke_ms: str,
        result: str,
    ) -> str:
        if requested != "true":
            return f"""
  <tr>
    <td><code>{html.escape(name)}</code></td>
    <td><a href="{html.escape(url)}">{html.escape(url)}</a></td>
    <td>{badge("SKIPPED")}</td>
    <td>{badge("SKIPPED")}</td>
    <td>{badge("SKIPPED")}</td>
    <td>{badge("SKIPPED")}</td>
    <td class="num">—</td>
    <td>{badge("SKIPPED")}</td>
  </tr>"""

        smoke_inner = (
            "<br>".join(
                f'<span class="dim">{html.escape(label)}:</span> {badge(status)}' for label, status in smoke_checks
            )
            if smoke_result != "skipped"
            else badge("SKIPPED")
        )

        total_ns_ms = 0
        for v in (deploy_ms, smoke_ms):
            try:
                total_ns_ms += int(v)
            except (TypeError, ValueError):
                continue

        return f"""
  <tr>
    <td><code>{html.escape(name)}</code></td>
    <td><a href="{html.escape(url)}">{html.escape(url)}</a></td>
    <td>{badge(apply_status)}</td>
    <td>{badge(rollout_status)}</td>
    <td class="num">{fmt_ms(deploy_ms)}</td>
    <td>{smoke_inner}</td>
    <td class="num">{fmt_ms(str(total_ns_ms)) if total_ns_ms else "—"}</td>
    <td>{badge(result)}</td>
  </tr>"""

    table_rows = "".join([
        namespace_row(
            name="sms-api-rke-dev",
            url="https://sms-dev.cam.uchc.edu",
            requested=dev_requested,
            apply_status=dev_apply,
            rollout_status=dev_rollout,
            deploy_ms=dev_deploy_ms,
            smoke_result=dev_smoke_result,
            smoke_checks=[
                ("/health", dev_smoke_health),
                ("/version", dev_smoke_version),
                ("/api/v1/analyses", dev_smoke_ana),
            ],
            smoke_ms=dev_smoke_ms,
            result=dev_result,
        ),
        namespace_row(
            name="sms-api-rke",
            url="https://sms.cam.uchc.edu",
            requested=prod_requested,
            apply_status=prod_apply,
            rollout_status=prod_rollout,
            deploy_ms=prod_deploy_ms,
            smoke_result=prod_smoke_result,
            smoke_checks=[
                ("/health", prod_smoke_health),
                ("/version", prod_smoke_version),
                ("/core/v1/simulator/versions", prod_smoke_db),
            ],
            smoke_ms=prod_smoke_ms,
            result=prod_result,
        ),
    ])

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SMS-API — Continuous Deployment</title>
<style>
  :root {{
    --bg: #0d0d0d; --surface: #1a1a2e; --border: #2a2a4a; --text: #e0e0e0;
    --dim: #888; --accent: #00e5ff; --success: #00e676; --warning: #ffab00;
    --error: #ff3366; --pink: #ff3366; --purple: #7c4dff; --code-bg: #12121e;
    --gold: #ffd700;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'SF Mono','Fira Code','JetBrains Mono',monospace; background: var(--bg); color: var(--text); padding: 2rem; line-height: 1.6; }}
  .header {{ text-align: center; margin-bottom: 2rem; padding: 2rem; border: 1px solid var(--border); border-radius: 12px; background: linear-gradient(135deg, rgba(124,77,255,0.08), rgba(0,229,255,0.05)); }}
  .header h1 {{ font-size: 1.8rem; background: linear-gradient(90deg, var(--pink), var(--accent)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.3rem; }}
  .header .meta {{ color: var(--dim); font-size: 0.82rem; }}
  .header .version {{ display: inline-block; margin-top: 0.5rem; padding: 0.2rem 0.8rem; border: 1px solid var(--accent); border-radius: 20px; color: var(--accent); font-size: 0.78rem; }}
  .banner {{ padding: 1rem 1.2rem; margin-bottom: 1.5rem; border-radius: 8px; border: 1px solid var(--border); font-size: 0.86rem; }}
  .banner-warn {{ background: rgba(255,171,0,0.10); border-color: var(--warning); color: var(--warning); }}
  .summary {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.8rem; margin-bottom: 2rem; }}
  @media (max-width: 900px) {{ .summary {{ grid-template-columns: repeat(2, 1fr); }} }}
  .stat {{ text-align: center; padding: 1rem; border: 1px solid var(--border); border-radius: 8px; background: var(--surface); }}
  .stat .number {{ font-size: 1.5rem; font-weight: bold; }}
  .stat .label {{ color: var(--dim); font-size: 0.72rem; text-transform: uppercase; margin-top: 0.3rem; }}
  .stat.s1 .number {{ color: var(--accent); }}
  .stat.s2 .number {{ color: var(--purple); }}
  .stat.s3 .number {{ color: var(--gold); }}
  .stat.s4 .number {{ color: var(--success); }}
  .section-title {{ font-size: 1rem; color: var(--accent); margin: 2rem 0 0.8rem; padding-bottom: 0.4rem; border-bottom: 1px solid var(--border); letter-spacing: 0.5px; }}
  .check-table {{ width: 100%; border-collapse: collapse; margin: 0.6rem 0; font-size: 0.8rem; }}
  .check-table th, .check-table td {{ padding: 0.6rem 0.8rem; border: 1px solid var(--border); text-align: left; vertical-align: middle; }}
  .check-table th {{ background: rgba(0,0,0,0.3); color: var(--accent); font-size: 0.72rem; text-transform: uppercase; }}
  .check-table td.num {{ font-variant-numeric: tabular-nums; }}
  .check-table .dim {{ color: var(--dim); }}
  .badge {{ display: inline-block; padding: 0.12rem 0.55rem; border-radius: 12px; font-size: 0.68rem; font-weight: bold; text-transform: uppercase; }}
  .b-pass {{ background: rgba(0,230,118,0.15); color: var(--success); border: 1px solid var(--success); }}
  .b-warn {{ background: rgba(255,171,0,0.15); color: var(--warning); border: 1px solid var(--warning); }}
  .b-fail {{ background: rgba(255,51,102,0.15); color: var(--error); border: 1px solid var(--error); }}
  .b-skip {{ background: rgba(136,136,136,0.10); color: var(--dim); border: 1px solid var(--border); }}
  .footer {{ text-align: center; margin-top: 2.5rem; padding: 1.2rem; color: var(--dim); font-size: 0.7rem; border-top: 1px solid var(--border); }}
  .footer a {{ color: var(--accent); text-decoration: none; }}
</style>
</head>
<body>

<div class="header">
  <h1>SMS-API · CONTINUOUS DEPLOYMENT</h1>
  <div class="meta">{html.escape(now_utc)} · trigger: <code>{html.escape(trigger)}</code> · tag: <code>{html.escape(target_version)}</code></div>
  <div class="meta"><a href="{html.escape(run_url)}" style="color:var(--accent);text-decoration:none;">Run #{html.escape(run_id)}</a></div>
  <span class="version">{badge(overall)}</span>
</div>

{partial_banner}

<div class="summary">
  <div class="stat s1"><div class="number">{badge(overall)}</div><div class="label">Overall Result</div></div>
  <div class="stat s2"><div class="number">{deployed_count}/{requested_count}</div><div class="label">Namespaces Deployed</div></div>
  <div class="stat s3"><div class="number">{html.escape(target_version)}</div><div class="label">Image Tag</div></div>
  <div class="stat s4"><div class="number">{fmt_ms(total_ms())}</div><div class="label">Total Wall Time</div></div>
</div>

<div class="section-title">Namespace Deployment Results</div>
<table class="check-table">
  <thead>
    <tr><th>Namespace</th><th>URL</th><th>Apply</th><th>Rollout</th><th>Deploy Wall</th><th>Smoke</th><th>Wall Total</th><th>Result</th></tr>
  </thead>
  <tbody>{table_rows}</tbody>
</table>

<div class="footer">
  Report generated by <code>.github/workflows/cd.yml</code> ·
  <a href="{html.escape(pages_url)}">{html.escape(pages_url)}</a> ·
  <a href="{html.escape(run_url)}">Run logs</a> ·
  Stanford (GovCloud) deployments use the SSO/CDK loop documented in <code>CLAUDE.md</code> and are out of scope here.
</div>

</body>
</html>
"""

    print(page)


if __name__ == "__main__":
    main()
