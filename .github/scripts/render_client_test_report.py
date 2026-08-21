#!/usr/bin/env python3
"""Render the sms-api client test-suite HTML report.

Reads the JSON file written by `run_client_tests.py` and emits a static HTML
report to stdout matching the cyber/monospace style used by ci.yml and cd.yml.

The report initially covers the Atlantis CLI dimension only; subsequent
commits in PR #129 extend it to cover the ptools dev HTTP and hosted marimo
app dimensions, plus a matrix-grouped view by user type.
"""

from __future__ import annotations

import argparse
import datetime
import html
import json
import os
import sys
from pathlib import Path
from typing import Any


def badge(status: str) -> str:
    s = (status or "").upper()
    cls = {
        "PASS": "b-pass",
        "FAIL": "b-fail",
        "WARN": "b-warn",
        "PARTIAL": "b-warn",
        "SKIPPED": "b-skip",
    }.get(s, "b-skip")
    label = s or "—"
    return f'<span class="badge {cls}">{html.escape(label)}</span>'


def fmt_ms(ms: int | str) -> str:
    try:
        v = int(ms)
    except (TypeError, ValueError):
        return "—"
    if v >= 60_000:
        return f"{v / 1000:.1f}s"
    if v >= 1000:
        return f"{v / 1000:.2f}s"
    return f"{v}ms"


def category_section(category: str, results: list[dict[str, Any]]) -> str:
    rows: list[str] = []
    for r in results:
        cmd = html.escape(r["command"])
        name = html.escape(r["name"])
        desc = html.escape(r["description"])
        wall = fmt_ms(r["wall_ms"])
        status = r["status"]
        reason = html.escape(r.get("failure_reason") or "")
        details_id = f"d-{name.replace(':', '-').replace(' ', '-')}"

        stdout_excerpt = html.escape(r.get("stdout_excerpt") or "")
        stderr_excerpt = html.escape(r.get("stderr_excerpt") or "")
        # Only render the <details> block when there's something useful to show.
        details_inner_parts = []
        if reason:
            details_inner_parts.append(f'<div class="reason"><strong>Reason:</strong> {reason}</div>')
        details_inner_parts.append(
            f'<div class="meta-row"><span class="dim">expected_exit:</span> '
            f"<code>{r['expected_exit']}</code> · "
            f'<span class="dim">actual_exit:</span> <code>{r["actual_exit"]}</code> · '
            f'<span class="dim">wall:</span> <code>{wall}</code></div>'
        )
        if r.get("expected_pattern"):
            details_inner_parts.append(
                f'<div class="meta-row"><span class="dim">expected_pattern:</span> '
                f"<code>{html.escape(r['expected_pattern'])}</code></div>"
            )
        if r.get("forbidden_pattern"):
            details_inner_parts.append(
                f'<div class="meta-row"><span class="dim">forbidden_pattern:</span> '
                f"<code>{html.escape(r['forbidden_pattern'])}</code></div>"
            )
        if stdout_excerpt:
            details_inner_parts.append(
                f'<div class="stream"><div class="stream-label">stdout</div><pre>{stdout_excerpt}</pre></div>'
            )
        if stderr_excerpt:
            details_inner_parts.append(
                f'<div class="stream"><div class="stream-label">stderr</div><pre>{stderr_excerpt}</pre></div>'
            )
        details_inner = "\n".join(details_inner_parts)

        rows.append(
            f"""
  <div class="test {("test-fail" if status == "FAIL" else "test-pass")}">
    <div class="test-head">
      <span class="test-name">{name}</span>
      {badge(status)}
      <span class="test-wall">{wall}</span>
    </div>
    <div class="test-cmd"><code>$ {cmd}</code></div>
    <div class="test-desc">{desc}</div>
    <details id="{details_id}">
      <summary>diagnostics</summary>
      <div class="diagnostics">{details_inner}</div>
    </details>
  </div>"""
        )

    return f"""
<div class="section-title">{html.escape(category)} ({len(results)} tests)</div>
<div class="test-grid">{"".join(rows)}</div>"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_json", type=Path)
    args = parser.parse_args()

    payload = json.loads(args.results_json.read_text(encoding="utf-8"))
    summary = payload["summary"]
    results = payload["results"]

    now_utc = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    run_id = os.environ.get("RUN_ID", "0")
    repo = os.environ.get("REPO", "vivarium-collective/sms-api")
    trigger = os.environ.get("TRIGGER", "unknown")
    run_url = f"https://github.com/{repo}/actions/runs/{run_id}"
    pages_url = "https://vivarium-collective.github.io/sms-api/"

    # Group results by category in stable order.
    category_order = ["preflight", "help", "api-readonly", "error-handling"]
    by_cat: dict[str, list[dict[str, Any]]] = {}
    for r in results:
        by_cat.setdefault(r["category"], []).append(r)
    ordered_cats = [c for c in category_order if c in by_cat] + [c for c in by_cat if c not in category_order]

    # Build sections.
    sections = "\n".join(category_section(c, by_cat[c]) for c in ordered_cats)

    # Category stat strip.
    cat_chips: list[str] = []
    for c in ordered_cats:
        chip = summary["by_category"].get(c, {"total": 0, "passed": 0, "failed": 0})
        passed = chip.get("passed", 0)
        failed = chip.get("failed", 0)
        cls = "chip-ok" if failed == 0 else "chip-err"
        cat_chips.append(
            f'<span class="chip {cls}">{html.escape(c)} '
            f'<span class="chip-counts">{passed}/{chip.get("total", 0)}</span></span>'
        )

    overall = summary.get("overall", "FAIL")
    pass_rate = (summary["passed"] * 100 // summary["total"]) if summary["total"] else 0

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>sms-api — Client Test Suite Report</title>
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
  .header .pill {{ display: inline-block; margin-top: 0.5rem; }}
  .summary {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 0.8rem; margin-bottom: 1.4rem; }}
  @media (max-width: 900px) {{ .summary {{ grid-template-columns: repeat(2, 1fr); }} }}
  .stat {{ text-align: center; padding: 1rem; border: 1px solid var(--border); border-radius: 8px; background: var(--surface); }}
  .stat .number {{ font-size: 1.5rem; font-weight: bold; }}
  .stat .label {{ color: var(--dim); font-size: 0.72rem; text-transform: uppercase; margin-top: 0.3rem; }}
  .stat.s1 .number {{ color: var(--accent); }}
  .stat.s2 .number {{ color: var(--success); }}
  .stat.s3 .number {{ color: var(--error); }}
  .stat.s4 .number {{ color: var(--gold); }}
  .stat.s5 .number {{ color: var(--purple); }}
  .chips {{ display: flex; flex-wrap: wrap; gap: 0.4rem; margin-bottom: 1.6rem; justify-content: center; }}
  .chip {{ padding: 0.2rem 0.7rem; border: 1px solid var(--border); border-radius: 16px; font-size: 0.78rem; background: var(--surface); }}
  .chip .chip-counts {{ margin-left: 0.4rem; color: var(--dim); }}
  .chip-ok {{ border-color: var(--success); color: var(--success); }}
  .chip-err {{ border-color: var(--error); color: var(--error); }}
  .section-title {{ font-size: 1rem; color: var(--accent); margin: 2rem 0 0.8rem; padding-bottom: 0.4rem; border-bottom: 1px solid var(--border); letter-spacing: 0.5px; text-transform: uppercase; }}
  .test-grid {{ display: grid; grid-template-columns: 1fr; gap: 0.6rem; }}
  .test {{ border: 1px solid var(--border); border-left-width: 3px; border-radius: 6px; background: var(--surface); padding: 0.7rem 0.9rem; }}
  .test-pass {{ border-left-color: var(--success); }}
  .test-fail {{ border-left-color: var(--error); background: linear-gradient(90deg, rgba(255,51,102,0.05), var(--surface) 30%); }}
  .test-head {{ display: flex; align-items: center; gap: 0.6rem; margin-bottom: 0.3rem; }}
  .test-name {{ font-weight: bold; color: var(--accent); font-size: 0.88rem; }}
  .test-wall {{ margin-left: auto; color: var(--dim); font-size: 0.78rem; }}
  .test-cmd {{ font-size: 0.78rem; color: var(--text); background: var(--code-bg); padding: 0.3rem 0.6rem; border-radius: 4px; margin: 0.3rem 0; overflow-x: auto; }}
  .test-cmd code {{ color: #c8c8d0; }}
  .test-desc {{ color: var(--dim); font-size: 0.78rem; margin-bottom: 0.3rem; }}
  details {{ margin-top: 0.3rem; }}
  details summary {{ cursor: pointer; color: var(--purple); font-size: 0.76rem; padding: 0.2rem 0; }}
  details summary:hover {{ color: var(--accent); }}
  .diagnostics {{ margin-top: 0.4rem; padding: 0.5rem 0.7rem; background: var(--code-bg); border: 1px solid var(--border); border-radius: 4px; }}
  .diagnostics .reason {{ color: var(--error); font-size: 0.8rem; margin-bottom: 0.4rem; }}
  .meta-row {{ font-size: 0.76rem; margin-bottom: 0.3rem; }}
  .meta-row code {{ color: var(--gold); }}
  .stream {{ margin-top: 0.5rem; }}
  .stream-label {{ color: var(--accent); font-size: 0.72rem; text-transform: uppercase; margin-bottom: 0.2rem; }}
  .stream pre {{ background: #0a0a14; border: 1px solid var(--border); border-radius: 4px; padding: 0.4rem 0.6rem; font-size: 0.72rem; line-height: 1.4; color: #c8c8d0; overflow-x: auto; white-space: pre-wrap; word-break: break-word; max-height: 320px; overflow-y: auto; }}
  .badge {{ display: inline-block; padding: 0.12rem 0.55rem; border-radius: 12px; font-size: 0.68rem; font-weight: bold; text-transform: uppercase; }}
  .b-pass {{ background: rgba(0,230,118,0.15); color: var(--success); border: 1px solid var(--success); }}
  .b-warn {{ background: rgba(255,171,0,0.15); color: var(--warning); border: 1px solid var(--warning); }}
  .b-fail {{ background: rgba(255,51,102,0.15); color: var(--error); border: 1px solid var(--error); }}
  .b-skip {{ background: rgba(136,136,136,0.10); color: var(--dim); border: 1px solid var(--border); }}
  .dim {{ color: var(--dim); }}
  .footer {{ text-align: center; margin-top: 2.5rem; padding: 1.2rem; color: var(--dim); font-size: 0.7rem; border-top: 1px solid var(--border); }}
  .footer a {{ color: var(--accent); text-decoration: none; }}
</style>
</head>
<body>

<div class="header">
  <h1>SMS-API · CLIENT TEST SUITE</h1>
  <div class="meta">{html.escape(now_utc)} · trigger: <code>{html.escape(trigger)}</code></div>
  <div class="meta">Target: <code>{html.escape(summary["base_url"])}</code> · deployed: <code>{html.escape(summary["deployed_version"])}</code></div>
  <div class="meta"><a href="{html.escape(run_url)}" style="color:var(--accent);text-decoration:none;">Run #{html.escape(run_id)}</a></div>
  <div class="pill">{badge(overall)}</div>
</div>

<div class="summary">
  <div class="stat s1"><div class="number">{summary["total"]}</div><div class="label">Total Tests</div></div>
  <div class="stat s2"><div class="number">{summary["passed"]}</div><div class="label">Passed</div></div>
  <div class="stat s3"><div class="number">{summary["failed"]}</div><div class="label">Failed</div></div>
  <div class="stat s4"><div class="number">{pass_rate}%</div><div class="label">Pass Rate</div></div>
  <div class="stat s5"><div class="number">{badge(overall)}</div><div class="label">Overall</div></div>
</div>

<div class="chips">
  {"".join(cat_chips)}
</div>

{sections}

<div class="footer">
  Report generated by <code>.github/workflows/client_test_suite.yml</code> ·
  <a href="{html.escape(pages_url)}">{html.escape(pages_url)}</a> ·
  <a href="{html.escape(run_url)}">Run logs</a>
</div>

</body>
</html>
"""

    sys.stdout.write(page)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
