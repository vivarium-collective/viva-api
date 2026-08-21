#!/usr/bin/env python3
"""sms-api client comprehensive test runner.

Invokes every read-only and metadata Atlantis CLI command against a live
sms-api base URL (default: prod). Captures exit code, stdout/stderr excerpts,
and wall time for each test. Writes a JSON results file consumed by
`render_client_test_report.py`.

This first revision covers the Atlantis CLI dimension only. Subsequent
commits in PR #129 extend it to also cover the ptools dev HTTP path
(scripts/test_analyses.mjs) and the hosted marimo apps (app/ui/).

Tests are intentionally **non-destructive** — no simulation submissions,
no analyses POSTs that fire fresh HPC jobs, no container builds. The suite
exercises the client surface and the read-only API endpoints it talks to.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

# Per-step output cap (characters) — keep the JSON small enough for an artifact.
EXCERPT_LIMIT = 4000


@dataclass
class TestSpec:
    name: str
    category: str
    description: str
    command: str
    expected_exit: int = 0
    expected_pattern: str | None = None
    forbidden_pattern: str | None = None
    timeout_s: int = 60


@dataclass
class TestResult:
    name: str
    category: str
    description: str
    command: str
    expected_exit: int
    actual_exit: int
    status: str
    wall_ms: int
    stdout_excerpt: str
    stderr_excerpt: str
    failure_reason: str | None = None
    expected_pattern: str | None = None
    forbidden_pattern: str | None = None


def excerpt(blob: bytes | str, limit: int = EXCERPT_LIMIT) -> str:
    if isinstance(blob, bytes):
        try:
            text = blob.decode("utf-8", errors="replace")
        except Exception:
            text = repr(blob)
    else:
        text = blob
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n… [+{len(text) - limit} chars truncated]"


def run_test(spec: TestSpec, env: dict[str, str]) -> TestResult:
    argv = shlex.split(spec.command)
    start = time.monotonic_ns()
    try:
        proc = subprocess.run(  # noqa: S603 — commands are defined in build_test_specs(), not user input
            argv,
            capture_output=True,
            timeout=spec.timeout_s,
            env=env,
            check=False,
        )
        timed_out = False
        rc = proc.returncode
        out = proc.stdout
        err = proc.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        rc = -1
        out = exc.stdout or b""
        err = (exc.stderr or b"") + f"\n[TIMEOUT after {spec.timeout_s}s]".encode()
    wall_ms = (time.monotonic_ns() - start) // 1_000_000

    failure_reason: str | None = None
    if timed_out:
        status = "FAIL"
        failure_reason = f"command timed out after {spec.timeout_s}s"
    elif rc != spec.expected_exit:
        status = "FAIL"
        failure_reason = f"exit code {rc} (expected {spec.expected_exit})"
    else:
        status = "PASS"
        # combine stdout and stderr for pattern matching (Rich often writes to stderr)
        haystack = (
            (out.decode("utf-8", errors="replace") if isinstance(out, bytes) else out)
            + "\n"
            + (err.decode("utf-8", errors="replace") if isinstance(err, bytes) else err)
        )
        if spec.expected_pattern and not re.search(spec.expected_pattern, haystack):
            status = "FAIL"
            failure_reason = f"expected pattern not found: {spec.expected_pattern!r}"
        if status == "PASS" and spec.forbidden_pattern and re.search(spec.forbidden_pattern, haystack):
            status = "FAIL"
            failure_reason = f"forbidden pattern matched: {spec.forbidden_pattern!r}"

    return TestResult(
        name=spec.name,
        category=spec.category,
        description=spec.description,
        command=spec.command,
        expected_exit=spec.expected_exit,
        actual_exit=rc,
        status=status,
        wall_ms=int(wall_ms),
        stdout_excerpt=excerpt(out),
        stderr_excerpt=excerpt(err),
        failure_reason=failure_reason,
        expected_pattern=spec.expected_pattern,
        forbidden_pattern=spec.forbidden_pattern,
    )


def build_test_specs(base_url: str) -> list[TestSpec]:
    a = "uv run atlantis"

    # ── 1. CLI top-level + help surface (the CLI must be installable + introspectable)
    help_tests: list[TestSpec] = [
        TestSpec(
            name=f"help::{group or 'root'}",
            category="help",
            description=f"`atlantis {group} --help` returns successfully and renders the help text",
            command=f"{a} {group} --help".strip(),
            expected_pattern=r"(Usage|Commands|Options)",
        )
        for group in [
            "",
            "simulator",
            "simulation",
            "parca",
            "analysis",
            "compose",
            "demo",
        ]
    ]

    subcommand_help_tests: list[TestSpec] = [
        TestSpec(
            name=f"help::{group}::{cmd}",
            category="help",
            description=f"`atlantis {group} {cmd} --help` returns successfully",
            command=f"{a} {group} {cmd} --help",
            expected_pattern=r"(Usage|Options)",
        )
        for (group, cmd) in [
            ("simulator", "latest"),
            ("simulator", "list"),
            ("simulator", "status"),
            ("simulation", "run"),
            ("simulation", "get"),
            ("simulation", "list"),
            ("simulation", "configs"),
            ("simulation", "analyses"),
            ("simulation", "status"),
            ("simulation", "outputs"),
            ("simulation", "log"),
            ("simulation", "cancel"),
            ("simulation", "analysis"),
            ("parca", "list"),
            ("parca", "status"),
            ("analysis", "get"),
            ("analysis", "status"),
            ("analysis", "log"),
            ("analysis", "plots"),
            ("compose", "simulators"),
            ("compose", "processes"),
            ("compose", "steps"),
            ("compose", "biomodels-ids"),
            ("compose", "biomodels-meta"),
            ("compose", "status"),
            ("compose", "results"),
            ("compose", "build-status"),
        ]
    ]

    # ── 2. Read-only API queries against the configured base URL
    #     Each is bounded by a 60s timeout. `expected_pattern` is intentionally
    #     permissive — we want PASS as long as the CLI rendered *something*
    #     coherent, not a stack trace.
    api_tests: list[TestSpec] = [
        TestSpec(
            name="api::simulator-list",
            category="api-readonly",
            description="`atlantis simulator list` returns the registered vEcoli simulator versions",
            command=f"{a} simulator list --base-url {base_url}",
            forbidden_pattern=r"(Traceback|Internal Server Error|Connection refused)",
            timeout_s=45,
        ),
        TestSpec(
            name="api::simulation-list",
            category="api-readonly",
            description="`atlantis simulation list` returns simulation records",
            command=f"{a} simulation list --base-url {base_url}",
            forbidden_pattern=r"(Traceback|Internal Server Error|Connection refused)",
            timeout_s=45,
        ),
        TestSpec(
            name="api::parca-list",
            category="api-readonly",
            description="`atlantis parca list` returns parca datasets",
            command=f"{a} parca list --base-url {base_url}",
            forbidden_pattern=r"(Traceback|Internal Server Error|Connection refused)",
            timeout_s=45,
        ),
        TestSpec(
            name="api::compose-simulators",
            category="api-readonly",
            description="`atlantis compose simulators` lists registered compose simulators",
            command=f"{a} compose simulators --base-url {base_url}",
            forbidden_pattern=r"(Traceback|Internal Server Error|Connection refused)",
            timeout_s=45,
        ),
        TestSpec(
            name="api::compose-processes",
            category="api-readonly",
            description="`atlantis compose processes` lists registered process-bigraph processes",
            command=f"{a} compose processes --base-url {base_url}",
            forbidden_pattern=r"(Traceback|Internal Server Error|Connection refused)",
            timeout_s=45,
        ),
        TestSpec(
            name="api::compose-steps",
            category="api-readonly",
            description="`atlantis compose steps` lists registered process-bigraph steps",
            command=f"{a} compose steps --base-url {base_url}",
            forbidden_pattern=r"(Traceback|Internal Server Error|Connection refused)",
            timeout_s=45,
        ),
        TestSpec(
            name="api::compose-biomodels-ids",
            category="api-readonly",
            description="`atlantis compose biomodels-ids` lists BioModels database identifiers (EBI proxy)",
            command=f"{a} compose biomodels-ids --base-url {base_url}",
            forbidden_pattern=r"(Traceback|Internal Server Error|Connection refused)",
            timeout_s=60,
        ),
    ]

    # ── 3. Error-handling paths: invalid IDs must yield a clean non-zero exit
    #     code (NOT a Python traceback).
    error_tests: list[TestSpec] = [
        TestSpec(
            name="error::simulator-status-invalid",
            category="error-handling",
            description="`simulator status` with a non-existent ID exits non-zero without crashing",
            command=f"{a} simulator status 999999999 --base-url {base_url}",
            expected_exit=1,
            forbidden_pattern=r"Traceback \(most recent call last\)",
            timeout_s=30,
        ),
        TestSpec(
            name="error::simulation-get-invalid",
            category="error-handling",
            description="`simulation get` with a non-existent ID exits non-zero without crashing",
            command=f"{a} simulation get 999999999 --base-url {base_url}",
            expected_exit=1,
            forbidden_pattern=r"Traceback \(most recent call last\)",
            timeout_s=30,
        ),
        TestSpec(
            name="error::simulation-status-invalid",
            category="error-handling",
            description="`simulation status` with a non-existent ID exits non-zero without crashing",
            command=f"{a} simulation status 999999999 --base-url {base_url}",
            expected_exit=1,
            forbidden_pattern=r"Traceback \(most recent call last\)",
            timeout_s=30,
        ),
        TestSpec(
            name="error::unknown-command",
            category="error-handling",
            description="An unknown subcommand exits with Typer's standard usage error",
            command=f"{a} this-command-does-not-exist",
            expected_exit=2,
            expected_pattern=r"(No such command|Usage)",
            timeout_s=15,
        ),
    ]

    # ── 4. Server-side preflight (so the report can distinguish "API down"
    #     from "CLI broken"). Curl is always available on GH runners.
    preflight: list[TestSpec] = [
        TestSpec(
            name="preflight::health",
            category="preflight",
            description="Sanity-check that the target API is reachable before judging CLI failures.",
            command=f"curl -sf --max-time 10 -o /dev/null -w %{{http_code}} {base_url}/health",
            expected_pattern=r"^200",
            timeout_s=15,
        ),
        TestSpec(
            name="preflight::version",
            category="preflight",
            description="Capture the deployed sms-api version for the report header.",
            command=f"curl -sf --max-time 10 {base_url}/version",
            timeout_s=15,
        ),
    ]

    return preflight + help_tests + subcommand_help_tests + api_tests + error_tests


@dataclass
class SuiteSummary:
    base_url: str
    total: int
    passed: int
    failed: int
    by_category: dict[str, dict[str, int]] = field(default_factory=dict)
    overall: str = "FAIL"
    started_at: str = ""
    finished_at: str = ""
    deployed_version: str = "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.environ.get("CLIENT_TEST_BASE_URL", os.environ.get("CLI_TEST_BASE_URL", "https://sms.cam.uchc.edu")),
        help="API base URL the clients should target.",
    )
    parser.add_argument(
        "--out",
        default="client-test-results.json",
        help="Path to write the JSON results file.",
    )
    args = parser.parse_args()

    # Force the CLI to read base URL from --base-url; also set the env var as
    # a belt-and-suspenders default for tests that didn't pass --base-url.
    env = dict(os.environ)
    env["API_BASE_URL"] = args.base_url
    env["NO_COLOR"] = "1"
    env["TERM"] = "dumb"

    specs = build_test_specs(args.base_url)
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    results: list[TestResult] = []
    for spec in specs:
        print(f"▸ {spec.name:<48} ", end="", flush=True)
        result = run_test(spec, env=env)
        results.append(result)
        marker = "✓" if result.status == "PASS" else "✗"
        print(f"{marker} {result.status:<4} ({result.wall_ms} ms)")
        if result.failure_reason:
            print(f"   reason: {result.failure_reason}")

    finished_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    summary = SuiteSummary(
        base_url=args.base_url,
        total=len(results),
        passed=sum(1 for r in results if r.status == "PASS"),
        failed=sum(1 for r in results if r.status == "FAIL"),
        started_at=started_at,
        finished_at=finished_at,
    )

    for r in results:
        cat = summary.by_category.setdefault(r.category, {"total": 0, "passed": 0, "failed": 0})
        cat["total"] += 1
        bucket = "passed" if r.status == "PASS" else "failed"
        cat[bucket] += 1

    if summary.total > 0 and summary.failed == 0:
        summary.overall = "PASS"
    elif summary.failed > 0 and summary.passed > 0:
        summary.overall = "PARTIAL"
    else:
        summary.overall = "FAIL"

    # Pull the deployed version out of the preflight result so the report can
    # display it without re-running curl.
    for r in results:
        if r.name == "preflight::version" and r.status == "PASS":
            summary.deployed_version = r.stdout_excerpt.strip().strip('"') or "unknown"
            break

    payload = {
        "summary": asdict(summary),
        "results": [asdict(r) for r in results],
    }
    Path(args.out).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("\n── summary ──")
    print(f"  base_url={summary.base_url}")
    print(f"  total={summary.total}  passed={summary.passed}  failed={summary.failed}")
    print(f"  overall={summary.overall}")
    print(f"  results written to: {args.out}")

    return 0 if summary.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
