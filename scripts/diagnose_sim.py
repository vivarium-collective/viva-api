"""
diagnose_sim.py — one-shot diagnostic for a failed atlantis simulation.

Pulls the Nextflow workflow log via the sms-api data service, parses it for
task status, then fetches each failing (or silently-empty) task's
``.command.err`` / ``.command.out`` / ``.command.sh`` directly from S3 so
the user can see the actual Python traceback, stderr, and argv the
container received.

Usage
-----
::

    uv run python scripts/diagnose_sim.py <SIM_ID>

    # Optional overrides:
    uv run python scripts/diagnose_sim.py <SIM_ID> \\
        --base-url https://sms.cam.uchc.edu \\
        --bucket smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91 \\
        --tail 40                    # how many lines of each file to show

The bucket is auto-detected from ``Settings.storage_s3_bucket`` /
``Settings.s3_work_bucket`` when available; ``--bucket`` only needs to be
passed if those aren't configured locally.

What it reports
---------------
1. Sim header: status, experiment_id, simulator commit, Batch job id.
2. Nextflow log tail (last ~30 lines) for context.
3. Per-task breakdown: status (✓ / FAILED / IGNORED / running / pending),
   hash, process name, and — for tasks that failed or that completed but
   produced no downstream work — the ``.command.err`` tail.
4. A parca_out check: lists the contents of the expected kb output
   locations to catch "success-but-empty-output" bugs (e.g. parca wrote
   somewhere different from what template.nf published).

This script makes no changes; it is read-only.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("boto3 is required. Install with `uv sync` (it's a project dep).")
    sys.exit(1)

# Make the sms-api package importable when running this script directly
import os

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from app.app_data_service import get_data_service  # noqa: E402
from viva_api.config import get_settings  # noqa: E402

# ---------------------------------------------------------------------------
# Log-parsing utilities
# ---------------------------------------------------------------------------

# Nextflow status-table lines, e.g.:
#   [78/9f57e8] runParca (1)        | 1 of 1 ✔
#   [00/d563ca] createVariants (1)  | 1 of 1, ignored: 1 ✔
#   [-        ] sim_gen_1           -
_PROC_LINE = re.compile(
    r"\[(?P<hash>[a-f0-9]{2}/[a-f0-9]{6}|-\s*)\]\s+"
    r"(?P<process>\S+(?:\s*\(\d+\))?)\s+"
    r"(?:\|\s+)?(?P<status>.*?)$"
)

# Explicit "Process X failed" / NOTE / Caused-by blocks
_FAIL_NOTE = re.compile(
    r"\[(?P<hash>[a-f0-9]{2}/[a-f0-9]{6})\]\s+NOTE:\s+Process\s+"
    r"`(?P<process>[^`]+)`\s+failed"
)

# Explicit work dir URIs that sometimes appear on caused-by blocks
_WORK_DIR_URI = re.compile(r"s3://\S+/nextflow_workdirs/[a-f0-9]{2}/[a-f0-9]+")


@dataclass
class TaskRecord:
    hash_prefix: str  # e.g. "78/9f57e8"
    process: str  # e.g. "runParca (1)"
    status_raw: str  # the tail of the progress line
    failed: bool
    ignored: bool
    completed: bool
    work_uri: str | None = None  # filled in by S3 search

    @property
    def label(self) -> str:
        if self.failed and self.ignored:
            return "FAILED (ignored)"
        if self.failed:
            return "FAILED"
        if self.ignored:
            return "ignored"
        if self.completed:
            return "OK"
        return "running / pending"


def parse_log(log: str) -> list[TaskRecord]:
    """
    Walk the workflow log and return one TaskRecord per distinct task hash.
    Later status lines for the same hash overwrite earlier ones so we end
    up with the final state.
    """
    by_hash: dict[str, TaskRecord] = {}
    failed_processes: dict[str, str] = {}  # hash -> process (from NOTE line)

    # First pass: collect explicit failure notes (Nextflow emits these even
    # when errorStrategy='ignore' hides the failure from the progress table).
    for line in log.splitlines():
        m = _FAIL_NOTE.search(line)
        if m:
            failed_processes[m.group("hash")] = m.group("process")

    for line in log.splitlines():
        # Skip NOTE lines entirely — _PROC_LINE would otherwise match them
        # with "NOTE:" as the process name.
        if "NOTE:" in line or "WARN:" in line:
            continue
        m = _PROC_LINE.search(line)
        if not m:
            continue
        h = m.group("hash").strip()
        if h == "-":
            continue  # pending task with no hash yet
        process = m.group("process").strip()
        status = m.group("status").strip()
        # Reject false-positive "processes" like a bare "NOTE:" that slipped
        # through (defense in depth).
        if process.endswith(":"):
            continue

        failed = h in failed_processes or "FAILED" in status.upper() or "error" in status.lower()
        ignored = "ignored" in status.lower()
        completed = "✔" in status

        by_hash[h] = TaskRecord(
            hash_prefix=h,
            process=process,
            status_raw=status,
            failed=failed,
            ignored=ignored,
            completed=completed and not failed,
        )

    # If we know about a failed hash but missed its status line (rare), still
    # register it so the S3 fetch runs.
    for h, proc in failed_processes.items():
        if h not in by_hash:
            by_hash[h] = TaskRecord(
                hash_prefix=h,
                process=proc,
                status_raw="(no progress line seen)",
                failed=True,
                ignored=True,
                completed=False,
            )

    return list(by_hash.values())


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _candidate_workroots(bucket: str, experiment_id: str) -> list[str]:
    """
    Known / observed work-dir layouts. We try each in turn — the first one
    that resolves a given hash is the winner. The doubled ``<exp>/<exp>``
    case reflects the current sms-api path composition; the single-``<exp>``
    case is there for forward-compat if that gets fixed.
    """
    return [
        f"s3://{bucket}/vecoli-output/{experiment_id}/{experiment_id}/nextflow/nextflow_workdirs",
        f"s3://{bucket}/vecoli-output/{experiment_id}/nextflow/nextflow_workdirs",
        f"s3://{bucket}/nextflow/work",
    ]


_s3_errors_seen: set[str] = set()


def _list_subdirs(s3, bucket: str, prefix: str, max_keys: int = 1000) -> list[str]:
    """Return the set of keys directly under ``prefix`` (using Delimiter='/').

    Returns full keys with trailing ``/`` for subdirectories. Surfaces S3
    client errors exactly once per error code (so repeated credential
    failures don't flood the output) by printing a warning on first sight.
    """
    out: list[str] = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=max_keys):
            for cp in page.get("CommonPrefixes", []) or []:
                out.append(cp["Prefix"])
            for obj in page.get("Contents", []) or []:
                out.append(obj["Key"])
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in _s3_errors_seen:
            _s3_errors_seen.add(code)
            print(f"\n  ⚠  S3 listing failed: {code} — {e.response['Error'].get('Message', '')}")
            if code in (
                "InvalidToken",
                "ExpiredToken",
                "TokenRefreshRequired",
                "UnrecognizedClientException",
                "AuthFailure",
            ):
                print("  ⚠  Your AWS credentials are expired / invalid.")
                print("     Run `aws sso login` (or `aws configure sso`) and retry.")
            elif code == "AccessDenied":
                print("  ⚠  Current IAM role lacks ListObjects on this bucket.")
        return []
    return out


def find_work_uri(s3, bucket: str, experiment_id: str, hash_prefix: str) -> str | None:
    """
    Resolve ``hh/xxxxxx`` (6-char short) to a full
    ``s3://bucket/.../hh/xxxxxx...`` URI by probing candidate work roots.

    Nextflow's short hash is actually the first 6 characters of a longer
    hash, and the directory on S3 is named with the FULL hash — so we can't
    just concat ``{root}/{hh}/{short}``; we have to list ``{root}/{hh}/``
    and pick the entry that starts with ``short``.
    """
    hh, short = hash_prefix.split("/")
    for root in _candidate_workroots(bucket, experiment_id):
        no_scheme = root[len("s3://") :]
        bkt, _, root_key = no_scheme.partition("/")
        if bkt != bucket:
            continue
        # List everything under {root}/{hh}/ — subdirs are the candidate tasks.
        entries = _list_subdirs(s3, bucket, f"{root_key}/{hh}/")
        for entry in entries:
            # entry is a subdir like "...nextflow_workdirs/78/9f57e8abcdef.../"
            basename = entry.rstrip("/").split("/")[-1]
            if basename.startswith(short):
                return f"s3://{bucket}/{entry.rstrip('/')}"
    return None


def show_experiment_tree(s3, bucket: str, experiment_id: str, max_entries: int = 50) -> None:
    """
    Last-resort diagnostic: dump the top few levels of what actually exists
    under each candidate path, so the user can see the real layout when
    auto-resolution fails.
    """
    tried: set[str] = set()
    for root in _candidate_workroots(bucket, experiment_id):
        no_scheme = root[len("s3://") :]
        bkt, _, root_key = no_scheme.partition("/")
        if bkt != bucket or root_key in tried:
            continue
        tried.add(root_key)
        entries = _list_subdirs(s3, bucket, f"{root_key}/")
        if not entries:
            print(f"  (empty) s3://{bucket}/{root_key}/")
            continue
        print(f"  s3://{bucket}/{root_key}/")
        for e in entries[:max_entries]:
            short = e[len(root_key) + 1 :]  # drop the root prefix
            print(f"      {short}")
        if len(entries) > max_entries:
            print(f"      ... (+{len(entries) - max_entries} more)")


def _fetch_s3_text(s3, s3_uri: str, tail: int) -> str:
    """Download a text object from S3 and return last ``tail`` lines (or all)."""
    if not s3_uri.startswith("s3://"):
        return f"<invalid URI: {s3_uri}>"
    no_scheme = s3_uri[len("s3://") :]
    bucket, _, key = no_scheme.partition("/")
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", "replace")
    except ClientError as e:
        return f"<unreachable: {e.response['Error']['Code']}>"
    lines = body.splitlines()
    if tail and len(lines) > tail:
        return "\n".join([f"… ({len(lines) - tail} earlier lines elided)", *lines[-tail:]])
    return body


def list_kb_outputs(s3, bucket: str, experiment_id: str) -> list[tuple[str, int]]:
    """List files under each candidate parca_N/kb/ path, returning (uri, size) pairs."""
    results: list[tuple[str, int]] = []
    candidates = (
        [
            f"vecoli-output/{experiment_id}/{experiment_id}/parca_{i}/kb/"
            for i in range(16)  # more than anyone would realistically run
        ]
        + [f"vecoli-output/{experiment_id}/parca_{i}/kb/" for i in range(16)]
        + [
            f"vecoli-output/{experiment_id}/{experiment_id}/parca/kb/",
            f"vecoli-output/{experiment_id}/parca/kb/",
        ]
    )
    for prefix in candidates:
        try:
            r = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=20)
        except ClientError:
            continue
        for obj in r.get("Contents", []) or []:
            results.append((f"s3://{bucket}/{obj['Key']}", obj["Size"]))
    return results


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


def _hr(title: str) -> str:
    line = "─" * max(8, 74 - len(title))
    return f"\n── {title} {line}"


def _resolve_bucket(cli_bucket: str | None) -> str:
    if cli_bucket:
        return cli_bucket
    settings = get_settings()
    for attr in ("s3_work_bucket", "storage_s3_bucket"):
        v = getattr(settings, attr, "") or ""
        if v:
            return v
    env = os.environ.get("STORAGE_S3_BUCKET") or os.environ.get("S3_WORK_BUCKET")
    if env:
        return env
    raise SystemExit(
        "Could not auto-detect bucket name. Pass --bucket explicitly or set "
        "STORAGE_S3_BUCKET / S3_WORK_BUCKET in the environment."
    )


def _make_s3_client(region: str | None, profile: str | None):
    """
    Build an S3 client, explicitly taking ``profile`` so we don't silently
    fall back on (possibly stale) env-var credentials or the wrong named
    profile. Also dumps what boto3 actually picked up so we can debug
    credential-source mismatches with the AWS CLI.
    """
    # Default region for Stanford GovCloud is us-gov-west-1.
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    creds = session.get_credentials()
    if creds is None:
        print("  ⚠  boto3 found no credentials. Run `aws sso login` (or export AWS_PROFILE) and retry.")
    else:
        frozen = creds.get_frozen_credentials()
        # Redact the secret; we only care about identity + token presence.
        ak = frozen.access_key or ""
        print(
            f"  boto3 credentials: access_key={ak[:4]}…{ak[-4:] if len(ak) >= 8 else ''}"
            f"  profile={profile or session.profile_name or 'default'}"
            f"  session_token={'set' if frozen.token else 'unset'}"
            f"  region={region or session.region_name or 'unset'}"
        )
    return session.client("s3", region_name=region or session.region_name or "us-gov-west-1")


def _inspect_interesting_tasks(s3, bucket: str, experiment_id: str, interesting: list[TaskRecord], tail: int) -> bool:
    """Fetch and display S3 work-dir contents for failed/interesting tasks. Returns True if any resolved."""
    any_resolved = False
    for t in interesting:
        print(_hr(f"Task {t.hash_prefix} · {t.process}  [{t.label}]"))
        work_uri = find_work_uri(s3, bucket, experiment_id, t.hash_prefix)
        if not work_uri:
            print("  <could not locate work directory on S3>")
            continue
        any_resolved = True
        print(f"  work dir: {work_uri}")
        no_scheme = work_uri[len("s3://") :]
        _, _, work_key = no_scheme.partition("/")
        entries = _list_subdirs(s3, bucket, f"{work_key}/")
        if entries:
            print("  contents:")
            for e in entries[:30]:
                print(f"    {e[len(work_key) + 1 :]}")
        for fn, label in [
            (".command.sh", "command.sh (what was actually run)"),
            (".command.err", f"command.err (tail {tail})"),
            (".command.out", f"command.out (tail {tail})"),
        ]:
            print(_hr(label))
            text = _fetch_s3_text(s3, f"{work_uri}/{fn}", tail)
            for line in text.splitlines()[-tail:]:
                print(f"    {line}")
    return any_resolved


def _report_kb_outputs(s3, bucket: str, experiment_id: str) -> None:
    """Cross-check whether parca kb paths have contents."""
    print(_hr("Published kb/ locations (sanity)"))
    kb_hits = list_kb_outputs(s3, bucket, experiment_id)
    if not kb_hits:
        print("  (no objects found under any candidate parca_*/kb/ path)")
        print("  → parca reported ✓ but wrote nothing to the expected S3 prefix.")
        print("  → Check the parca task's .command.sh above for the -o path it used.")
    else:
        for uri, size in kb_hits[:20]:
            print(f"  {size:>10,} bytes  {uri}")
        if len(kb_hits) > 20:
            print(f"  ... (+{len(kb_hits) - 20} more)")


def _report_experiment_listing(s3, bucket: str, experiment_id: str) -> None:
    """Dump everything under the experiment prefix as a last-resort diagnostic."""
    print(_hr(f"Recursive listing of everything under vecoli-output/{experiment_id}/"))
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    total_bytes = 0
    prefixes_seen: dict[str, int] = {}
    try:
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=f"vecoli-output/{experiment_id}/",
            MaxKeys=1000,
        ):
            for obj in page.get("Contents", []) or []:
                count += 1
                total_bytes += obj["Size"]
                key_parts = obj["Key"].split("/")
                group = "/".join(key_parts[:5]) if len(key_parts) >= 5 else "/".join(key_parts)
                prefixes_seen[group] = prefixes_seen.get(group, 0) + 1
    except ClientError as e:
        print(f"  <listing failed: {e}>")
    if count == 0:
        print(f"  (nothing under vecoli-output/{experiment_id}/)")
        alt_prefix = f"nextflow/work/{experiment_id}/"
        print(f"\n  trying alt prefix: {alt_prefix}")
        try:
            r = s3.list_objects_v2(Bucket=bucket, Prefix=alt_prefix, MaxKeys=20)
            for obj in r.get("Contents", []) or []:
                print(f"    {obj['Size']:>10,}  {obj['Key']}")
        except ClientError as e:
            print(f"    <{e}>")
    else:
        print(f"  {count} objects, {total_bytes:,} total bytes")
        for prefix, n in sorted(prefixes_seen.items(), key=lambda x: -x[1])[:20]:
            print(f"    {n:>4} files  s3://{bucket}/{prefix}/…")


def _print_status(ds: object, sim_id: int) -> None:
    """Print the simulation status, handling errors gracefully."""
    try:
        status = ds.get_workflow_status(simulation_id=sim_id)
        print(
            f"  status:         {status.status.value}" + (f"  · {status.error_message}" if status.error_message else "")
        )
    except Exception as e:
        print(f"  status:         <unavailable: {e}>")


def _resolve_region(region: str | None, bucket: str) -> str:
    """Resolve the AWS region, with a GovCloud heuristic for Stanford buckets."""
    if region is not None:
        return region
    if "smsvpctest" in bucket:
        resolved = "us-gov-west-1"
        print(f"  (auto-selected region {resolved} based on bucket name)")
        return resolved
    return (
        get_settings().storage_s3_region
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "us-gov-west-1"
    )


def diagnose(
    sim_id: int,
    *,
    base_url: str | None = None,
    bucket: str | None = None,
    region: str | None = None,
    profile: str | None = None,
    tail: int = 40,
) -> int:
    ds = get_data_service(base_url=base_url) if base_url else get_data_service()
    try:
        sim = ds.get_workflow(simulation_id=sim_id)
    except Exception as e:
        print(f"Could not fetch simulation {sim_id}: {e}")
        return 2

    bucket = _resolve_bucket(bucket)
    region = _resolve_region(region, bucket)

    experiment_id = sim.experiment_id
    print(_hr(f"Simulation {sim_id}  ·  {experiment_id}"))
    print(f"  simulator_id:   {sim.simulator_id}")
    print(f"  config file:    {sim.simulation_config_filename}")
    print(f"  job id:         {sim.job_id or '(unset)'}")
    print(f"  bucket:         s3://{bucket}")

    _print_status(ds, sim_id)

    # Pull the nextflow log
    try:
        log = ds.get_workflow_log(simulation_id=sim_id, truncate=False)
    except Exception as e:
        print(f"\nCould not fetch workflow log: {e}")
        return 2

    print(_hr("Nextflow log tail (last 40 lines)"))
    for line in log.splitlines()[-40:]:
        print(f"  {line}")

    tasks = parse_log(log)
    print(_hr(f"Task breakdown ({len(tasks)} tasks seen)"))

    # Sort: failed → ignored → running → ok
    def _key(t: TaskRecord) -> tuple[int, str]:
        return (
            0 if t.failed else (1 if t.ignored else (2 if t.completed else 3)),
            t.process,
        )

    tasks.sort(key=_key)
    for t in tasks:
        print(f"  {t.label:<22} [{t.hash_prefix}] {t.process:<35} | {t.status_raw}")

    # Set up S3 client (explicit profile handling so we don't silently pick
    # up stale env-var credentials when the AWS CLI works via SSO cache).
    # Region was already resolved above with a GovCloud-bucket override.
    s3 = _make_s3_client(region=region, profile=profile)

    # Always inspect FAILED / IGNORED tasks. Also inspect runParca /
    # createVariants regardless — if parca reported ✓ but the kb/ is empty
    # (see sanity check below), the parca task's .command.err is where the
    # real error hides.
    interesting: list[TaskRecord] = [t for t in tasks if t.failed or t.ignored]
    for t in tasks:
        if t in interesting:
            continue
        if "runParca" in t.process or "createVariants" in t.process:
            interesting.append(t)

    any_resolved = _inspect_interesting_tasks(s3, bucket, experiment_id, interesting, tail)

    if interesting and not any_resolved:
        print(_hr("S3 layout under candidate work roots (auto-resolve failed)"))
        show_experiment_tree(s3, bucket, experiment_id)

    _report_kb_outputs(s3, bucket, experiment_id)
    _report_experiment_listing(s3, bucket, experiment_id)

    return 0 if not any(t.failed for t in tasks) else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("sim_id", type=int, help="Simulation database ID")
    p.add_argument("--base-url", default=None, help="API base URL (overrides API_BASE_URL env)")
    p.add_argument("--bucket", default=None, help="S3 bucket (overrides auto-detection from settings)")
    p.add_argument("--region", default=None, help="AWS region (defaults to storage_s3_region)")
    p.add_argument("--tail", type=int, default=40, help="Lines to show from each task file (default: 40)")
    p.add_argument(
        "--profile",
        default=None,
        help="AWS named profile to use (overrides AWS_PROFILE env var). "
        "Useful when the AWS CLI works but boto3 picks up stale env credentials.",
    )
    args = p.parse_args(argv)

    return diagnose(
        sim_id=args.sim_id,
        base_url=args.base_url,
        bucket=args.bucket,
        region=args.region,
        profile=args.profile,
        tail=args.tail,
    )


if __name__ == "__main__":
    raise SystemExit(main())
