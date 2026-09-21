"""The container contract, run for real (``docs/plan-core.md`` P2.3c).

``viva_core/runtime/batch-container-entrypoint.sh`` is what runs INSIDE a container job: stage
inputs in, run one command, sync outputs out. Its other half, ``stage_out_env``, writes the
``CONTAINER_*`` variables it reads. Until now the script lived only in the science repositories,
whose copies have diverged; this file is the first test the contract has had.

Hermetic: the script runs under ``bash`` with a fake ``aws`` first on ``PATH`` that maps
``s3://bucket/key`` to a directory. No Docker, no AWS.
"""

import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

from viva_core.backends.batch import stage_out_env

ENTRYPOINT = Path("viva_core/runtime/batch-container-entrypoint.sh")
DOCKERFILE = Path("Dockerfile-core-runtime")
REQUIREMENTS = Path("viva_core/runtime/requirements.txt")
DOMAIN_TERMS = ("ecoli", "parca", "vecoli", "biocyc", "ptools")

FAKE_AWS = """#!/usr/bin/env bash
# aws s3 sync|cp SRC DST [...]: s3://bucket/key is $FAKE_S3_ROOT/bucket/key.
set -euo pipefail
[[ "$1" == "s3" ]] || exit 2
verb="$2"; src="$3"; dst="$4"
local_path() { if [[ "$1" == s3://* ]]; then echo "${FAKE_S3_ROOT}/${1#s3://}"; else echo "$1"; fi; }
if [[ "$dst" == s3://* && -n "${FAKE_AWS_FAIL_UPLOAD:-}" ]]; then echo "fake aws: upload refused" >&2; exit 1; fi
s="$(local_path "$src")"; d="$(local_path "$dst")"
echo "$verb $src $dst" >> "${FAKE_S3_ROOT}/calls.log"
if [[ "$verb" == "sync" ]]; then
  [[ -d "$s" ]] || exit 0            # like the real one: an empty prefix is exit 0 and nothing copied
  mkdir -p "$d"; cp -R "$s"/. "$d"/
else
  mkdir -p "$(dirname "$d")"; cp "$s" "$d"
fi
"""


@pytest.fixture
def run(tmp_path: Path):  # type: ignore[no-untyped-def]
    bin_dir, s3 = tmp_path / "bin", tmp_path / "s3"
    bin_dir.mkdir()
    s3.mkdir()
    aws = bin_dir / "aws"
    aws.write_text(FAKE_AWS, encoding="utf-8")
    aws.chmod(0o755)

    def _run(command: str | None, **env: str) -> subprocess.CompletedProcess[str]:
        environ = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(tmp_path),
            "FAKE_S3_ROOT": str(s3),
            "CONTAINER_OUT_SYNC_INTERVAL": "1",
            **env,
        }
        if command is not None:
            environ["CONTAINER_JOB_CMD"] = command
        return subprocess.run(  # noqa: S603 - the script under test, with a controlled environment
            ["bash", str(ENTRYPOINT.resolve())],  # noqa: S607
            env=environ,
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

    _run.s3 = s3  # type: ignore[attr-defined]
    return _run


# ------------------------------------------------------------------ run one command


def test_it_runs_the_command_and_its_exit_code_is_the_jobs(run, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    done = run(f"echo ran > {tmp_path}/proof")
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "proof").read_text().strip() == "ran"
    assert "exited rc=0" in done.stdout

    failed = run("exit 3")
    assert failed.returncode == 3
    assert "exited rc=3" in failed.stdout


def test_no_command_is_a_failure_not_a_silent_success(run) -> None:  # type: ignore[no-untyped-def]
    done = run(None)
    assert done.returncode != 0
    assert "CONTAINER_JOB_CMD not set" in done.stderr


# ------------------------------------------------------------------ stage in


def test_inputs_are_staged_before_the_command_runs(run, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    (run.s3 / "bucket/inputs").mkdir(parents=True)
    (run.s3 / "bucket/inputs/data.txt").write_text("staged", encoding="utf-8")
    stage = tmp_path / "stage"
    done = run(
        f"cat {stage}/data.txt > {tmp_path}/seen",
        CONTAINER_STAGE_S3="s3://bucket/inputs",
        CONTAINER_STAGE_DIR=str(stage),
    )
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "seen").read_text() == "staged"


def test_a_stage_prefix_that_holds_nothing_fails_the_job_before_the_command_runs(run, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """``aws s3 sync`` exits 0 on an empty prefix. If staging was asked for, nothing staged is a
    failure -- said here, not by the workload minutes later."""
    done = run(
        f"touch {tmp_path}/ran", CONTAINER_STAGE_S3="s3://bucket/nothing-here", CONTAINER_STAGE_DIR=str(tmp_path / "st")
    )
    assert done.returncode == 1
    assert "EMPTY after sync" in done.stderr
    assert not (tmp_path / "ran").exists()


# ------------------------------------------------------------------ stage out


def test_outputs_reach_s3_whether_the_command_succeeds_or_fails(run, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    out = tmp_path / "out"
    for command, code in ((f"echo result > {out}/r.txt", 0), (f"echo partial > {out}/r.txt; exit 7", 7)):
        done = run(command, CONTAINER_OUT_DIR=str(out), CONTAINER_OUT_S3=f"s3://bucket/results-{code}")
        assert done.returncode == code, done.stderr
        assert (run.s3 / f"bucket/results-{code}/r.txt").exists()  # a failed run's output is evidence too


def test_a_failed_upload_fails_a_job_whose_command_succeeded(run, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """The results are the point: exit 0 with nothing uploaded would be COMPLETED with no output."""
    out = tmp_path / "out"
    done = run(
        f"echo result > {out}/r.txt",
        CONTAINER_OUT_DIR=str(out),
        CONTAINER_OUT_S3="s3://bucket/results",
        FAKE_AWS_FAIL_UPLOAD="1",
    )
    assert done.returncode == 1
    assert "output staging to s3://bucket/results failed" in done.stderr


def test_the_report_is_uploaded_beside_the_logs(run, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    report = tmp_path / "report.json"
    done = run(
        f"echo '{{}}' > {report}",
        CONTAINER_REPORT_PATH=str(report),
        CONTAINER_LOG_S3_PREFIX="s3://bucket/logs",
        AWS_BATCH_JOB_ID="job-1",
    )
    assert done.returncode == 0, done.stderr
    assert (run.s3 / "bucket/logs/job-1/report.json").exists()


# ------------------------------------------------------------------ the two halves agree


def test_every_variable_the_submitter_writes_is_one_the_entrypoint_reads() -> None:
    written = {
        entry["name"]
        for entry in stage_out_env(
            prefix="CONTAINER",
            out_dir="/o",
            out_s3="s3://b/o",
            stage_s3="s3://b/i",
            stage_dir="/i",
            log_s3_prefix="s3://b/l",
        )
    }
    script = ENTRYPOINT.read_text(encoding="utf-8")
    read = set(re.findall(r"\$\{(CONTAINER_[A-Z0-9_]+)", script))
    assert written and written <= read, f"written by stage_out_env but never read by the entrypoint: {written - read}"
    assert "CONTAINER_JOB_CMD" in read


# ------------------------------------------------------------------ the image, statically


def test_the_image_installs_the_entrypoint_at_the_path_job_definitions_call() -> None:
    """``/opt/batch-container-entrypoint.sh`` is an absolute path in every container job definition
    (``sms-cdk/lib/ray-batch-stack.ts``): the path is part of the contract."""
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    assert "COPY viva_core/runtime/batch-container-entrypoint.sh /opt/batch-container-entrypoint.sh" in dockerfile
    assert 'ENTRYPOINT ["/opt/batch-container-entrypoint.sh"]' in dockerfile
    assert ENTRYPOINT.stat().st_mode & stat.S_IXUSR


def test_the_runtime_is_no_applications() -> None:
    """Comments may name the application that motivated something; what RUNS may not."""
    for path in (ENTRYPOINT, DOCKERFILE, REQUIREMENTS):
        code = [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        offenders = [line for line in code if any(term in line.lower() for term in DOMAIN_TERMS)]
        assert not offenders, f"{path}: a domain term in something that runs: {offenders}"
