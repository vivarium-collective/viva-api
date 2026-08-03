"""End-to-end CLI tests for the full EUTE pipeline.

Tests the Atlantis CLI against a live deployed API server. No mocks.
Runs the real pipeline: build simulator -> run simulation -> download outputs.

Prerequisites:
- AWS SSO session active (`aws sso login --profile stanford-sso`)
- ptools-proxy running (`ptools-proxy.sh -s smsvpctest`) on localhost:8080
- Network access to the deployed stanford-test API

Run with:
    uv run pytest tests/api/app/test_cli_e2e.py -v -s

Skip behavior:
- Skips entirely if localhost:8080 is not reachable
- Skips entirely if AWS credentials are not configured
"""

import os
import re
import shutil
import subprocess
import tarfile
from collections.abc import Generator
from pathlib import Path

import httpx
import pytest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE_URL = "http://localhost:8080"
REPO_URL = "https://github.com/CovertLabEcoli/vEcoli-private"
BRANCH = "master"
EXPERIMENT_ID = "test_cli_e2e_pytest"
GENERATIONS = 1
SEEDS = 1
OUTPUT_DIR = Path("test_cli_e2e_outputs")

# ---------------------------------------------------------------------------
# Pre-flight checks (run once at module load to decide skip)
# ---------------------------------------------------------------------------


def _api_is_reachable() -> bool:
    try:
        r = httpx.get(f"{API_BASE_URL}/health", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def _aws_credentials_valid() -> bool:
    try:
        result = subprocess.run(  # noqa: S603
            ["aws", "sts", "get-caller-identity"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=10,
            env={**os.environ, "AWS_PROFILE": os.environ.get("AWS_PROFILE", "stanford-sso")},
        )
        return result.returncode == 0
    except Exception:
        return False


_skip_no_api = pytest.mark.skipif(not _api_is_reachable(), reason=f"API not reachable at {API_BASE_URL}")
_skip_no_aws = pytest.mark.skipif(not _aws_credentials_valid(), reason="AWS credentials not configured")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_atlantis(*args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    """Run an atlantis CLI command and return the result."""
    cmd = ["uv", "run", "atlantis", *args, "--base-url", API_BASE_URL]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)  # noqa: S603
    return result


def _extract_id_from_output(output: str, pattern: str) -> int | None:
    """Extract a numeric ID from CLI output using a regex pattern."""
    match = re.search(pattern, output)
    if match:
        return int(match.group(1))
    return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@_skip_no_api
@_skip_no_aws
class TestCLIEndToEnd:
    """Full EUTE pipeline test via the Atlantis CLI.

    Tests run in order via pytest-ordering or explicit state passing.
    Each test stores results as class attributes for the next step.
    """

    # Shared state across test methods (set by earlier tests)
    simulator_id: int | None = None
    simulation_id: int | None = None

    # -- Step 1-3: Build simulator --

    def test_step_1_3_simulator_latest(self) -> None:
        """EUTE steps 1-3: Fetch latest commit, upload, and poll build to completion."""
        result = _run_atlantis(
            "simulator",
            "latest",
            "--repo-url",
            REPO_URL,
            "--branch",
            BRANCH,
            timeout=600,
        )

        assert result.returncode == 0, f"simulator latest failed:\n{result.stdout}\n{result.stderr}"

        # Extract simulator ID from output like "Simulator ID: 11"
        sid = _extract_id_from_output(result.stdout, r"Simulator ID:\s*(\d+)")
        assert sid is not None, f"Could not find Simulator ID in output:\n{result.stdout}"

        # Verify build completed
        assert "COMPLETED" in result.stdout, f"Build did not complete:\n{result.stdout}"

        TestCLIEndToEnd.simulator_id = sid

    # -- Step 4-5: Run simulation --

    def test_step_4_5_simulation_run(self) -> None:
        """EUTE steps 4-5: Submit simulation workflow and poll to completion."""
        assert TestCLIEndToEnd.simulator_id is not None, "Simulator ID not set (step 1-3 must pass first)"

        result = _run_atlantis(
            "simulation",
            "run",
            EXPERIMENT_ID,
            str(TestCLIEndToEnd.simulator_id),
            "--generations",
            str(GENERATIONS),
            "--seeds",
            str(SEEDS),
            "--run-parca",
            "--poll",
            timeout=2400,  # 40 min max for full pipeline
        )

        assert result.returncode == 0, f"simulation run failed:\n{result.stdout}\n{result.stderr}"

        # Extract simulation ID from output like "Simulation submitted!  ID: 35"
        sid = _extract_id_from_output(result.stdout, r"ID:\s*(\d+)")
        assert sid is not None, f"Could not find Simulation ID in output:\n{result.stdout}"

        # Verify simulation completed
        assert "COMPLETED" in result.stdout, f"Simulation did not complete:\n{result.stdout}"

        TestCLIEndToEnd.simulation_id = sid

    # -- Step 6: Download outputs --

    def test_step_6_simulation_outputs(self) -> None:
        """EUTE step 6: Download simulation output data."""
        assert TestCLIEndToEnd.simulation_id is not None, "Simulation ID not set (step 4-5 must pass first)"

        # Clean output dir
        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        result = _run_atlantis(
            "simulation",
            "outputs",
            str(TestCLIEndToEnd.simulation_id),
            "--dest",
            str(OUTPUT_DIR),
            timeout=300,
        )

        assert result.returncode == 0, f"simulation outputs failed:\n{result.stdout}\n{result.stderr}"
        assert "Saved simulation outputs to:" in result.stdout, f"No save confirmation:\n{result.stdout}"

        # Verify output directory has content
        assert OUTPUT_DIR.exists(), f"Output directory {OUTPUT_DIR} was not created"

        # Find the tar.gz archive
        archives = list(OUTPUT_DIR.glob("*.tar.gz"))
        assert len(archives) >= 1, f"No .tar.gz archive found in {OUTPUT_DIR}: {list(OUTPUT_DIR.iterdir())}"

        # Verify archive has analysis outputs
        archive = archives[0]
        with tarfile.open(archive, "r:gz") as tar:
            names = tar.getnames()

        tsv_files = [n for n in names if n.endswith(".tsv")]
        json_files = [n for n in names if n.endswith(".json")]
        analyses_dirs = [n for n in names if "analyses/" in n]

        assert len(tsv_files) > 0, f"No .tsv files in archive. Contents:\n{names}"
        assert len(json_files) > 0, f"No .json files in archive. Contents:\n{names}"
        assert len(analyses_dirs) > 0, f"No analyses/ directory in archive. Contents:\n{names}"

    # -- Verification: check simulation details via API --

    def test_verify_simulation_status(self) -> None:
        """Verify the completed simulation is queryable via CLI."""
        assert TestCLIEndToEnd.simulation_id is not None, "Simulation ID not set"

        result = _run_atlantis(
            "simulation",
            "status",
            str(TestCLIEndToEnd.simulation_id),
        )

        assert result.returncode == 0, f"simulation status failed:\n{result.stdout}\n{result.stderr}"
        assert "COMPLETED" in result.stdout.upper(), f"Status not COMPLETED:\n{result.stdout}"

    def test_verify_simulation_get(self) -> None:
        """Verify simulation metadata is retrievable."""
        assert TestCLIEndToEnd.simulation_id is not None, "Simulation ID not set"

        result = _run_atlantis(
            "simulation",
            "get",
            str(TestCLIEndToEnd.simulation_id),
        )

        assert result.returncode == 0, f"simulation get failed:\n{result.stdout}\n{result.stderr}"

        # The output should contain JSON with our experiment ID
        assert EXPERIMENT_ID in result.stdout, f"Experiment ID not in output:\n{result.stdout}"

    # -- Cleanup --

    @pytest.fixture(autouse=True, scope="class")
    def cleanup_outputs(self) -> Generator[None]:
        """Clean up downloaded outputs after all tests complete."""
        yield
        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
