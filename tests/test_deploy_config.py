"""Static sanity checks on kustomize deploy config that has no natural home
in the application test tree — catches config drift a Python test otherwise
never would.

See kustomize/base/workbench/workbench.yaml's own comment for the incident
this guards: the smscdk workbench deployment's pinned remote-run target
regressed to vivarium-collective/v2ecoli (a structurally-diverged sibling
repo) instead of the canonical CovertLabEcoli/sms-ecoli, undetected until an
independent live-session check caught it before any real dispatch.
"""

from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKBENCH_BASE = REPO_ROOT / "kustomize" / "base" / "workbench" / "workbench.yaml"
DOCKERFILE_API = REPO_ROOT / "Dockerfile-api"

CANONICAL_REMOTE_REPO_URL = "https://github.com/CovertLabEcoli/sms-ecoli"


def _workbench_container_env() -> list[dict[str, Any]]:
    docs: list[Any] = list(yaml.safe_load_all(WORKBENCH_BASE.read_text(encoding="utf-8")))
    deployments = [d for d in docs if d and d.get("kind") == "Deployment"]
    assert len(deployments) == 1, f"expected exactly one Deployment doc in {WORKBENCH_BASE}"
    containers = deployments[0]["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1, f"expected exactly one container in {WORKBENCH_BASE}"
    env: list[dict[str, Any]] = containers[0]["env"]
    return env


def _env_value(env: list[dict[str, Any]], name: str) -> str:
    matches: list[str] = [e["value"] for e in env if e.get("name") == name]
    assert len(matches) == 1, f"expected exactly one {name} entry, found {len(matches)}"
    return matches[0]


def test_workbench_remote_pinned_repo_is_sms_ecoli() -> None:
    """The deployed workbench's pinned-remote-run target must be sms-ecoli,
    never v2ecoli — the canonical workflow's own workspace requirement.
    Regression test for the 2026-08-11 live mismatch: this base manifest's
    VIVARIUM_WORKBENCH_REMOTE_REPO_URL silently pointed at
    vivarium-collective/v2ecoli, so every session on the smscdk deployment
    that never explicitly called /api/source/switch-build inherited the
    wrong dispatch target with zero warning until independently checked.
    """
    env = _workbench_container_env()
    assert _env_value(env, "VIVARIUM_WORKBENCH_REMOTE_REPO_URL") == CANONICAL_REMOTE_REPO_URL
    assert _env_value(env, "VIVARIUM_WORKBENCH_REMOTE_BRANCH") == "main"
    assert _env_value(env, "VIVARIUM_WORKBENCH_REMOTE_PINNED") == "1"


def test_api_image_excludes_dev_and_docs_dependency_groups() -> None:
    """The runtime api image must not ship the dev/docs dependency groups.

    Guards BOTH halves, because either alone is insufficient:

    * ``ENV UV_NO_DEFAULT_GROUPS=true`` — the load-bearing one. Every command
      the image runs goes through ``uv run`` (the CMD, and the alembic-migrate
      Job's ``uv run python -m viva_api.simulation.db_reconcile``), and
      ``uv run`` re-syncs against ``[tool.uv] default-groups = ["dev", "docs"]``
      before executing. Without this env var a ``--no-default-groups`` build is
      inert: the dev group is reinstalled at container start (measured: 37
      packages), which silently restores pytest/debugpy/ipdb in production AND
      makes startup depend on reaching the package index.
    * ``--no-default-groups`` on every ``uv sync`` — so the groups are never
      written into the image layers in the first place.

    Note ``--no-dev`` is NOT sufficient in place of ``--no-default-groups``:
    it excludes only the ``dev`` group, leaving the separate ``docs`` group
    (sphinx et al.) installed.
    """
    dockerfile = DOCKERFILE_API.read_text(encoding="utf-8")

    assert "ENV UV_NO_DEFAULT_GROUPS=true" in dockerfile, (
        "Dockerfile-api must set UV_NO_DEFAULT_GROUPS=true; without it `uv run` "
        "reinstalls the dev group at container start and the build-time flags are inert"
    )

    sync_lines = [
        line.strip() for line in dockerfile.splitlines() if "uv sync" in line and not line.lstrip().startswith("#")
    ]
    assert sync_lines, "expected at least one `uv sync` line in Dockerfile-api"
    for line in sync_lines:
        assert "--no-default-groups" in line, f"`uv sync` line must pass --no-default-groups: {line!r}"
