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
DOCKERFILE_CORE = REPO_ROOT / "Dockerfile-core"

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


# --- no positional JSON patches in the supported overlays (core split, docs/plan-core.md P0) ---

SUPPORTED_OVERLAYS = ("sms-api-stanford-test", "sms-api-stanford", "viva-core-rke-dev", "viva-core-rke")


def _json_patch_ops(overlay: str) -> list[dict[str, Any]]:
    kustomization = REPO_ROOT / "kustomize" / "overlays" / overlay / "kustomization.yaml"
    doc: dict[str, Any] = yaml.safe_load(kustomization.read_text(encoding="utf-8"))
    ops: list[dict[str, Any]] = []
    for entry in doc.get("patches", []):
        body = yaml.safe_load(entry.get("patch", "")) if "patch" in entry else None
        if isinstance(body, list):  # a JSON patch is a list of ops; a strategic-merge patch is a mapping
            ops.extend(op for op in body if isinstance(op, dict))
    return ops


def test_supported_overlays_never_patch_a_list_by_index() -> None:
    """A JSON-patch path ending in ``/<n>`` addresses a list slot, not a thing.

    The Stanford overlays used to strip the SLURM/SSH wiring from ``base/api.yaml`` with
    ``remove .../env/5``, ``volumeMounts/2..0`` and ``volumes/2..0``. That is right only
    until someone inserts or reorders an entry in the base -- after which it silently
    deletes whatever moved into the slot. With a second Deployment about to be cloned from
    that base, by-index patches are a trap: use a strategic-merge ``$patch: delete`` keyed
    by name (``mountPath`` for volumeMounts). Appending with ``/-`` is fine.
    """
    offenders = [
        f"{overlay}: {op.get('op')} {op.get('path')}"
        for overlay in SUPPORTED_OVERLAYS
        for op in _json_patch_ops(overlay)
        if str(op.get("path", "")).rsplit("/", 1)[-1].isdigit()
    ]
    assert not offenders, "positional JSON patch(es) in a supported overlay:\n  " + "\n  ".join(offenders)


# --- every shipped top-level package reaches the image (core split, docs/plan-core.md P1) ---


def test_api_image_copies_every_package_the_wheel_ships() -> None:
    """``Dockerfile-api`` copies source trees one by one, so a NEW top-level package is
    absent from the image until someone adds its ``COPY`` -- and the failure is an
    ``ImportError`` at pod start, not at build. ``viva_core`` is the first package added
    since ``sms_api``; this keeps the wheel's package list and the image in step.
    """
    import tomllib

    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    shipped = set(pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["include"]) - {"tests"}
    assert "viva_core" in shipped, "viva_core must ship in the wheel"

    dockerfile = DOCKERFILE_API.read_text(encoding="utf-8")
    missing = sorted(pkg for pkg in shipped if f" {pkg} /app/{pkg}" not in dockerfile)
    assert not missing, f"Dockerfile-api has no COPY for shipped package(s): {missing}"


def test_core_image_carries_core_and_nothing_of_the_application() -> None:
    """``Dockerfile-core`` (plan-core §4b U2f) is core as its own service: it copies ``viva_core``
    and no other shipped package -- the application's absence from the image is the boundary a
    standalone core is held to (``core-is-standalone``), made physical."""
    import tomllib

    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    shipped = set(pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["include"]) - {"tests"}
    dockerfile = DOCKERFILE_CORE.read_text(encoding="utf-8")
    copied = {pkg for pkg in shipped if f" {pkg} /app/{pkg}" in dockerfile}
    assert copied == {"viva_core"}, f"Dockerfile-core copies {sorted(copied)}; it must copy viva_core alone"
    assert "--no-install-project" in dockerfile, "the wheel would need the application packages"
    assert "ENV UV_NO_DEFAULT_GROUPS=true" in dockerfile or "UV_NO_DEFAULT_GROUPS=true" in dockerfile
    assert 'uvicorn", "--factory", "viva_core.api.app:create_core_app"' in dockerfile
    assert '"uv", "run"' not in dockerfile, "`uv run` would try to install the project at container start"


# --- the core overlays' known_hosts must name the submit host the way asyncssh looks it up ---

CORE_OVERLAYS = tuple(o for o in SUPPORTED_OVERLAYS if o.startswith("viva-core-"))


def _yaml_docs(path: Path) -> list[dict[str, Any]]:
    return [d for d in yaml.safe_load_all(path.read_text(encoding="utf-8")) if isinstance(d, dict)]


def _core_known_hosts(overlay: str) -> tuple[str, str, list[str]]:
    """(submit host, ConfigMap name, its known_hosts lines) as the rendered core Deployment sees them."""
    overlay_dir = REPO_ROOT / "kustomize" / "overlays" / overlay
    kustomization = yaml.safe_load((overlay_dir / "kustomization.yaml").read_text(encoding="utf-8"))
    namespace = kustomization["namespace"]
    env_text = (REPO_ROOT / "kustomize" / "config" / overlay / "core.env").read_text(encoding="utf-8")
    hosts = [line.split("=", 1)[1].strip() for line in env_text.splitlines() if line.startswith("SLURM_SUBMIT_HOST=")]
    assert len(hosts) == 1, f"{overlay}: expected exactly one SLURM_SUBMIT_HOST, found {hosts}"

    configmap_name = "ssh-known-hosts"  # kustomize/base/viva-core/core.yaml's default
    for entry in kustomization.get("patches", []):
        body = yaml.safe_load(entry.get("patch", "")) if "patch" in entry else None
        if not isinstance(body, dict) or body.get("kind") != "Deployment":
            continue
        for volume in body.get("spec", {}).get("template", {}).get("spec", {}).get("volumes", []):
            if volume.get("name") == "ssh-known-hosts" and "configMap" in volume:
                configmap_name = volume["configMap"]["name"]

    candidates = [
        *overlay_dir.glob("*.yaml"),
        REPO_ROOT / "kustomize" / "config" / namespace / "ssh-known-hosts-configmap.yaml",
    ]
    for path in candidates:
        if not path.is_file() or path.name == "kustomization.yaml":
            continue
        for doc in _yaml_docs(path):
            if doc.get("kind") == "ConfigMap" and doc.get("metadata", {}).get("name") == configmap_name:
                return hosts[0], configmap_name, doc["data"]["known_hosts"].splitlines()
    raise AssertionError(f"{overlay}: no ConfigMap {configmap_name!r} found among {[str(p) for p in candidates]}")


def test_core_overlays_known_hosts_name_the_submit_host_bare() -> None:
    """asyncssh matches a default-port host by its bare name (``haproxy-ssh``); it looks up
    ``[host]:port`` only for a non-default port. Prod's SMS ``ssh-known-hosts`` spells the entry
    ``[haproxy-ssh]:22`` and so trusts nothing -- unnoticed there because the SMS pod leaves
    ``SLURM_SUBMIT_KNOWN_HOSTS`` unset, but fatal for core (checkpoint UC, 2026-09-26: every SSH
    session failed with ``Host key is not trusted for host haproxy-ssh``). Each core overlay's
    known_hosts must carry a line whose host field is exactly ``SLURM_SUBMIT_HOST``."""
    for overlay in CORE_OVERLAYS:
        host, configmap_name, lines = _core_known_hosts(overlay)
        bare = [line for line in lines if line.strip() and host in line.split()[0].split(",")]
        assert bare, (
            f"{overlay}: ConfigMap {configmap_name!r} has no known_hosts line for bare host {host!r} "
            f"(asyncssh never matches `[{host}]:22` on port 22); lines: {lines}"
        )
