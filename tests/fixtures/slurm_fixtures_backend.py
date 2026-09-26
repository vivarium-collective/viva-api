"""Backend selection for the SSH / SLURM tests (``docs/plan-core.md`` §4b, U1).

One test body runs against either backend:

* ``container`` -- a throwaway SLURM cluster from ``tests/fixtures/slurm_cluster``, needing only
  Docker: one controller with sshd, one worker, accounting, Apptainer inside. This is what CI uses.
* ``cluster``   -- the real submit host named in settings (Mantis, through the UConn VPN), needing
  a key. Opt in with ``--slurm-backend cluster``.

Selection is per-test parameterisation (``tests/conftest.py``), not a fork in the test body, so
the two backends cannot drift: there is exactly one body. Differences between them are DATA on
:class:`SlurmBackend`, never a branch on ``kind``.

Ported from compose-api's harness (Jim, 2026-09-11/12) -- the compose file and the two config
files are verbatim; only this module speaks viva-api. The switch is two things at once: the
SLURM ``SSHSessionService`` registered under ``SSHTarget.SLURM`` (what the services ask
``dependencies`` for), and the SLURM fields of the settings object (what sbatch templates and
remote paths are built from), both pointed at the cluster for the whole session and restored after.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import asyncssh
import pytest

from viva_api.common.models import SSHTarget
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service_or_none, set_ssh_session_service
from viva_core.infra.ssh.ssh_service import SSHSessionService
from viva_core.storage.file_paths import HPCFilePath

COMPOSE_DIR = Path(__file__).parent / "slurm_cluster"
CONTAINER_USER = "root"
#: Facts of the upstream image (giovtorres/slurm-docker-cluster), not of this project.
CONTAINER_PARTITION = "cpu"
CONTAINER_QOS = "normal"  # slurmdbd creates it; naming it keeps the sbatch templates identical to production
CONTAINER_BUILD_NODE = "c1"
CONTAINER_REMOTE_BASE = Path("/data/viva-api")

#: The SLURM settings the switch points at the container, restored after the session.
_OVERRIDDEN_SETTINGS = (
    "slurm_submit_host",
    "slurm_submit_port",
    "slurm_submit_user",
    "slurm_submit_key_path",
    "slurm_submit_known_hosts",
    "slurm_partition",
    "slurm_qos",
    "slurm_node_list",
    "slurm_log_base_path",
    "slurm_base_path",
    # what the SLURM compose service derives its remote paths from (viva_core.compose.hpc_paths)
    "compose_image_base_path",
    "compose_sim_base_path",
)


@dataclass(frozen=True)
class SlurmBackend:
    """What a test needs to know about the scheduler it is talking to. Read these instead of
    reaching into settings, so a new difference between the backends is a field here -- visible
    in review -- rather than a branch."""

    kind: Literal["container", "cluster"]
    partition: str
    qos: str
    node_list: str
    remote_base: Path
    can_build_singularity: bool
    ssh: SSHSessionService

    @property
    def log_dir(self) -> HPCFilePath:
        return HPCFilePath(remote_path=self.remote_base / "htclogs")


@dataclass(frozen=True)
class _ContainerCluster:
    port: int
    key_path: Path
    env: dict[str, str]


def _compose(*args: str, env: dict[str, str], check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run ``docker compose`` and, on failure, say what it printed: a plain ``check=True`` names the
    command and throws away compose's own diagnosis (an image that would not pull, a port already
    bound)."""
    result = subprocess.run(  # noqa: S603
        ["docker", "compose", *args],  # noqa: S607
        cwd=COMPOSE_DIR,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"`docker compose {' '.join(args)}` exited {result.returncode}\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
        )
    return result


def docker_available() -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "info"],  # noqa: S607
                capture_output=True,
                check=False,
                timeout=15,
            ).returncode
            == 0
        )
    except (OSError, subprocess.SubprocessError):
        return False


def cluster_credentials_present() -> bool:
    settings = get_settings()
    return bool(settings.slurm_submit_host) and os.path.isfile(os.path.expanduser(settings.slurm_submit_key_path))


async def _ssh_probe(port: int, key_path: Path) -> None:
    async with asyncssh.connect(
        host="127.0.0.1", port=port, username=CONTAINER_USER, client_keys=[str(key_path)], known_hosts=None
    ) as conn:
        await conn.run("true", check=True)


def _wait_for_ssh(port: int, key_path: Path, env: dict[str, str], timeout_seconds: float = 90.0) -> None:
    """``docker compose up --wait`` waits for the healthchecks, and slurmctld's is ``scontrol ping``,
    which says nothing about sshd: the container can be healthy while SSH is not yet usable. Probe
    the real thing, and on failure hand back the container's own logs instead of every SSH test
    failing separately with ``ConnectionLost``."""
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            asyncio.run(_ssh_probe(port, key_path))
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    ps = _compose("ps", env=env, check=False).stdout
    logs = _compose("logs", "--no-color", "--tail", "60", "slurmctld", env=env, check=False).stdout
    raise RuntimeError(
        f"the SLURM container never accepted SSH on 127.0.0.1:{port} within {timeout_seconds:.0f}s; "
        f"last error was {last_error!r}\n\n{ps}\n\n{logs}"
    )


@pytest.fixture(scope="session")
def _container_cluster(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_ContainerCluster]:
    """Bring up the throwaway cluster once per session and yield its connection details."""
    workdir = tmp_path_factory.mktemp("slurm-cluster")
    key_path = workdir / "id_ed25519"
    subprocess.run(  # noqa: S603
        ["ssh-keygen", "-t", "ed25519", "-N", "", "-q", "-f", str(key_path)],  # noqa: S607
        check=True,
        capture_output=True,
    )
    authorized_keys = workdir / "authorized_keys"
    authorized_keys.write_text(key_path.with_suffix(".pub").read_text())
    authorized_keys.chmod(0o644)

    # A unique project name per session keeps concurrent runs from colliding, and the worker
    # entrypoint needs the same value to derive its node name (see the compose file).
    project = f"vivaapi-slurm-{uuid.uuid4().hex[:8]}"
    env = {**os.environ, "SSH_AUTHORIZED_KEYS": str(authorized_keys), "COMPOSE_PROJECT_NAME": project}
    _compose("down", "-v", "--remove-orphans", env=env, check=False)
    _compose("up", "-d", "--wait", env=env)
    try:
        # `docker compose port` can print one line per address family; both carry the same port.
        published = _compose("port", "slurmctld", "22", env=env).stdout.strip().splitlines()[0]
        port = int(published.rsplit(":", 1)[1])
        _wait_for_ssh(port=port, key_path=key_path, env=env)
        yield _ContainerCluster(port=port, key_path=key_path, env=env)
    finally:
        _compose("down", "-v", "--remove-orphans", env=env, check=False)


def _provision_remote_tree(env: dict[str, str]) -> None:
    """The directory tree an administrator made once on the real cluster. Through ``docker compose
    exec`` rather than SSH: this fixture is synchronous, and ``/data`` is a shared volume the
    worker sees too."""
    dirs = [str(CONTAINER_REMOTE_BASE / name) for name in ("htclogs", "sims", "images", "compose")]
    _compose("exec", "-T", "slurmctld", "mkdir", "-p", *dirs, env=env)


@pytest.fixture(scope="session")
def slurm_backend(request: pytest.FixtureRequest) -> Iterator[SlurmBackend]:
    """The selected backend, with the SLURM SSH service registered and the settings pointed at it
    for the whole session."""
    kind: str = request.param
    settings = get_settings()
    previous_ssh = get_ssh_session_service_or_none(SSHTarget.SLURM)
    saved = {name: getattr(settings, name) for name in _OVERRIDDEN_SETTINGS}
    try:
        if kind == "cluster":
            if not cluster_credentials_present():
                pytest.skip("slurm_submit_host / slurm_submit_key_path are unset or missing; cannot reach the cluster")
            ssh = SSHSessionService(
                hostname=settings.slurm_submit_host,
                port=settings.slurm_submit_port,
                username=settings.slurm_submit_user,
                key_path=Path(os.path.expanduser(settings.slurm_submit_key_path)),
                known_hosts=Path(settings.slurm_submit_known_hosts) if settings.slurm_submit_known_hosts else None,
            )
            set_ssh_session_service(ssh, name=SSHTarget.SLURM)
            yield SlurmBackend(
                kind="cluster",
                partition=settings.slurm_partition,
                qos=settings.slurm_qos,
                node_list=settings.slurm_node_list,
                remote_base=Path(settings.slurm_base_path.remote_path),
                can_build_singularity=True,
                ssh=ssh,
            )
            return

        details: _ContainerCluster = request.getfixturevalue("_container_cluster")
        ssh = SSHSessionService(
            hostname="127.0.0.1", port=details.port, username=CONTAINER_USER, key_path=details.key_path
        )
        settings.slurm_submit_host = "127.0.0.1"
        settings.slurm_submit_port = details.port
        settings.slurm_submit_user = CONTAINER_USER
        settings.slurm_submit_key_path = str(details.key_path)
        settings.slurm_submit_known_hosts = None
        settings.slurm_partition = CONTAINER_PARTITION
        settings.slurm_qos = CONTAINER_QOS
        settings.slurm_node_list = ""
        settings.slurm_log_base_path = HPCFilePath(remote_path=CONTAINER_REMOTE_BASE / "htclogs")
        settings.slurm_base_path = HPCFilePath(remote_path=CONTAINER_REMOTE_BASE)
        settings.compose_image_base_path = str(CONTAINER_REMOTE_BASE / "images")
        settings.compose_sim_base_path = str(CONTAINER_REMOTE_BASE / "compose")
        set_ssh_session_service(ssh, name=SSHTarget.SLURM)
        _provision_remote_tree(env=details.env)
        yield SlurmBackend(
            kind="container",
            partition=CONTAINER_PARTITION,
            qos=CONTAINER_QOS,
            node_list="",
            remote_base=CONTAINER_REMOTE_BASE,
            # Measured on compose-api's identical cluster, not assumed: with the subordinate id range
            # mounted in, the container builds a definition with --fakeroot and runs the result from
            # inside a job. What it lacks is the science images, which is why those stay cluster_only.
            can_build_singularity=True,
            ssh=ssh,
        )
    finally:
        for name, value in saved.items():
            setattr(settings, name, value)
        set_ssh_session_service(previous_ssh, name=SSHTarget.SLURM)
