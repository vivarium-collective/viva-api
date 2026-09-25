"""The SLURM implementation of :class:`~viva_core.backends.base.JobBackend` (U2g): a job is an
sbatch script written from the spec, submitted over SSH, and asked about through ``squeue`` and
``scontrol``. The second implementation, and the one a SLURM site (UConn) runs on.

What it assumes of the site: a submit host reachable by SSH (``sessions``), a directory it may
write under (``work_dir``: the scripts go to ``sbatch/``, the output to ``logs/``), and, for a spec
with an ``image``, Apptainer/Singularity on the compute nodes -- the runtime's command name is a
setting because sites differ (``singularity`` is a symlink on most). Exercised against a real
scheduler in ``tests/core/test_job_backend_slurm.py`` (the Docker cluster in CI, Mantis with
``--slurm-backend cluster``).
"""

from __future__ import annotations

import logging
import shlex
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from textwrap import dedent

from viva_core.backends.base import BackendStatus, JobHandle, JobSpec
from viva_core.backends.models import SlurmJob
from viva_core.backends.slurm_service import SlurmService
from viva_core.infra.ssh.ssh_service import SSHSessionService
from viva_core.models import JobStatus
from viva_core.storage.file_paths import HPCFilePath

logger = logging.getLogger(__name__)

KIND = "slurm"


def _exit_code(slurm_exit_code: str | None) -> int | None:
    """SLURM reports ``<exit>:<signal>``; the exit is what a caller means."""
    if not slurm_exit_code:
        return None
    head = slurm_exit_code.split(":", 1)[0]
    return int(head) if head.lstrip("-").isdigit() else None


def _to_status(job: SlurmJob) -> BackendStatus:
    return BackendStatus(
        status=JobStatus.from_slurm_state(job.job_state),
        exit_code=_exit_code(job.exit_code),
        reason=job.reason,
        start_time=job.start_time,
        end_time=job.end_time,
    )


class SlurmJobBackend:
    kind = KIND

    def __init__(
        self,
        sessions: Callable[[], SSHSessionService],
        *,
        partition: str,
        work_dir: Path,
        qos: str = "",
        node_list: str = "",
        container_runtime: str = "singularity",
    ) -> None:
        self._sessions = sessions
        self._partition = partition
        self._work_dir = work_dir
        self._qos = qos
        self._node_list = node_list
        self._container_runtime = container_runtime
        self._slurm = SlurmService()

    # ── the script ─────────────────────────────────────────────────────────

    def output_path(self, handle: JobHandle) -> Path:
        """Where the job's stdout+stderr land: ``%x-%j`` in the directive, so the path follows
        from a handle without asking the scheduler."""
        return self._work_dir / "logs" / f"{handle.name}-{handle.id}.out"

    def render(self, spec: JobSpec) -> str:
        """The sbatch script for a spec: the directives the site needs, the env exported, then the
        command -- inside the image through the container runtime when the spec names one, bare
        otherwise. Public so a test can read what would be submitted."""
        qos = f"#SBATCH --qos={self._qos}" if self._qos else ""
        nodelist = f"#SBATCH --nodelist={self._node_list}" if self._node_list else ""
        depends = ""
        if spec.depends_on:
            foreign = [h for h in spec.depends_on if h.backend != self.kind]
            if foreign:
                raise ValueError(f"a SLURM job can only wait on SLURM jobs; got {foreign}")
            depends = "#SBATCH --dependency=afterok:" + ":".join(h.id for h in spec.depends_on)
        exports = "\n".join(f"export {name}={shlex.quote(value)}" for name, value in spec.env.items())
        if spec.image:
            run = f"{self._container_runtime} exec {shlex.quote(spec.image)} sh -c {shlex.quote(spec.command)}"
        else:
            run = spec.command
        r = spec.resources
        return dedent(f"""\
            #!/bin/bash
            #SBATCH --job-name={spec.name}
            #SBATCH --output={self._work_dir}/logs/%x-%j.out
            #SBATCH --partition={self._partition}
            {qos}
            {nodelist}
            {depends}
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task={r.cpus}
            #SBATCH --mem={r.memory_mb}M
            #SBATCH --time={r.time_minutes}
            {exports}
            {run}
            """)

    # ── the Protocol ───────────────────────────────────────────────────────

    async def submit(self, spec: JobSpec) -> JobHandle:
        with tempfile.TemporaryDirectory() as tmpdir:
            local = Path(tmpdir) / f"{spec.name}.sbatch"
            local.write_text(self.render(spec))
            remote = HPCFilePath(remote_path=self._work_dir / "sbatch" / local.name)
            async with self._sessions().session() as ssh:
                await ssh.run_command(f"mkdir -p {self._work_dir / 'logs'}")
                job_id = await self._slurm.submit_job(ssh, local_sbatch_file=local, remote_sbatch_file=remote)
        logger.info("Submitted SLURM job %s (id=%s) to partition %s", spec.name, job_id, self._partition)
        return JobHandle(backend=self.kind, id=str(job_id), name=spec.name)

    async def status(self, handles: Sequence[JobHandle]) -> dict[str, BackendStatus]:
        """``squeue`` for what is queued or running, ``scontrol`` for what has just finished (it
        still answers for a while after ``squeue`` drops the job); ``scontrol`` wins where both
        answer because it carries the exit code and the times."""
        ids = [int(h.id) for h in handles]
        if not ids:
            return {}
        async with self._sessions().session() as ssh:
            live = await self._slurm.get_job_status_squeue(ssh, ids)
            finished = await self._slurm.get_job_status_scontrol(ssh, ids)
        by_id = {job.job_id: job for job in live}
        by_id.update({job.job_id: job for job in finished})
        return {str(job_id): _to_status(job) for job_id, job in by_id.items() if job.job_state}

    async def cancel(self, handle: JobHandle) -> None:
        async with self._sessions().session() as ssh:
            retcode, _, stderr = await ssh.run_command(f"scancel {handle.id}")
        if retcode != 0:
            raise RuntimeError(f"scancel {handle.id} exited {retcode}: {stderr.strip()}")

    async def logs(self, handle: JobHandle, *, tail: int | None = None) -> list[str]:
        path = self.output_path(handle)
        command = f"tail -n {int(tail)} {path}" if tail is not None else f"cat {path}"
        async with self._sessions().session() as ssh:
            retcode, out, _ = await ssh.run_command(f"test -f {path} && {command} || true")
        return out.splitlines() if retcode == 0 else []
