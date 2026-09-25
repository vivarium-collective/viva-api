"""Compose simulation service — submits process-bigraph jobs to SLURM via SSH.

What the service knows: how to turn a composite request into an sbatch script that runs it in a
Singularity container, how to build that container from its definition, and how to fetch the
results archive the job zipped. What it does NOT know (U2b-1): any one science image's way of
being run. That arrives as a hook, ``ContainerRun``, handed in by the composition root; with no
hook, or a hook that declines, the generic ``singularity run`` is used.
"""

import logging
import random
import string
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from textwrap import dedent
from typing import Protocol, override

from viva_core.backends.slurm_service import SlurmService
from viva_core.compose.database_service import ComposeDatabaseService
from viva_core.compose.hpc_paths import (
    get_compose_correlation_id,
    get_compose_experiment_dir,
    get_compose_sim_input_path,
    get_compose_sim_results_path,
    get_compose_singularity_container_file,
    get_compose_singularity_def_file,
    get_compose_slurm_log_file,
    get_compose_slurm_submit_file,
)
from viva_core.compose.models import (
    ComposeHpcRun,
    ComposeJobType,
    ComposeSimulation,
    ComposeSimulatorVersion,
)
from viva_core.compose.service import ComposeSimulationService
from viva_core.infra.ssh.ssh_service import SSHSessionService
from viva_core.models import JobBackend
from viva_core.settings import CoreSettings, get_core_settings
from viva_core.storage.file_paths import HPCFilePath

logger = logging.getLogger(__name__)

#: Where the results archives fetched from the HPC side are kept, by default: the deployment's
#: results-cache mount. The composition root hands in the site's own directory.
DEFAULT_RESULTS_CACHE_DIR = Path("/app/.results_cache/compose")


@dataclass(frozen=True)
class RunPlan:
    """What a hook wants run instead of the generic command: the command line the sbatch script
    executes, and files to place in the experiment directory first (``name -> content``)."""

    command: str
    files: dict[str, str] = field(default_factory=dict)


class ContainerRun(Protocol):
    """An application's way of running one of its images (U2b-1). Given the run's override
    command (opaque to the service), the bind clause and container the sbatch script will use,
    the job name and the request, answer with a plan -- or ``None`` to have the service run its
    generic ``singularity run``."""

    def __call__(
        self,
        override_command: str | None,
        bind_clause: str,
        container: Path,
        job_name: str,
        simulation: ComposeSimulation,
    ) -> RunPlan | None: ...


class ComposeSimulationServiceHpc(ComposeSimulationService):
    env: CoreSettings
    backend = JobBackend.SLURM

    def __init__(
        self,
        env: CoreSettings | None = None,
        *,
        slurm_ssh: Callable[[], SSHSessionService] | None = None,
        run_command: ContainerRun | None = None,
        results_cache_dir: Path = DEFAULT_RESULTS_CACHE_DIR,
    ) -> None:
        self.env = env or get_core_settings()
        # The SSH sessions jobs are submitted over, HANDED IN as a provider (P3d-3).
        self._slurm_ssh = slurm_ssh
        # The application's run command for its own images, if any (U2b-1).
        self._run_command = run_command
        self._results_cache_dir = results_cache_dir

    def _ssh_sessions(self) -> SSHSessionService:
        if self._slurm_ssh is None:
            raise RuntimeError("No SLURM SSH session provider was handed to the SLURM compose service.")
        return self._slurm_ssh()

    async def results_archive(self, experiment_id: str) -> Path | None:
        """The zip the HPC-side job runner built, downloaded over SSH into the local results cache
        (once; later calls find it). Raises ``RuntimeError`` when the download fails."""
        remote_path = get_compose_sim_results_path(experiment_id)
        cache_dir = self._results_cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)
        local_path = cache_dir / f"{experiment_id}_results.zip"
        if not local_path.exists():
            async with self._ssh_sessions().session() as ssh:
                await ssh.scp_download(local_file=local_path, remote_path=HPCFilePath(remote_path=remote_path))
        return local_path if local_path.exists() else None

    def _run_plan(
        self,
        override_command: str | None,
        bind_clause: str,
        singularity_container: Path,
        slurm_job_name: str,
        simulation: ComposeSimulation,
    ) -> RunPlan:
        """The hook's plan when it has one; else the generic ``singularity run`` of the input file."""
        if self._run_command is not None:
            plan = self._run_command(override_command, bind_clause, singularity_container, slurm_job_name, simulation)
            if plan is not None:
                return plan
        indent = " " * 16
        return RunPlan(
            command=(
                f"singularity run \\\n"
                f"{indent}    --compat \\\n"
                f"{indent}    {bind_clause} \\\n"
                f"{indent}    {singularity_container} \\\n"
                f"{indent}    /experiment/{slurm_job_name}."
                f"{simulation.sim_request.simulation_file_type.get_files_suffix()} \\\n"
                f'{indent}    -o "{self.env.compose_containers_output_dir}" \\\n'
                f"{indent}    -n {simulation.sim_request.end_time_point}"
            )
        )

    @override
    async def submit_simulation_job(
        self, simulation: ComposeSimulation, experiment_id: str, override_command: str | None = None
    ) -> str:
        if simulation.sim_request.request_file_path is None:
            raise RuntimeError("Simulation request file path is not available.")

        slurm_job_name = experiment_id
        singularity_container = get_compose_singularity_container_file(
            singularity_hash=simulation.simulator_version.singularity_def_hash
        )
        experiment_path = get_compose_experiment_dir(experiment_id=slurm_job_name)

        with tempfile.TemporaryDirectory() as tmpdir:
            local_submit_file = Path(tmpdir) / f"{slurm_job_name}.sbatch"
            qos_clause = f"#SBATCH --qos={self.env.slurm_qos}" if self.env.slurm_qos else ""
            nodelist_clause = f"#SBATCH --nodelist={self.env.slurm_node_list}" if self.env.slurm_node_list else ""

            bind_args = [f"--bind {experiment_path}:/experiment"]
            if self.env.compose_cache_base_path:
                bind_args.append(f"--bind {self.env.compose_cache_base_path}:/out/cache")
            bind_clause = " \\\n                    ".join(bind_args)

            plan = self._run_plan(override_command, bind_clause, singularity_container, slurm_job_name, simulation)
            run_cmd = plan.command
            script_content = dedent(f"""\
                #!/bin/bash
                #SBATCH --job-name={slurm_job_name}
                #SBATCH --time=30:00
                #SBATCH --cpus-per-task {"1" if simulation.sim_request.is_batch else "2"}
                #SBATCH --mem={"1GB" if simulation.sim_request.is_batch else "8GB"}
                #SBATCH --partition={self.env.slurm_partition}
                {qos_clause}
                {nodelist_clause}
                #SBATCH --output={get_compose_slurm_log_file(slurm_job_name=slurm_job_name)}

                set -e

                mkdir -p {experiment_path}/output
                echo "Simulation {slurm_job_name} running."
                {run_cmd}

                pushd {experiment_path}
                cd output
                zip -r ../results.zip ./*
                cd ..
                rm -r output
                popd
                echo "Simulation run completed. data saved to {experiment_path!s}."
                """)
            local_submit_file.write_text(script_content)

            # The plan's files (an application's runner script, say), written locally for upload.
            plan_files: list[tuple[Path, str]] = []
            for name, content in plan.files.items():
                local_file = Path(tmpdir) / name
                local_file.write_text(content)
                plan_files.append((local_file, name))

            async with self._ssh_sessions().session() as ssh:
                await ssh.run_command(f"mkdir -p {experiment_path}")
                # Upload the simulation input file (OMEX/PBG/SBML)
                remote_input = HPCFilePath(remote_path=get_compose_sim_input_path(experiment_id=slurm_job_name))
                await ssh.scp_upload(local_file=simulation.sim_request.request_file_path, remote_path=remote_input)
                for local_file, name in plan_files:
                    await ssh.scp_upload(
                        local_file=local_file, remote_path=HPCFilePath(remote_path=experiment_path / name)
                    )
                slurm_service = SlurmService()
                remote_sbatch = HPCFilePath(remote_path=get_compose_slurm_submit_file(slurm_job_name=slurm_job_name))
                slurm_jobid = await slurm_service.submit_job(
                    ssh,
                    local_sbatch_file=local_submit_file,
                    remote_sbatch_file=remote_sbatch,
                )
                return str(slurm_jobid)

    @override
    async def build_container(
        self, simulator_version: ComposeSimulatorVersion, random_str: str, db_service: ComposeDatabaseService
    ) -> ComposeHpcRun:
        rand_string = "".join(random.choices(string.hexdigits, k=5))
        slurm_job_name = f"singularity_build_{simulator_version.singularity_def_hash[:5]}_{rand_string}"
        singularity_container = get_compose_singularity_container_file(
            singularity_hash=simulator_version.singularity_def_hash
        )
        singularity_def_file = get_compose_singularity_def_file(singularity_hash=simulator_version.singularity_def_hash)

        with tempfile.TemporaryDirectory() as tmpdir:
            local_singularity_file = Path(tmpdir) / "singularity.def"
            local_singularity_file.write_text(simulator_version.singularity_def.representation)

            local_submit_file = Path(tmpdir) / f"{slurm_job_name}.sbatch"
            qos_clause = f"#SBATCH --qos={self.env.slurm_qos}" if self.env.slurm_qos else ""
            nodelist_clause = f"#SBATCH --nodelist={self.env.slurm_node_list}" if self.env.slurm_node_list else ""
            script_content = dedent(f"""\
                #!/bin/bash
                #SBATCH --job-name={slurm_job_name}
                #SBATCH --time=30:00
                #SBATCH --cpus-per-task 1
                #SBATCH --mem=4GB
                #SBATCH --partition={self.env.slurm_partition}
                {qos_clause}
                {nodelist_clause}
                #SBATCH --output={get_compose_slurm_log_file(slurm_job_name=slurm_job_name)}

                set -e
                echo "Starting build for container {singularity_container}"
                pushd /tmp
                mv {singularity_def_file} /tmp/{singularity_def_file.name}
                singularity build --fakeroot {singularity_container.name} {singularity_def_file.name}
                mv {singularity_container.name} {singularity_container}
                mv {singularity_def_file.name} {singularity_def_file}
                popd
                echo "Finished building container."
                """)
            local_submit_file.write_text(script_content)

            async with self._ssh_sessions().session() as ssh:
                slurm_service = SlurmService()
                remote_def = HPCFilePath(remote_path=singularity_def_file)
                await ssh.scp_upload(local_file=local_singularity_file, remote_path=remote_def)
                remote_sbatch = HPCFilePath(remote_path=get_compose_slurm_submit_file(slurm_job_name=slurm_job_name))
                slurm_jobid = await slurm_service.submit_job(
                    ssh,
                    local_sbatch_file=local_submit_file,
                    remote_sbatch_file=remote_sbatch,
                )

            hpc_run = await db_service.get_hpc_db().insert_hpcrun(
                slurmjobid=slurm_jobid,
                job_type=ComposeJobType.BUILD_CONTAINER,
                ref_id=simulator_version.database_id,
                correlation_id=get_compose_correlation_id(
                    random_string=random_str, job_type=ComposeJobType.BUILD_CONTAINER
                ),
            )
            return hpc_run
