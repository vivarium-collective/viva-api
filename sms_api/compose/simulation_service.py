"""Compose simulation service — submits process-bigraph jobs to SLURM via sms-api SSH."""

import json
import logging
import random
import string
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from textwrap import dedent
from typing import override

from sms_api.common.hpc.slurm_service import SlurmService
from sms_api.common.models import JobBackend, SSHTarget
from sms_api.common.storage.file_paths import HPCFilePath
from sms_api.compose.database_service import ComposeDatabaseService
from sms_api.compose.hpc_utils import (
    get_compose_correlation_id,
    get_compose_experiment_dir,
    get_compose_sim_input_path,
    get_compose_singularity_container_file,
    get_compose_singularity_def_file,
    get_compose_slurm_log_file,
    get_compose_slurm_submit_file,
)
from sms_api.compose.models import (
    ComposeHpcRun,
    ComposeJobStatus,
    ComposeJobType,
    ComposeSimulation,
    ComposeSimulatorVersion,
)
from sms_api.config import Settings, get_settings
from sms_api.dependencies import get_ssh_session_service

logger = logging.getLogger(__name__)


class ComposeSimulationService(ABC):
    # Which JobBackend this service submits to — tags the ComposeHpcRun so status
    # polling knows whether to query SLURM (via SSH) or AWS Batch (describe_jobs).
    backend: "JobBackend"
    # SLURM builds a per-def Singularity container before the run; prebuilt-image
    # backends (Ray/Batch) skip that build-and-wait step in _dispatch_compose_job.
    requires_container_build: bool = True

    @abstractmethod
    async def submit_simulation_job(
        self, simulation: ComposeSimulation, experiment_id: str, override_command: str | None = None
    ) -> str:
        """Submit the run; return the backend job id as a string (SLURM int-as-str or Batch UUID)."""

    @abstractmethod
    async def build_container(
        self, simulator_version: ComposeSimulatorVersion, random_str: str, db_service: ComposeDatabaseService
    ) -> ComposeHpcRun:
        pass

    async def get_job_status(self, job_id_ext: str) -> ComposeJobStatus | None:
        """Poll this backend for a run's status. Default None = 'use the SLURM monitor path'."""
        return None


class ComposeSimulationServiceHpc(ComposeSimulationService):
    env: Settings
    backend = JobBackend.SLURM

    def __init__(self, env: Settings | None = None) -> None:
        self.env = env or get_settings()

    def _build_run_command(
        self,
        override_command: str | None,
        bind_clause: str,
        singularity_container: Path,
        slurm_job_name: str,
        simulation: ComposeSimulation,
    ) -> str:
        """Build the singularity command for the sbatch script."""
        if override_command:
            params = json.loads(override_command)
            if params.get("mode") == "v2ecoli":
                python = "/micromamba_env/runtime_env/bin/python3.12"
                mamba_env = "/micromamba_env/runtime_env"
                # Lines must be indented to match the dedent template (16 spaces)
                indent = " " * 16
                return (
                    f"CONDA_PREFIX={mamba_env} singularity exec \\\n"
                    f"{indent}    --compat \\\n"
                    f"{indent}    --env CONDA_PREFIX={mamba_env} \\\n"
                    f"{indent}    {bind_clause} \\\n"
                    f"{indent}    {singularity_container} \\\n"
                    f"{indent}    {python} /experiment/v2ecoli_run.py || true\n"
                    f"{indent}test -f /experiment/output/final_state.json || "
                    f'{{ echo "v2ecoli failed: no output produced"; exit 1; }}'
                )
        indent = " " * 16
        return (
            f"singularity run \\\n"
            f"{indent}    --compat \\\n"
            f"{indent}    {bind_clause} \\\n"
            f"{indent}    {singularity_container} \\\n"
            f"{indent}    /experiment/{slurm_job_name}."
            f"{simulation.sim_request.simulation_file_type.get_files_suffix()} \\\n"
            f'{indent}    -o "{self.env.compose_containers_output_dir}" \\\n'
            f"{indent}    -n {simulation.sim_request.end_time_point}"
        )

    @staticmethod
    def _write_v2ecoli_script(params: dict[str, object], output_dir: str) -> str:
        """Generate the Python script content for a v2ecoli direct invocation."""
        cache_dir = params["cache_dir"]
        seed = params["seed"]
        features = params.get("features", [])
        duration = params["duration"]
        return (
            f"import os, json\n"
            f"from v2ecoli.composite import make_composite\n"
            f"from v2ecoli.cache import save_json\n"
            f"composite = make_composite(cache_dir='{cache_dir}', seed={seed}, features={features!r})\n"
            f"composite.run({duration})\n"
            f"outdir = '/experiment/output'\n"
            f"os.makedirs(outdir, exist_ok=True)\n"
            f"save_json(dict(composite.state), os.path.join(outdir, 'final_state.json'))\n"
            f"print('v2ecoli simulation complete')\n"
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

            run_cmd = self._build_run_command(
                override_command, bind_clause, singularity_container, slurm_job_name, simulation
            )
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

            # For v2ecoli mode: write the Python runner script to a local file for upload
            v2ecoli_script_file: Path | None = None
            if override_command:
                params = json.loads(override_command)
                if params.get("mode") == "v2ecoli":
                    params["duration"] = simulation.sim_request.end_time_point
                    script_content_py = self._write_v2ecoli_script(params, self.env.compose_containers_output_dir)
                    v2ecoli_script_file = Path(tmpdir) / "v2ecoli_run.py"
                    v2ecoli_script_file.write_text(script_content_py)

            async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
                await ssh.run_command(f"mkdir -p {experiment_path}")
                # Upload the simulation input file (OMEX/PBG/SBML)
                remote_input = HPCFilePath(remote_path=get_compose_sim_input_path(experiment_id=slurm_job_name))
                await ssh.scp_upload(local_file=simulation.sim_request.request_file_path, remote_path=remote_input)
                # Upload v2ecoli runner script if applicable
                if v2ecoli_script_file is not None:
                    remote_script = HPCFilePath(remote_path=experiment_path / "v2ecoli_run.py")
                    await ssh.scp_upload(local_file=v2ecoli_script_file, remote_path=remote_script)
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

            async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
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
