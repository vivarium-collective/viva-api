"""HPC path utilities for compose simulation subsystem."""

from pathlib import Path

from viva_api.compose.models import ComposeJobType, ComposeSimulatorVersion
from viva_api.config import get_settings


def get_compose_slurm_log_file(slurm_job_name: str) -> Path:
    return Path(str(get_settings().slurm_log_base_path)) / f"{slurm_job_name}.out"


def get_compose_slurm_submit_file(slurm_job_name: str) -> Path:
    return Path(str(get_settings().slurm_log_base_path)).parent / "sbatch" / f"{slurm_job_name}.sbatch"


def get_compose_singularity_def_file(singularity_hash: str) -> Path:
    return Path(get_settings().compose_image_base_path) / f"{singularity_hash}.def"


def get_compose_singularity_container_file(singularity_hash: str) -> Path:
    return Path(get_settings().compose_image_base_path) / f"{singularity_hash}.sif"


def get_compose_experiment_dir(experiment_id: str) -> Path:
    return Path(get_settings().compose_sim_base_path) / f"experiment-{experiment_id}"


def get_compose_sim_input_path(experiment_id: str) -> Path:
    return get_compose_experiment_dir(experiment_id) / f"{experiment_id}.omex"


def get_compose_sim_results_path(experiment_id: str) -> Path:
    return get_compose_experiment_dir(experiment_id) / "results.zip"


def get_compose_correlation_id(random_string: str, job_type: ComposeJobType) -> str:
    return f"{job_type.value}-{random_string}"


def get_compose_experiment_id(simulator: ComposeSimulatorVersion, random_str: str) -> str:
    return f"{simulator.singularity_def_hash}_{random_str}"
