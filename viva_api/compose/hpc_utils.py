"""HPC path utilities for compose simulation subsystem."""

from pathlib import Path

from viva_core.compose.ids import compose_correlation_id, compose_experiment_id
from viva_core.settings import get_core_settings as get_settings


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


# The two ids moved to ``viva_core.compose.ids`` (P3d-4d-1); these names stay for their importers.
get_compose_correlation_id = compose_correlation_id
get_compose_experiment_id = compose_experiment_id
