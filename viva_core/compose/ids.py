"""The two identifiers a compose run is known by: the correlation id its worker events carry, and
the experiment id its outputs are filed under. Out of the SLURM path module they sat in (P3d-4d-1):
neither is a path, and every backend needs them."""

from viva_core.compose.models import ComposeJobType, ComposeSimulatorVersion


def compose_correlation_id(random_string: str, job_type: ComposeJobType) -> str:
    return f"{job_type.value}-{random_string}"


def compose_experiment_id(simulator: ComposeSimulatorVersion, random_str: str) -> str:
    return f"{simulator.singularity_def_hash}_{random_str}"
