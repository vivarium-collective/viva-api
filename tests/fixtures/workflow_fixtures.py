"""Fixtures for workflow.py integration tests.

These fixtures support testing the vEcoli workflow.py script which orchestrates
Nextflow execution from outside a container, allowing proper SLURM integration.
"""

from pathlib import Path
from textwrap import dedent

import pytest

from viva_api.config import get_settings


def _build_workflow_sbatch_template(
    *,
    job_name: str,
    cpus_per_task: int = 2,
    mem_gb: int = 8,
    time_limit: str = "0-02:00:00",
) -> str:
    """
    Build a Slurm sbatch template for running workflow.py from the vEcoli repo.

    This runs workflow.py OUTSIDE of a container, allowing it to properly
    interact with SLURM for scheduling Nextflow subtasks.

    Args:
        job_name: Slurm job name
        cpus_per_task: Number of CPU cores for the parent job
        time_limit: Maximum runtime in D-HH:MM:SS format

    Returns:
        Sbatch template string with placeholders for paths
    """
    settings = get_settings()
    partition = settings.slurm_partition
    qos_clause = f"#SBATCH --qos={settings.slurm_qos}" if settings.slurm_qos else ""
    nodelist_clause = f"#SBATCH --nodelist={settings.slurm_node_list}" if settings.slurm_node_list else ""

    return f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --output=REMOTE_LOG_OUTPUT_FILE
#SBATCH --error=REMOTE_LOG_ERROR_FILE
#SBATCH --partition={partition}
{qos_clause}
{nodelist_clause}
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --mem={mem_gb}G
#SBATCH --time={time_limit}

set -e

echo "=== Workflow.py Job Starting ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Working directory: $(pwd)"

# Initialize module system if available
if [ -f /etc/profile.d/modules.sh ]; then
    source /etc/profile.d/modules.sh
elif [ -f /usr/share/Modules/init/bash ]; then
    source /usr/share/Modules/init/bash
fi

# Check Java is available (required by Nextflow)
echo "=== Checking Java installation ==="
if ! command -v java &> /dev/null && [ -z "$JAVA_HOME" ]; then
    echo "Java not found, attempting to load java module..."
    if command -v module &> /dev/null; then
        module load java || {{ echo "ERROR: Failed to load java module"; exit 1; }}
    else
        echo "ERROR: Neither java nor module system available"
        exit 1
    fi
fi
java -version

# Check Nextflow is available
echo "=== Checking Nextflow installation ==="
which nextflow || {{ echo "ERROR: nextflow not found in PATH"; exit 1; }}
nextflow -version

# Paths set by placeholder replacement
VECOLI_REPO_PATH="VECOLI_REPO_PATH_PLACEHOLDER"
WORKFLOW_CONFIG="WORKFLOW_CONFIG_PATH_PLACEHOLDER"
OUTPUT_DIR="OUTPUT_DIR_PLACEHOLDER"
EXPERIMENT_ID="EXPERIMENT_ID_PLACEHOLDER"

# Verify vEcoli repo exists
if [ ! -d "$VECOLI_REPO_PATH" ]; then
    echo "ERROR: vEcoli repo not found: $VECOLI_REPO_PATH"
    exit 1
fi

# Verify workflow.py exists
if [ ! -f "$VECOLI_REPO_PATH/runscripts/workflow.py" ]; then
    echo "ERROR: workflow.py not found: $VECOLI_REPO_PATH/runscripts/workflow.py"
    exit 1
fi

# Verify config file exists
if [ ! -f "$WORKFLOW_CONFIG" ]; then
    echo "ERROR: Workflow config not found: $WORKFLOW_CONFIG"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "=== Configuration ==="
echo "vEcoli Repo: $VECOLI_REPO_PATH"
echo "Workflow Config: $WORKFLOW_CONFIG"
echo "Output Directory: $OUTPUT_DIR"
echo "Experiment ID: $EXPERIMENT_ID"

echo "=== Workflow config contents ==="
cat "$WORKFLOW_CONFIG"

echo "=== Running workflow.py ==="
cd "$VECOLI_REPO_PATH"

# Export SLURM environment variables for workflow.py (required for ccam profile)
export SLURM_PARTITION="{partition}"
export SLURM_QOS="{settings.slurm_qos if settings.slurm_qos else partition}"
export SLURM_NODE_LIST="{settings.slurm_node_list if settings.slurm_node_list else ""}"
export SLURM_LOG_BASE_PATH="{settings.slurm_log_base_path.remote_path}"

# Run workflow.py using uv from the repo's virtual environment
# workflow.py will internally call Nextflow with SLURM executor
uv run python runscripts/workflow.py --config "$WORKFLOW_CONFIG"

WORKFLOW_EXIT_CODE=$?

echo "=== Workflow.py completed with exit code: $WORKFLOW_EXIT_CODE ==="

# List output directory contents
echo "=== Output directory contents ==="
ls -la "$OUTPUT_DIR" 2>/dev/null || echo "Output directory empty or not found"

exit $WORKFLOW_EXIT_CODE
"""


@pytest.fixture(scope="session")
def slurm_template_workflow() -> str:
    """Slurm sbatch template for running workflow.py."""
    return _build_workflow_sbatch_template(
        job_name="workflow_test",
        cpus_per_task=2,
        mem_gb=8,
        time_limit="0-02:00:00",
    )


@pytest.fixture(scope="session")
def workflow_test_config_content() -> str:
    """
    Workflow config JSON for integration testing.

    This is a minimal config with reduced scope for faster test execution:
    - 2 seeds instead of 35
    - 2 generations instead of 5
    - Minimal analysis options
    """
    settings = get_settings()
    sim_outdir = settings.simulation_outdir.remote_path

    config = dedent(f"""\
    {{
        "inherit_from": [],
        "experiment_id": "EXPERIMENT_ID_PLACEHOLDER",
        "suffix_time": false,
        "description": "Integration test workflow",
        "progress_bar": false,
        "sim_data_path": null,
        "emitter": "parquet",
        "emitter_arg": {{
            "out_dir": "{sim_outdir}"
        }},
        "emit_topology": false,
        "emit_processes": false,
        "emit_config": false,
        "emit_unique": false,
        "log_updates": false,
        "raw_output": true,
        "seed": 0,
        "mar_regulon": false,
        "amp_lysis": false,
        "initial_state_file": "",
        "initial_state_overrides": [],
        "initial_state": {{}},
        "time_step": 1,
        "max_duration": 10800.0,
        "initial_global_time": 0,
        "fail_at_max_duration": true,
        "variants": {{}},
        "skip_baseline": false,
        "n_init_sims": 2,
        "generations": 2,
        "single_daughters": true,
        "daughter_outdir": "out",
        "lineage_seed": 0,
        "parca_options": {{
            "cpus": 2,
            "outdir": "{sim_outdir}",
            "operons": true,
            "ribosome_fitting": true,
            "rnapoly_fitting": true,
            "remove_rrna_operons": false,
            "remove_rrff": false,
            "stable_rrna": false,
            "new_genes": "off",
            "debug_parca": false,
            "load_intermediate": null,
            "save_intermediates": false,
            "intermediates_directory": "",
            "variable_elongation_transcription": true,
            "variable_elongation_translation": false
        }},
        "analysis_options": {{
            "cpus": 2,
            "multiseed": {{}},
            "multigeneration": {{}},
            "single": {{}}
        }},
        "gcloud": null,
        "agent_id": "0",
        "parallel": false,
        "divide": true,
        "d_period": true,
        "division_threshold": true,
        "division_variable": ["divide"],
        "chromosome_path": ["unique", "full_chromosome"],
        "spatial_environment": false,
        "spatial_environment_config": {{}},
        "fixed_media": "minimal",
        "condition": "basal",
        "save": false,
        "save_times": [],
        "add_processes": [],
        "exclude_processes": [],
        "swap_processes": {{}},
        "profile": false,
        "ccam": {{
            "build_image": false,
            "direct": true,
            "container_image": "CONTAINER_IMAGE_PLACEHOLDER"
        }}
    }}
    """)
    return config


@pytest.fixture(scope="session")
def workflow_inputs_dir() -> Path:
    """Path to the workflow test inputs directory."""
    return Path(__file__).parent / "workflow_inputs"
