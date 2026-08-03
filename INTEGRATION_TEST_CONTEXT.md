# vEcoli Workflow.py Integration Test Context

## Date: 2026-01-13

## Summary

Created an integration test for running the vEcoli `workflow.py` script on an HPC cluster via SLURM. Unlike the previous stub-mode Nextflow test, this test runs the full workflow (parca, simulations) using Nextflow with the SLURM executor.

## Architecture

The test runs `workflow.py` **outside** of a Singularity container, which allows proper SLURM integration for Nextflow subtasks. The workflow:

1. Clones the vEcoli repo (if not already present)
2. Builds a Singularity container image (if not already present)
3. Runs `workflow.py` which internally calls Nextflow
4. Nextflow submits parca and simulation jobs to SLURM
5. Jobs run inside the Singularity container

## Files Added

### 1. `tests/common/test_workflow.py`
Main test file containing:
- `_check_repo_exists()` - Verify vEcoli repo exists on HPC
- `_check_image_exists()` - Verify Singularity image exists
- `_get_or_create_simulator()` - Get/create simulator DB entry
- `_ensure_repo_and_image_exist()` - Clone repo and build image if needed
- `_run_workflow_test()` - Submit workflow job and poll for completion
- `test_workflow_py_execution()` - Main integration test

### 2. `tests/fixtures/workflow_fixtures.py`
Fixtures for the workflow test:
- `_build_workflow_sbatch_template()` - Generate sbatch script with SLURM settings
- `slurm_template_workflow` - Session-scoped fixture for sbatch template
- `workflow_test_config_content` - JSON config for minimal workflow (2 seeds, 2 generations)
- `workflow_inputs_dir` - Path to workflow inputs (currently unused)

### 3. `tests/conftest.py`
Added imports for workflow fixtures:
```python
from tests.fixtures.workflow_fixtures import (
    slurm_template_workflow,
    workflow_inputs_dir,
    workflow_test_config_content,
)
```

### 4. `tests/fixtures/api_fixtures.py`
Updated simulator configuration:
```python
SIMULATOR_BRANCH = "api-support"  # was "ccam-nextflow"
SIMULATOR_COMMIT = "a417d6e"      # was "8f119dd"
```

## vEcoli Changes Required

The `api-support` branch of vEcoli (commit `a417d6e`) includes fixes for:

1. **Singularity Python accessibility**: Python installed in `/vEcoli/.python` (not `/root/`) so it's accessible at container runtime
2. **Python symlinks**: Created `python` and `python3` symlinks in `.venv/bin/`
3. **singularity vs apptainer**: Use `singularity.enabled = true` (not `apptainer.enabled`)
4. **SLURM placeholder replacement**: `workflow.py` replaces `SLURM_QUEUE`, `SLURM_QOS`, `SLURM_CLUSTER_OPTIONS` from environment variables
5. **ccam profile resources**: Added proper memory, time, and parca-specific settings

## How to Run the Test

```bash
# Run the workflow integration test
uv run pytest tests/common/test_workflow.py -v

# Or with verbose logging
uv run pytest tests/common/test_workflow.py -v -s
```

## Environment Variables Required

The test requires SSH access to the HPC cluster:
- `SLURM_SUBMIT_HOST` - HPC submit node hostname
- `SLURM_SUBMIT_USER` - SSH username
- `SLURM_SUBMIT_KEY_PATH` - Path to SSH private key
- `SLURM_PARTITION` - SLURM partition to use
- `SLURM_QOS` - SLURM QoS (optional)
- `SLURM_NODE_LIST` - Specific nodes (optional)

## Test Configuration

The test uses a minimal workflow config:
- **n_init_sims**: 2 (instead of default 35)
- **generations**: 2 (instead of default 5)
- **ccam.direct**: true (Nextflow runs inline, not via another sbatch)
- **ccam.build_image**: false (image pre-built)

## Expected Runtime

- **First run** (with build): ~30-60 minutes
  - Cloning repo: ~2 minutes
  - Building Singularity image: ~10-20 minutes
  - Running parca: ~15 minutes
  - Running simulations: ~10-20 minutes
- **Subsequent runs** (repo/image exist): ~30-45 minutes
  - Running parca: ~15 minutes
  - Running simulations: ~10-20 minutes

## Log File Locations on HPC

- Sbatch script: `/projects/SMS/viva_api/<user>/htclogs/workflow_<uuid>.sbatch`
- Stdout: `/projects/SMS/viva_api/<user>/htclogs/workflow_<uuid>.out`
- Stderr: `/projects/SMS/viva_api/<user>/htclogs/workflow_<uuid>.err`
- Workflow config: `/projects/SMS/viva_api/<user>/htclogs/workflow_<uuid>_output/workflow_config.json`
- Nextflow work: `/projects/SMS/viva_api/<user>/workspace/api_outputs/workflow_test_<uuid>/nextflow/nextflow_workdirs/`

## Debugging Tips

1. **Check sbatch output**: Look at `.out` file for Nextflow progress
2. **Check Nextflow logs**: Look in `nextflow_workdirs/<hash>/` for `.command.log`
3. **Check SLURM jobs**: `sacct -u <user> --starttime=today`
4. **Check parca log**: `cat nextflow_workdirs/<hash>/.command.log`

## Key Code Patterns

### Sbatch Template Variables
The sbatch template uses these placeholders:
- `VECOLI_REPO_PATH_PLACEHOLDER` - Path to cloned vEcoli repo
- `WORKFLOW_CONFIG_PATH_PLACEHOLDER` - Path to uploaded workflow_config.json
- `OUTPUT_DIR_PLACEHOLDER` - Output directory path
- `EXPERIMENT_ID_PLACEHOLDER` - Unique experiment identifier
- `REMOTE_LOG_OUTPUT_FILE` - Slurm stdout file
- `REMOTE_LOG_ERROR_FILE` - Slurm stderr file

### Workflow Config Placeholders
- `EXPERIMENT_ID_PLACEHOLDER` - Replaced with unique experiment ID
- `CONTAINER_IMAGE_PLACEHOLDER` - Replaced with Singularity image path

## Test Execution Results (2026-01-13)

### Successful Run
- Parca: Completed in ~15 minutes
- createVariants: Completed
- sim_gen_1: 2 of 2 completed
- sim_gen_2: Running (as of last check)

### Issues Resolved During Development

1. **OUT_OF_MEMORY**: Added `mem_gb=8` parameter to sbatch template
2. **apptainer not found**: Changed to `singularity.enabled = true`
3. **python not found**: Fixed Python installation path and symlinks
4. **ModuleNotFoundError dotenv**: Regenerated `uv.lock` to include dependencies
5. **Hardcoded SLURM settings**: Added placeholder replacement in `workflow.py`
