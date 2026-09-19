"""Absolute paths inside the simulator image, and where a job writes its report.

Carved out of ``simulation_service_ray.py`` (``docs/plan-core.md`` P2.1, cut 3) as a pure
move, because it is the leaf everything else in this package needs: a module under
``simulation/ray/`` cannot import these from the service module, which imports *it*.

Constants only. No imports, so nothing here can ever be part of an import cycle.
"""

# Absolute paths inside the v2ecoli Ray image (WORKDIR=/app/v2ecoli). The
# entrypoint runs RAY_JOB_CMD on the head; v2ecoli reads the cache from
# CACHE_DIR and writes the ensemble outputs under OUT_DIR.
V2ECOLI_DIR = "/app/v2ecoli"
PARCA_CACHE_DIR = f"{V2ECOLI_DIR}/out/cache"
PARCA_SIMDATA_DIR = f"{V2ECOLI_DIR}/out/sim_data"
# Backlog item 105: scripts/build_new_gene_cache.py's own output dir, mirroring
# its DEFAULT_CACHE_DIR ("out/cache-new-genes") -- see submit_new_gene_cache_job.
NEW_GENE_INDUCED_CACHE_DIR = f"{V2ECOLI_DIR}/out/cache-new-genes"
# Backlog item 451: scripts/build_variant_cache.py's own output dir, mirroring
# its DEFAULT_CACHE_DIR ("out/cache-variant") -- see submit_variant_cache_job.
VARIANT_CACHE_DIR = f"{V2ECOLI_DIR}/out/cache-variant"
SIM_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/phase0-xarray"
# The analysis DAG node writes its outputs straight to S3 (see _analysis_command),
# so this local dir normally never exists and the entrypoint's RAY_OUT_DIR sync is a
# documented no-op ("no <dir>; nothing to upload"). It is still declared so anything
# the analysis does drop locally lands under the run's own S3 prefix.
ANALYSIS_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/analysis"
# In-region task compute (viva-api#631 slice 1): an arbitrary repo-path script
# run through the SAME standalone container path as ParCa/the analysis DAG
# node. Mirrors ANALYSIS_OUT_DIR's own rationale — a script that writes only to
# S3 leaves this empty (a documented sync no-op); declared so anything a
# script drops locally still lands under the run's own S3 prefix.
TASK_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/task"
# Where the container entrypoint syncs an uploaded task script (viva-api#631
# slice 2): submit_uploaded_task stages the script to an S3 prefix and passes it
# as CONTAINER_STAGE_S3, which batch-container-entrypoint.sh `aws s3 sync`s into
# CONTAINER_STAGE_DIR before running the command -- so the job_cmd runs
# `python <TASK_STAGE_DIR>/<script>`.
TASK_STAGE_DIR = f"{V2ECOLI_DIR}/.pbg/task_script"

# Where the head writes the entrypoint's metrics report (uploaded as report.json).
REPORT_PATH = "/tmp/report.json"  # noqa: S108
