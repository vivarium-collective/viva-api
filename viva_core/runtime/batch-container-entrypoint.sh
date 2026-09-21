#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# batch-container-entrypoint.sh
#
# Entrypoint for a plain, standalone AWS Batch CONTAINER-type job: one task,
# one container, no array indexing (unlike batch-array-entrypoint.sh) and no
# multi-node cluster formation (unlike ray-batch-entrypoint.sh). AWS Batch's
# OWN managed compute environment handles fleet sizing per queue depth.
#
# CONTAINER_JOB_CMD is a shell string built by the submitter. Nothing in this
# script is workload-aware; it only stages data in, runs the command it is
# given, and ships data and logs back out.
#
# THIS IS CORE'S COPY (viva_core/runtime/, docs/plan-core.md P2.3c), installed
# at /opt/batch-container-entrypoint.sh -- the absolute path every container job
# definition calls, which makes the path part of the contract. It began as
# v2ecoli's docker/batch-container-entrypoint.sh at fb4e091ce, the last copy that
# was application-free: the science repositories' copies have since diverged
# (one now imports its model's cache verifier), which is what a generic script
# kept in an application's repository does. tests/core/test_runtime_entrypoint.py
# runs this file against a fake `aws` and pins the contract below.
#
# OBSERVABILITY:
#   - High-signal lines go to stdout → CloudWatch (keep this stream small).
#   - CONTAINER_DEBUG_HOLD keeps a FAILED job alive so you can SSM in and
#     inspect it; it prints the exact SSM command.
#
# Env knobs (all optional except CONTAINER_JOB_CMD):
#   CONTAINER_JOB_CMD            workload to run for this job (required at run time)
#   CONTAINER_STAGE_S3           S3 URI of an input dataset to fetch before the
#                                 job. Paired
#                                 with CONTAINER_STAGE_DIR; the image stays
#                                 data-free.
#   CONTAINER_STAGE_DIR          local dir to sync CONTAINER_STAGE_S3 into
#   CONTAINER_OUT_DIR            local dir the workload writes results to.
#                                 Synced to CONTAINER_OUT_S3 periodically during
#                                 the job and once at teardown, so results
#                                 survive a Spot interruption's SIGTERM grace
#                                 window. Paired with CONTAINER_OUT_S3.
#   CONTAINER_OUT_S3             S3 URI to sync CONTAINER_OUT_DIR to
#   CONTAINER_OUT_SYNC_INTERVAL  seconds between periodic output syncs (default 30)
#   CONTAINER_REPORT_PATH        if this file exists after the job, it's uploaded
#   CONTAINER_LOG_S3_PREFIX      s3:// prefix for this job's report.json (empty disables)
#   CONTAINER_DEBUG_HOLD         '0' (off) | '1' (hold on failure) | 'always'
#   CONTAINER_DEBUG_HOLD_SECONDS hold duration (default 1800)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

JOB_ID="${AWS_BATCH_JOB_ID:-unknown}"

# ── Helpers ──────────────────────────────────────────────────────────────────
imds_instance_id() {
  local t
  t="$(curl -sS -X PUT 'http://169.254.169.254/latest/api/token' \
        -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)"
  curl -sS -H "X-aws-ec2-metadata-token: ${t}" \
       'http://169.254.169.254/latest/meta-data/instance-id' 2>/dev/null || echo 'unknown'
}

# Best-effort: upload the workload's metrics report next to the logs, if it wrote one.
upload_report() {
  local path="${CONTAINER_REPORT_PATH:-}"
  [[ -z "${CONTAINER_LOG_S3_PREFIX:-}" || -z "${path}" || ! -f "${path}" ]] && return 0
  local dest="${CONTAINER_LOG_S3_PREFIX%/}/${JOB_ID}/report.json"
  if command -v aws >/dev/null 2>&1 && aws s3 cp "${path}" "${dest}" >/dev/null 2>&1; then
    echo "[batch-container] job ${JOB_ID}: report -> ${dest}"
  else
    echo "[batch-container] WARN: report upload to ${dest} skipped" >&2
  fi
}

# Stage an input dataset from S3 to a local dir before the job runs. Fails hard
# -- if it is configured, the job needs it.
stage_inputs() {
  [[ -z "${CONTAINER_STAGE_S3:-}" || -z "${CONTAINER_STAGE_DIR:-}" ]] && return 0
  command -v aws >/dev/null 2>&1 || { echo "[batch-container] FATAL: aws CLI needed for input staging" >&2; exit 1; }
  echo "[batch-container] job ${JOB_ID}: staging ${CONTAINER_STAGE_S3} -> ${CONTAINER_STAGE_DIR}"
  mkdir -p "${CONTAINER_STAGE_DIR}"
  if ! aws s3 sync "${CONTAINER_STAGE_S3}" "${CONTAINER_STAGE_DIR}" --only-show-errors; then
    echo "[batch-container] FATAL: input staging from ${CONTAINER_STAGE_S3} failed" >&2
    exit 1
  fi
  # `aws s3 sync` exits 0 when the source prefix holds no objects (a stale or wrong
  # CONTAINER_STAGE_S3), silently leaving the dir empty. If staging was asked for,
  # nothing staged is a failure -- said here, not by the workload minutes later.
  if [[ -z "$(ls -A "${CONTAINER_STAGE_DIR}" 2>/dev/null)" ]]; then
    echo "[batch-container] FATAL: ${CONTAINER_STAGE_DIR} is EMPTY after sync from ${CONTAINER_STAGE_S3} (stale or wrong prefix?)" >&2
    exit 1
  fi
}

# Output capture (CONTAINER_OUT_DIR -> CONTAINER_OUT_S3), symmetric to stage_inputs().
#
# WHY PERIODIC + FINAL: AWS Batch SIGTERMs a job on Spot interruption or
# cancellation, then SIGKILLs after a short grace window. A from-scratch sync
# of a large output may not finish in that window. start_output_sync()
# uploads incrementally while the job runs, so the final stage_outputs() on
# teardown is a small delta that fits the grace window.
OUTPUT_SYNC_PID=""

start_output_sync() {
  [[ -z "${CONTAINER_OUT_S3:-}" || -z "${CONTAINER_OUT_DIR:-}" ]] && return 0
  if ! command -v aws >/dev/null 2>&1; then
    echo "[batch-container] WARN: aws CLI not in image; no S3 output sync" >&2
    return 0
  fi
  local interval="${CONTAINER_OUT_SYNC_INTERVAL:-30}"
  mkdir -p "${CONTAINER_OUT_DIR}"
  ( while true; do
      aws s3 sync "${CONTAINER_OUT_DIR}" "${CONTAINER_OUT_S3}" --only-show-errors >/dev/null 2>&1 || true
      sleep "${interval}"
    done ) &
  OUTPUT_SYNC_PID=$!
  echo "[batch-container] job ${JOB_ID}: periodic output sync ${CONTAINER_OUT_DIR} -> ${CONTAINER_OUT_S3} (every ${interval}s)"
}

# Final authoritative sync: stop the periodic loop, then do one last aws s3 sync.
# Returns non-zero on a real upload failure so a failed upload fails the task
# even if the workload itself exited 0 (the results are the point).
stage_outputs() {
  if [[ -n "${OUTPUT_SYNC_PID}" ]]; then
    kill "${OUTPUT_SYNC_PID}" 2>/dev/null || true
    wait "${OUTPUT_SYNC_PID}" 2>/dev/null || true
    OUTPUT_SYNC_PID=""
  fi
  [[ -z "${CONTAINER_OUT_S3:-}" || -z "${CONTAINER_OUT_DIR:-}" ]] && return 0
  command -v aws >/dev/null 2>&1 || return 0
  if [[ ! -d "${CONTAINER_OUT_DIR}" ]]; then
    echo "[batch-container] job ${JOB_ID}: no ${CONTAINER_OUT_DIR}; nothing to upload" >&2
    return 0
  fi
  echo "[batch-container] job ${JOB_ID}: final output sync ${CONTAINER_OUT_DIR} -> ${CONTAINER_OUT_S3}"
  if ! aws s3 sync "${CONTAINER_OUT_DIR}" "${CONTAINER_OUT_S3}" --only-show-errors; then
    echo "[batch-container] FATAL: output staging to ${CONTAINER_OUT_S3} failed (job ${JOB_ID})" >&2
    return 1
  fi
}

# Keep the container alive for interactive debugging, printing copy-paste SSM commands.
debug_hold() {
  local rc="$1"
  local mode="${CONTAINER_DEBUG_HOLD:-0}"
  local secs="${CONTAINER_DEBUG_HOLD_SECONDS:-1800}"
  local do_hold=0
  case "${mode}" in
    always)      do_hold=1 ;;
    1|true|yes)  [[ "${rc}" -ne 0 ]] && do_hold=1 ;;
  esac
  [[ "${do_hold}" -eq 1 ]] || return 0

  local iid; iid="$(imds_instance_id)"
  cat >&2 <<MSG
======================= BATCH CONTAINER DEBUG HOLD =======================
 job ${JOB_ID} rc=${rc}; holding this container for ${secs}s for debugging.
 instance: ${iid}

 shell into the container:
   aws ssm start-session --target ${iid}
   sudo docker ps                      # find this job's container
   sudo docker exec -it <container-id> bash
============================================================================
MSG
  sleep "${secs}"
}

# ── Stage inputs, run the workload, capture outputs ───────────────────────────
_FINALIZED=0
_finalize() {
  [[ "${_FINALIZED}" == 1 ]] && return 0
  _FINALIZED=1
  stage_outputs || true
}
trap _finalize EXIT

echo "[batch-container] job ${JOB_ID} starting"
stage_inputs
start_output_sync

set +e
bash -lc "${CONTAINER_JOB_CMD:?CONTAINER_JOB_CMD not set}"
rc=$?
set -e

if ! stage_outputs && [[ "${rc}" -eq 0 ]]; then
  rc=1
fi
_FINALIZED=1   # stage_outputs already ran above; the EXIT trap must not repeat it

upload_report
debug_hold "${rc}"
echo "[batch-container] job ${JOB_ID} exited rc=${rc}"
exit "${rc}"
