#!/usr/bin/env bash
#
# pbg-dispatch.sh - process-bigraph-native multiseed/multigeneration dispatch
# (item 101 / item 109). Sibling to chain-dispatch.sh, which fires the OTHER,
# proven-at-1000x10-scale mechanism (item 71/103, external AWS-Batch-job-chain
# orchestration) -- this script is NOT that.
#
# Design doc (read this first if you're new to this system):
#   docs/plan/design-pbg-native-for-jim.md
# The real, actually-dispatched composite document this script's own params
# map onto directly:
#   docs/assets/composites/pbg-native-lineage-ray-batch-composite.json
#
# --- Two ways to set the composite's own config, in precedence order (highest
# first) ---
#   1. Individual env vars below (N_SEEDS, N_GENERATIONS, CACHE_DIR, ...) --
#      always win when explicitly set.
#   2. CONFIG_FILE -- point this at a real lineage_ray_batch-shaped composite
#      JSON (e.g. docs/assets/composites/pbg-native-lineage-ray-batch-composite.json
#      itself) and this script extracts real defaults straight out of it via
#      jq, so you can literally point this script at "the config" Alex asked
#      for and it dispatches something matching it (adjusting `out_dir`/
#      `cache_dir` for your own environment via the env vars above, since
#      those in a checked-in example point at a specific real S3 prefix/local
#      path from when it was generated).
#   3. Otherwise: this script's own built-in defaults (documented inline).
#
# Because lineage_ray_batch's own document has ONE peer node per seed (all
# structurally identical except seed/lineage_seed/experiment_id -- see the
# design doc §3), CONFIG_FILE mode reads the FIRST lineage_* node it finds for
# the shared fields (cache_dir/out_dir/experiment_id/max_duration_per_gen/
# time_step/media/emitter) and infers n_seeds from how many lineage_* nodes
# exist, base_seed from the minimum seed among them.
#
# --- A REAL, DOCUMENTED, CURRENTLY-EXISTING GAP, not silently worked around ---
# lineage_ray_batch's own registered @composite_generator `parameters` schema
# (v2ecoli/composites/lineage_ray_batch.py) does NOT include `variants`,
# `injected_processes`, or `config_overrides` -- even though the underlying
# document-builder function (build_lineage_ray_batch_document) accepts all
# three. process_bigraph.composite_spec.CompositeSpec._merged_params() raises
# `KeyError: unknown override(s): [...]` for ANY key not in that registered
# schema -- confirmed directly from source (composite_spec.py:330-337). A real
# dispatch passing any of those three today would hard-fail before the
# composite is even built. This script therefore does NOT expose them as
# params (there is nothing real to wire them to yet) -- extending the
# registered schema is real, undone follow-up work, not a scripting problem.
#
# Prerequisite (not run by this script): a tunnel to the target env must
# already be up, e.g.
#   cd ../sms-cdk/scripts && AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1 \
#     ./sms-proxy.sh -s smsvpctest
#
set -euo pipefail

VIVA_API_BASE="${VIVA_API_BASE:-http://localhost:8080}"

# --- dispatch-level (NOT part of the composite's own config -- these size the
# real infrastructure the composite runs on) ---
SIMULATOR_ID="${SIMULATOR_ID:-97}"
COMPOSITE_ID="${COMPOSITE_ID:-v2ecoli.composites.lineage_ray_batch}"
NUM_NODES="${NUM_NODES:-2}"
STEPS="${STEPS:-36000}"

# --- optional: read composite-config defaults straight out of a real,
# lineage_ray_batch-shaped composite JSON file ---
CONFIG_FILE="${CONFIG_FILE:-}"
_cfg_default() {
  # _cfg_default <jq-field-path-inside-a-lineage-node's-.config> <fallback>
  local field="$1" fallback="$2"
  if [[ -n "${CONFIG_FILE}" && -f "${CONFIG_FILE}" ]]; then
    local first_lineage
    first_lineage=$(jq -r '.state | keys[] | select(startswith("lineage_"))' "${CONFIG_FILE}" 2>/dev/null | sort | head -1)
    if [[ -n "${first_lineage}" ]]; then
      local v
      v=$(jq -r ".state[\"${first_lineage}\"].config${field} // empty" "${CONFIG_FILE}" 2>/dev/null)
      if [[ -n "${v}" ]]; then echo "${v}"; return; fi
    fi
  fi
  echo "${fallback}"
}
_cfg_n_seeds_default() {
  if [[ -n "${CONFIG_FILE}" && -f "${CONFIG_FILE}" ]]; then
    local n
    n=$(jq -r '[.state | keys[] | select(startswith("lineage_"))] | length' "${CONFIG_FILE}" 2>/dev/null)
    if [[ -n "${n}" && "${n}" != "0" ]]; then echo "${n}"; return; fi
  fi
  echo "2"
}
_cfg_base_seed_default() {
  if [[ -n "${CONFIG_FILE}" && -f "${CONFIG_FILE}" ]]; then
    local s
    s=$(jq -r '[.state[] | objects | select(.address? == "ray:LineageProcess") | .config.seed] | min' "${CONFIG_FILE}" 2>/dev/null)
    if [[ -n "${s}" && "${s}" != "null" ]]; then echo "${s}"; return; fi
  fi
  echo "0"
}

# --- composite-native params: EXACT names lineage_ray_batch's own
# @composite_generator registers (v2ecoli/composites/lineage_ray_batch.py)
# -- these are the fields you see in each lineage_XXXX node's own "config"
# block in the real composite JSON. Each defaults from CONFIG_FILE when set,
# else the hardcoded fallback shown.
N_SEEDS="${N_SEEDS:-$(_cfg_n_seeds_default)}"
N_GENERATIONS="${N_GENERATIONS:-$(_cfg_default '.generations' '1')}"
BASE_SEED="${BASE_SEED:-$(_cfg_base_seed_default)}"
CACHE_DIR="${CACHE_DIR:-$(_cfg_default '.cache_dir' 'out/cache')}"
EXPERIMENT_ID="${EXPERIMENT_ID:-$(_cfg_default '.experiment_id' 'pbg-dispatch-smoke')}"
EMITTER="${EMITTER:-$(_cfg_default '.emitter' 'both')}"
MAX_DURATION_PER_GEN="${MAX_DURATION_PER_GEN:-$(_cfg_default '.max_duration_per_gen' '3600.0')}"
TIME_STEP="${TIME_STEP:-$(_cfg_default '.time_step' '1.0')}"
MEDIA="${MEDIA:-$(_cfg_default '.media' 'minimal')}"

# out_dir (item 109): "" (default) omits the field, so lineage_ray_batch's own
# composite-level default ("" -> resolve_out_dir() -> PBG_RESULTS_DIR, local,
# entrypoint-synced) applies unchanged. Pass a real s3:// URI to have every
# lineage's own parquet + xarray emitters write directly there instead -- no
# code change needed anywhere, this field already exists on the composite
# (v2ecoli/composites/lineage_ray_batch.py's own @composite_generator
# registration). See docs/plan/design-pbg-native-for-jim.md §6.
OUT_DIR="${OUT_DIR-$(_cfg_default '.out_dir' '')}"

# n_workers: set to a real int to override (proven working); set to the empty
# string ("") to OMIT the field entirely from the request body -- lets the
# composite's own None default take over, which correctly falls through to
# the cluster-derived RAY_SHARDS_DEFAULT env var (see design doc §5). NOTE:
# uses ${VAR-default} (no colon) deliberately, NOT ${VAR:-default} -- bash's
# `:-` treats an explicitly-empty value the same as unset, which would
# silently defeat the whole point of being able to pass N_WORKERS="" to omit.
N_WORKERS="${N_WORKERS-}"

workflow() {
  local n_workers_field=""
  if [[ -n "${N_WORKERS}" ]]; then
    n_workers_field=',"n_workers": '"${N_WORKERS}"
  fi
  local out_dir_field=""
  if [[ -n "${OUT_DIR}" ]]; then
    out_dir_field=',"out_dir": "'"${OUT_DIR}"'"'
  fi
  curl -s -X POST "${VIVA_API_BASE}/api/v1/simulations?simulator_id=${SIMULATOR_ID}&experiment_id=${EXPERIMENT_ID}" \
    -H "Content-Type: application/json" \
    -d '{
      "extra_params": {
        "multi_node_dispatch": {
          "composite_id": "'"${COMPOSITE_ID}"'",
          "num_nodes": '"${NUM_NODES}"',
          "params": {
            "n_seeds": '"${N_SEEDS}"',
            "n_generations": '"${N_GENERATIONS}"',
            "base_seed": '"${BASE_SEED}"',
            "cache_dir": "'"${CACHE_DIR}"'",
            "experiment_id": "'"${EXPERIMENT_ID}"'",
            "emitter": "'"${EMITTER}"'",
            "max_duration_per_gen": '"${MAX_DURATION_PER_GEN}"',
            "time_step": '"${TIME_STEP}"',
            "media": "'"${MEDIA}"'"'"${n_workers_field}${out_dir_field}"'
          },
          "steps": '"${STEPS}"'
        }
      }
    }'
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  workflow
fi
