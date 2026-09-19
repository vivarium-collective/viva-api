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
# --- UPDATE 2026-09-06: the gap this section used to describe is closed ---
# lineage_ray_batch's own registered @composite_generator `parameters` schema
# (v2ecoli/composites/lineage_ray_batch.py) DOES include `variants`,
# `injected_processes`, and `config_overrides` as of v2ecoli#663 (`ad22c4d7`,
# confirmed directly against current main). `INJECTED_PROCESSES` below is a
# real, working env var, not aspirational -- verified end to end on real
# infrastructure (Dispatch 368, smsvpctest, 2026-09-06): the real submitted
# AWS Batch command carried the swap, and the composite genuinely built with
# it. `variants`/`config_overrides` remain unwired here (nobody has needed
# them from this script yet) -- add the same way if/when needed.
#
# --- CACHE_VARIANT gotcha, real and costly to get wrong (learned the hard
# way, same session) ---
# cache_variant is a DISPATCH-LEVEL directive (which S3 slot to stage the
# ParCa cache from -- see SimulationServiceRay._submit_multi_node_composite),
# NOT a composite parameter. It belongs at the TOP LEVEL of
# multi_node_dispatch, a SIBLING of composite_id/num_nodes/steps -- NOT
# nested inside `params`. Nesting it inside `params` (an easy mistake -- it
# reads like it belongs with the other cache-ish fields) silently defeats the
# item105/106 stock-cache guard (viva-api#437): `params.cache_variant` is
# invisible to `mnp_dispatch.get("cache_variant")`, so the dispatch falls
# through as if cache_variant were never set at all -- a real, unconditional
# ParCa rebuild into the BARE per-commit path, reproducing the exact bug #437
# exists to prevent, self-inflicted by the caller rather than caused by any
# guard defect. `CACHE_VARIANT` below is wired at the correct (top) level.
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

# cache_variant: "" (default) omits the field, so cache_s3_uri's own
# variant=None default applies unchanged (every existing caller's byte-for-
# byte behavior). Set to a real staged variant name to reuse a pre-built
# strain-specific ParCa cache instead of the plain per-commit one -- MUST
# already be staged (POST /parca/new-gene-cache, or an explicit S3 sync) at
# this exact commit before dispatching, or the request fails loud with a
# ValueError (viva-api#437) rather than silently building a stock substitute
# under the variant's name. See the header comment above for the top-level-
# not-nested-in-params gotcha.
CACHE_VARIANT="${CACHE_VARIANT-}"

# injected_processes: "" (default) omits the field. Set to a real JSON object
# (e.g. '{"swap_processes":{"ecoli-metabolism":"ecoli-metabolism-redux"},
# "exclude_processes":["exchange_data"]}') to apply a real process swap/add/
# exclude, verified end to end on real infra (Dispatch 368, 2026-09-06) --
# this one DOES belong inside `params` (it's a real lineage_ray_batch
# composite parameter, unlike cache_variant above).
INJECTED_PROCESSES="${INJECTED_PROCESSES-}"

workflow() {
  local n_workers_field=""
  if [[ -n "${N_WORKERS}" ]]; then
    n_workers_field=',"n_workers": '"${N_WORKERS}"
  fi
  local out_dir_field=""
  if [[ -n "${OUT_DIR}" ]]; then
    out_dir_field=',"out_dir": "'"${OUT_DIR}"'"'
  fi
  local cache_variant_field=""
  if [[ -n "${CACHE_VARIANT}" ]]; then
    cache_variant_field=',"cache_variant": "'"${CACHE_VARIANT}"'"'
  fi
  local injected_processes_field=""
  if [[ -n "${INJECTED_PROCESSES}" ]]; then
    injected_processes_field=',"injected_processes": '"${INJECTED_PROCESSES}"
  fi
  curl -s -X POST "${VIVA_API_BASE}/api/v1/simulations?simulator_id=${SIMULATOR_ID}&experiment_id=${EXPERIMENT_ID}" \
    -H "Content-Type: application/json" \
    -d '{
      "extra_params": {
        "multi_node_dispatch": {
          "composite_id": "'"${COMPOSITE_ID}"'",
          "num_nodes": '"${NUM_NODES}${cache_variant_field}"'
          ,"params": {
            "n_seeds": '"${N_SEEDS}"',
            "n_generations": '"${N_GENERATIONS}"',
            "base_seed": '"${BASE_SEED}"',
            "cache_dir": "'"${CACHE_DIR}"'",
            "experiment_id": "'"${EXPERIMENT_ID}"'",
            "emitter": "'"${EMITTER}"'",
            "max_duration_per_gen": '"${MAX_DURATION_PER_GEN}"',
            "time_step": '"${TIME_STEP}"',
            "media": "'"${MEDIA}"'"'"${n_workers_field}${out_dir_field}${injected_processes_field}"'
          },
          "steps": '"${STEPS}"'
        }
      }
    }'
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  workflow
fi
