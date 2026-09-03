#!/usr/bin/env bash
#
# verify-pbg-dispatch.sh - fires a clean pbg-native (item 101/109) end-to-end
# verification dispatch: 2 seeds x 4 generations, deployment-standard out_dir
# (deliberately NOT overridden), so the auto-triggered multi-node analysis
# job reads from the exact same place the real biological data actually
# lands.
#
# WHY THIS EXISTS, PRECISELY: 2026-09-03, dispatch 282 (item 109's own
# out_dir-override verification) had its automatic post-completion analysis
# job (JobScheduler._advance_multi_node_job -> SimulationServiceRay.
# submit_multi_node_analysis, viva_api/simulation/job_scheduler.py:531-591 --
# a real, GENERIC mechanism for ANY multi-node composite dispatch, not
# colony-specific, live since item 88) run against the WRONG location: that
# analysis always reads from the deployment-standard
# `_results_s3_uri(experiment_id)`, with no awareness of a caller-supplied
# custom `out_dir` -- and 282 deliberately used a custom out_dir (the whole
# point of ITS OWN verification). Real result, pulled directly from the
# analysis job's own output:
#   "No in-memory emitter history was captured for this run."
# Not a sign analysis is missing or broken -- a real, narrower interaction
# gap between two things that each work correctly in isolation. This script
# is the decisive test: with out_dir left at its default, the auto-analysis
# and the real data land in the same place, so ParCa -> simulation ->
# analysis can be confirmed end to end. See
# docs/plan/design-pbg-native-for-jim.md for the full trace.
#
# Deliberately fully self-contained (NOT sourcing pbg-dispatch.sh) --
# duplicated on purpose for readability: this script's whole reason to exist
# is one specific, pinned verification scenario, and every line of its own
# request should be visible in this one file rather than inherited from a
# sibling script's own more general logic.
#
# Real, fired example: database_id=283,
# job_id=bc67b87d-e747-4c7a-bb96-e1f49864e907 (fired against simulator_id=109,
# commit 4da4e43).
#
# Prerequisite (not run by this script): a tunnel to the target env must
# already be up, e.g.
#   cd ../sms-cdk/scripts && AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1 \
#     ./sms-proxy.sh -s smsvpctest
#
set -euo pipefail

VIVA_API_BASE="${VIVA_API_BASE:-http://localhost:8080}"

SIMULATOR_ID="${SIMULATOR_ID:-109}"
EXPERIMENT_ID="${EXPERIMENT_ID:-pbg-native-analysis-flush-verify}"

COMPOSITE_ID="${COMPOSITE_ID:-v2ecoli.composites.lineage_ray_batch}"
NUM_NODES="${NUM_NODES:-2}"
N_SEEDS="${N_SEEDS:-2}"
N_GENERATIONS="${N_GENERATIONS:-4}"
BASE_SEED="${BASE_SEED:-0}"
CACHE_DIR="${CACHE_DIR:-out/cache}"
EMITTER="${EMITTER:-both}"
MAX_DURATION_PER_GEN="${MAX_DURATION_PER_GEN:-3600.0}"
TIME_STEP="${TIME_STEP:-1.0}"
MEDIA="${MEDIA:-minimal}"
STEPS="${STEPS:-36000}"

# n_workers: set to a real int to override; set to the empty string ("") to
# OMIT the field entirely, letting the composite's own None default fall
# through to the cluster-derived RAY_SHARDS_DEFAULT env var. NOTE: uses
# ${VAR-default} (no colon) deliberately, NOT ${VAR:-default} -- bash's `:-`
# treats an explicitly-empty value the same as unset, which would silently
# defeat the whole point of being able to pass N_WORKERS="" to omit.
N_WORKERS="${N_WORKERS-}"

# out_dir is DELIBERATELY NEVER SETTABLE by this script, unlike
# pbg-dispatch.sh's own OUT_DIR -- no env-var override, no CONFIG_FILE mode.
# The entire point of this script is to confirm the pipeline works when
# out_dir stays at its deployment-standard default, which is exactly where
# the auto-triggered analysis job looks. Use pbg-dispatch.sh directly (with
# OUT_DIR set) for the out_dir-override case instead -- the two scenarios are
# kept deliberately separate, in separate files, so neither can silently grow
# an option that reintroduces the mismatch this script exists to avoid.

workflow() {
  local n_workers_field=""
  if [[ -n "${N_WORKERS}" ]]; then
    n_workers_field=',"n_workers": '"${N_WORKERS}"
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
            "media": "'"${MEDIA}"'"'"${n_workers_field}"'
          },
          "steps": '"${STEPS}"'
        }
      }
    }'
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Firing pbg-native e2e verification: ${N_SEEDS} seeds x ${N_GENERATIONS} generations," \
       "simulator ${SIMULATOR_ID}, no out_dir override (deployment-standard location)." >&2
  workflow
fi
