#!/usr/bin/env bash
#
# cd2-run2-step1-parca-base.sh - STEP 1 of 3 in the real CD2 Run 2 (J3/K4)
# strain-cache pipeline. Fires a real ParCa build of the shared "composed
# vio+GFP" background (v2ecoli#637 multi-insertion loader) that every strain
# variant (J3, K4, M5-default) is derived from -- new_genes=violacein_gfp,
# bundle_overrides=models/parca/composed_overlay.tsv. Prints the real
# parca_dataset_id on success -- feed that into cd2-run2-step2-strain-cache.sh.
#
# ⚠ Chris's own reference config, configs/meteng_vio_gfp_composed_
# constitutive.json, ALSO sets parca_options.include_violacein_reactions --
# viva-api's real, live ParcaOptions model (extra="forbid") has no such field
# and 400s on it (confirmed empirically 2026-09-04). That field is not read
# anywhere in v2ecoli's own source either (repo-wide grep, zero hits), so it
# is safe to omit -- it changes nothing about the actual ParCa background.
# The default below points at a scratch, dispatch-safe copy with only that
# one field removed (configs/meteng_vio_gfp_composed_constitutive_dispatch.
# json, sms-ecoli branch scratch/cd2-run2-parca-base-dispatch, off Chris's own
# study/cd2-pnnl-02-strain-sims, otherwise byte-identical) -- verified working
# end-to-end (database_id=314, parca_dataset_id=176). If viva-api's model ever
# gains this field, point SIMULATION_CONFIG_FILENAME back at the original.
#
# Sibling scripts: cd2-run2-step2-strain-cache.sh (STEP 2, derived per-strain
# cache), cd2-run2-step3-dispatch.sh (STEP 3, the real simulation).
# Not yet a single call -- each step's own output feeds the next step's own
# required input (parca_dataset_id -> cache_s3_uri -> cache_dir), and the
# middle step structurally cannot start until ParCa has actually completed
# (POST /parca/new-gene-cache requires an already-COMPLETED parca_dataset_id).
#
# Prerequisite (not run by this script): a tunnel to the target env, e.g.
#   cd ../sms-cdk/scripts && AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1 \
#     ./sms-proxy.sh -s smsvpctest
#
set -euo pipefail

VIVA_API_BASE="${VIVA_API_BASE:-http://localhost:8080}"
# A real sms-ecoli simulator built from a branch/commit that has both
# configs/meteng_vio_gfp_composed_constitutive_dispatch.json and
# models/parca/composed_overlay.tsv -- e.g. scratch/cd2-run2-parca-base-
# dispatch (verified) or study/cd2-pnnl-02-strain-sims (Chris's own, needs
# the dispatch-safe config filename override below since it only has the
# original).
SIMULATOR_ID="${SIMULATOR_ID:?set SIMULATOR_ID, see comment above}"
SIMULATION_CONFIG_FILENAME="${SIMULATION_CONFIG_FILENAME:-meteng_vio_gfp_composed_constitutive_dispatch.json}"
EXPERIMENT_ID="${EXPERIMENT_ID:-cd2-run2-parca-base}"
# Simulation-phase params are irrelevant to this step's own real purpose (we
# only need ParCa to complete) -- kept minimal/cheap on purpose.
NUM_GENERATIONS="${NUM_GENERATIONS:-1}"
NUM_SEEDS="${NUM_SEEDS:-1}"

workflow() {
  local qs="simulator_id=${SIMULATOR_ID}"
  qs+="&experiment_id=${EXPERIMENT_ID}"
  qs+="&simulation_config_filename=${SIMULATION_CONFIG_FILENAME}"
  qs+="&num_generations=${NUM_GENERATIONS}"
  qs+="&num_seeds=${NUM_SEEDS}"
  qs+="&run_parca=true"
  curl -sS -X POST "${VIVA_API_BASE}/api/v1/simulations?${qs}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  workflow
  echo
  echo "# Poll the resulting parca_dataset_id -- the response's own field, or aws batch" >&2
  echo "# describe-jobs -- until its ParCa job reaches SUCCEEDED, then feed it into" >&2
  echo "# cd2-run2-step2-strain-cache.sh via PARCA_DATASET_ID." >&2
fi
