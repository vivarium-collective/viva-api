#!/usr/bin/env bash
#
# cd2-run2-step2-strain-cache.sh - STEP 2 of 3 in the real CD2 Run 2 (J3/K4)
# strain-cache pipeline. Calls the real, previously-shipped-but-never-fired
# POST /parca/new-gene-cache endpoint (viva-api#378, backlog item 105) to
# stamp ONE strain's specific expression vector onto an already-COMPLETED
# ParCa dataset (from cd2-run2-step1-parca-base.sh's own real
# parca_dataset_id) -- this is the cheap, fast "derived build" half of the
# 2-tier design Chris described: one ParCa serves N strain-specific derived
# caches. Prints the real cache_s3_uri on success -- feed the resolved local
# path (RayLayout stages it to /app/v2ecoli/out/cache by default) into
# cd2-run2-step3-dispatch.sh's CACHE_DIR.
#
# Default values below are J3's own real numbers, pulled directly from
# Chris's committed configs (workspace/studies/cd2-pnnl-02-strain-simulations/
# study.yaml's own j3-vs-11j3 variant + configs/
# meteng_vio_gfp_composed_constitutive.json's own reference vector), NOT
# guessed. Override every REL_TRL_EFF_ADJ/VARIANT pair for K4
# (rel_trl_eff_adj [VIOA..E] = [49.2222, 0.9490, 7.3600, 2.3748, 1.3284] per
# the same study.yaml) or any other strain sharing this composed background.
#
# ⚠ ORDERING HAZARD, from configs/meteng_vio_gfp_composed_constitutive.json's
# own _note: rel_trl_eff_adj pairs POSITIONALLY against the new-gene MONOMER
# list [gfp, vioA, vioB, vioC, vioD, vioE] -- GFP FIRST. rel_exp_adj instead
# pairs against the RNA list [vio operon, gfp] -- VIO FIRST. The two vectors
# use OPPOSITE orders. Counts match either way (2 and 6) so a swapped vector
# raises nothing -- verify by reading the applied values back BY NAME
# (new_genes.json beside the built cache), not by trusting this script alone.
#
# Sibling scripts: cd2-run2-step1-parca-base.sh (STEP 1), cd2-run2-step3-
# dispatch.sh (STEP 3).
#
set -euo pipefail

VIVA_API_BASE="${VIVA_API_BASE:-http://localhost:8080}"
PARCA_DATASET_ID="${PARCA_DATASET_ID:?set PARCA_DATASET_ID -- the real, COMPLETED ParCa dataset id from cd2-run2-step1-parca-base.sh}"

# Non-collision label for the derived cache's own S3 key (RayLayout.parca_cache_uri).
VARIANT="${VARIANT:-cd2-run2-j3}"

# Base induction level -- constant across J3/K4/M5 on this composed
# background per configs/meteng_vio_gfp_composed_constitutive.json's own
# reference variant (exp = 10**6.07, trl_eff = 0.285). Override only if a
# different base induction is genuinely intended.
EXPRESSION="${EXPRESSION:-1174897.5556}"
TRANSLATION_EFFICIENCY="${TRANSLATION_EFFICIENCY:-0.285}"

# vio-first (RNA-level; unchanged from the reference across strains on this
# background -- only the vio pathway's own translation-efficiency ladder
# varies per the study.yaml design).
REL_EXP_ADJ="${REL_EXP_ADJ:-1.0,10.0}"

# gfp-first (monomer-level) -- J3's own real vector: gfp held at the
# reference's own 35.0, then vioA..E = [1.4576, 1.3410, 2.9667, 0.9999, 0.0444]
# (study.yaml's j3-vs-11j3 variant, rel_trl_eff_adj [VIOA..E]).
REL_TRL_EFF_ADJ="${REL_TRL_EFF_ADJ:-35.0,1.4576,1.3410,2.9667,0.9999,0.0444}"

SEED="${SEED:-0}"

workflow() {
  curl -sS -X POST "${VIVA_API_BASE}/api/v1/parca/new-gene-cache" \
    -H "Content-Type: application/json" \
    -d '{
      "parca_dataset_id": '"${PARCA_DATASET_ID}"',
      "variant": "'"${VARIANT}"'",
      "expression": '"${EXPRESSION}"',
      "translation_efficiency": '"${TRANSLATION_EFFICIENCY}"',
      "rel_exp_adj": "'"${REL_EXP_ADJ}"'",
      "rel_trl_eff_adj": "'"${REL_TRL_EFF_ADJ}"'",
      "seed": '"${SEED}"'
    }'
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  workflow
  echo
  echo "# Poll the returned job_id directly against the compute backend (e.g. aws batch" >&2
  echo "# describe-jobs) -- this job type does not register with /simulations/{id}/status." >&2
  echo "# Once SUCCEEDED, feed the response's own cache_s3_uri (or its staged local path" >&2
  echo "# under /app/v2ecoli/out/cache once the next dispatch's own pre-staging runs) into" >&2
  echo "# cd2-run2-step3-dispatch.sh's CACHE_DIR." >&2
fi
