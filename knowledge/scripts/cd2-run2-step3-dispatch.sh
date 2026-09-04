#!/usr/bin/env bash
#
# cd2-run2-step3-dispatch.sh - STEP 3 of 3 in the real CD2 Run 2 (J3/K4)
# strain-cache pipeline. Fires the real pbg-native (lineage_ray_batch)
# simulation dispatch, pointed at a strain-specific cache built by
# cd2-run2-step2-strain-cache.sh, with the real metabolism-redux injection
# (swap_processes/exclude_processes/flow) Chris's own config
# (configs/cd2/run2_j3_injected_metabolism.json) declares -- this is the step
# that actually produces a scientifically meaningful CD2 Run 2 result, not
# just a mechanism-proof. Requires a simulator built from a v2ecoli commit
# past PR#663 (variants/injected_processes/config_overrides registered on
# lineage_ray_batch's own @composite_generator schema) -- an older simulator
# will fail loud with `KeyError: unknown override(s)`.
#
# Sibling scripts: cd2-run2-step1-parca-base.sh (STEP 1), cd2-run2-step2-
# strain-cache.sh (STEP 2). Design doc: docs/plan/design-pbg-native-for-jim.md.
#
set -euo pipefail

VIVA_API_BASE="${VIVA_API_BASE:-http://localhost:8080}"
SIMULATOR_ID="${SIMULATOR_ID:?set SIMULATOR_ID -- must be built from a v2ecoli commit past PR#663}"
COMPOSITE_ID="${COMPOSITE_ID:-v2ecoli.composites.lineage_ray_batch}"
NUM_NODES="${NUM_NODES:-2}"
STEPS="${STEPS:-36000}"

EXPERIMENT_ID="${EXPERIMENT_ID:-cd2-run2-j3-real}"
N_SEEDS="${N_SEEDS:-2}"
N_GENERATIONS="${N_GENERATIONS:-8}"
BASE_SEED="${BASE_SEED:-0}"
EMITTER="${EMITTER:-both}"
MAX_DURATION_PER_GEN="${MAX_DURATION_PER_GEN:-3600.0}"
TIME_STEP="${TIME_STEP:-1.0}"
MEDIA="${MEDIA:-minimal}"
OUT_DIR="${OUT_DIR:-}"
N_WORKERS="${N_WORKERS-}"

# The strain-specific cache -- an absolute container path. Point this at
# wherever cd2-run2-step2-strain-cache.sh's own derived cache lands once
# viva-api's own pre-staging resolves it into the container (confirm the
# real staged path against that step's own job before trusting this
# default -- it is NOT verified by this script).
CACHE_DIR="${CACHE_DIR:?set CACHE_DIR, see comment above}"

# Real injected_processes, byte-for-byte from Chris's own committed
# configs/cd2/run2_j3_injected_metabolism.json (swap_processes/
# exclude_processes/flow). Override INJECTED_PROCESSES_JSON wholesale for K4
# or any other strain sharing this same structural injection. Single-quoted
# heredoc deliberately -- the JSON's own double quotes would otherwise break
# out of a "${VAR:-default}" default embedded in a double-quoted string.
if [[ -z "${INJECTED_PROCESSES_JSON:-}" ]]; then
  read -r -d '' INJECTED_PROCESSES_JSON <<'JSON' || true
{
  "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
  "exclude_processes": ["exchange_data"],
  "flow": {
    "ecoli-metabolism-redux": [["ecoli-chromosome-structure"]],
    "ecoli-mass-listener": [["ecoli-metabolism-redux"]],
    "RNA_counts_listener": [["ecoli-metabolism-redux"]],
    "rna_synth_prob_listener": [["ecoli-metabolism-redux"]],
    "monomer_counts_listener": [["ecoli-metabolism-redux"]],
    "dna_supercoiling_listener": [["ecoli-metabolism-redux"]],
    "replication_data_listener": [["ecoli-metabolism-redux"]],
    "rnap_data_listener": [["ecoli-metabolism-redux"]],
    "unique_molecule_counts": [["ecoli-metabolism-redux"]],
    "ribosome_data_listener": [["ecoli-metabolism-redux"]]
  }
}
JSON
fi

workflow() {
  # Built with jq, not hand-quoted string concatenation -- the raw-JSON
  # injected_processes value plus optional fields made manual quote-balancing
  # too fragile (a real syntax bug was caught here by bash -n before this
  # script was ever used).
  local params
  params=$(jq -n \
    --argjson n_seeds "${N_SEEDS}" \
    --argjson n_generations "${N_GENERATIONS}" \
    --argjson base_seed "${BASE_SEED}" \
    --arg cache_dir "${CACHE_DIR}" \
    --arg experiment_id "${EXPERIMENT_ID}" \
    --arg emitter "${EMITTER}" \
    --argjson max_duration_per_gen "${MAX_DURATION_PER_GEN}" \
    --argjson time_step "${TIME_STEP}" \
    --arg media "${MEDIA}" \
    --argjson injected_processes "${INJECTED_PROCESSES_JSON}" \
    '{
      n_seeds: $n_seeds, n_generations: $n_generations, base_seed: $base_seed,
      cache_dir: $cache_dir, experiment_id: $experiment_id, emitter: $emitter,
      max_duration_per_gen: $max_duration_per_gen, time_step: $time_step,
      media: $media, injected_processes: $injected_processes
    }')
  if [[ -n "${N_WORKERS}" ]]; then
    params=$(jq --argjson n_workers "${N_WORKERS}" '. + {n_workers: $n_workers}' <<<"${params}")
  fi
  if [[ -n "${OUT_DIR}" ]]; then
    params=$(jq --arg out_dir "${OUT_DIR}" '. + {out_dir: $out_dir}' <<<"${params}")
  fi
  local body
  body=$(jq -n \
    --arg composite_id "${COMPOSITE_ID}" \
    --argjson num_nodes "${NUM_NODES}" \
    --argjson params "${params}" \
    --argjson steps "${STEPS}" \
    '{extra_params: {multi_node_dispatch: {
      composite_id: $composite_id, num_nodes: $num_nodes, params: $params, steps: $steps
    }}}')
  curl -sS -X POST "${VIVA_API_BASE}/api/v1/simulations?simulator_id=${SIMULATOR_ID}&experiment_id=${EXPERIMENT_ID}" \
    -H "Content-Type: application/json" \
    -d "${body}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  workflow
fi
