#!/usr/bin/env bash
#
# chain-dispatch.sh - the PROVEN AWS-Batch-job-chain multigeneration/multiseed
# dispatch (item 71/103's own mechanism). Sibling to pbg-dispatch.sh, which
# fires the OTHER, pure process-bigraph-native mechanism (item 101/109) --
# this script is NOT that. Chain-dispatch is an external AWS-Batch-job-
# dependency orchestrator: one standalone Batch job PER GENERATION PER SEED,
# chained via an app-level scheduler poll loop, hand-off via a JSON-serialized
# daughter-state checkpoint at each generation boundary. Real, proven at
# 1000x10 production scale (item 71).
#
# Design doc (read this first if you're new to this system):
#   docs/plan/design-chain-dispatch-for-jim.md
# The real, actually-dispatched composite document ONE generation's job
# builds (not the whole campaign -- see the design doc §3 for why there is no
# single "whole campaign" document for this mechanism):
#   docs/assets/composites/chain-dispatch-ecoli-baseline-composite.json
#
# --- WHY THIS SCRIPT CANNOT "directly consume" that JSON the same way
# pbg-dispatch.sh consumes its own composite JSON -- a real, load-bearing
# architectural difference, not a scripting gap ---
# Most of that JSON's own `batch_runner.config` fields are NOT settable by a
# dispatch caller at all -- they are computed FRESH, server-side, for EACH
# generation's own job by SimulationServiceRay._seed_generation_command
# (viva_api/simulation/simulation_service_ray.py:921-1062):
#   cache_dir, out_dir, seed, initial_generation_index,
#   initial_carry_state_path, daughter_state_out_path, analyses, parallel,
#   stop_at_division (always True, unconditional -- item 103)
# A campaign-level dispatch call (this script) has no field that maps to any
# of these -- there is no "cache_dir" or "seed" query param on
# POST /api/v1/simulations, by design: the whole point of chain-dispatch's own
# scheduler (JobScheduler._advance_chain_campaign) is that IT decides these
# per generation, per seed, as the campaign progresses. This script's own
# params below are therefore CAMPAIGN-level knobs, not a 1:1 mirror of one
# generation's own config -- that correspondence is documented, not faked.
#
# What a caller DOES really control, confirmed directly from
# viva_api/api/routers/sms.py's real POST /api/v1/simulations handler:
#   - simulator_id / experiment_id / num_seeds / num_generations / run_parca
#     (named query params, below)
#   - simulation_config_filename -- a real, committed JSON config in the
#     target repo (e.g. sms-ecoli's configs/) that supplies most of
#     ecoli_baseline.baseline()'s own biological kwargs server-side (media,
#     knockouts, features, ...) for EVERY generation of EVERY seed
#   - injected_processes / variants / composite_id (item 93/105) -- generic
#     passthrough fields, real and working, but via the JSON body's
#     `extra_params`, not a query param -- see EXTRA_PARAMS below
#
# Prerequisite (not run by this script): a tunnel to the target env must
# already be up, e.g.
#   cd ../sms-cdk/scripts && AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1 \
#     ./sms-proxy.sh -s smsvpctest
#
set -euo pipefail

VIVA_API_BASE="${VIVA_API_BASE:-http://localhost:8080}"

# --- optional: pull a default EXPERIMENT_ID out of a real composite JSON,
# same CONFIG_FILE convention pbg-dispatch.sh uses. Only experiment_id is
# extracted -- see the header comment above for exactly why nothing else in
# that file's own `config` block corresponds to a real campaign-level param.
CONFIG_FILE="${CONFIG_FILE:-}"
_cfg_experiment_id_default() {
  if [[ -n "${CONFIG_FILE}" && -f "${CONFIG_FILE}" ]]; then
    local v
    v=$(jq -r '.state.batch_runner.config.experiment_id // empty' "${CONFIG_FILE}" 2>/dev/null)
    if [[ -n "${v}" ]]; then echo "${v}"; return; fi
  fi
  echo "chain-dispatch-smoke"
}

# NOTE: simulator_id defaults go stale -- if this default 404s, or you need a
# specific commit's own config files, build a fresh one first:
#   atlantis simulator latest --repo-url https://github.com/CovertLabEcoli/sms-ecoli --branch main
SIMULATOR_ID="${SIMULATOR_ID:-95}"
EXPERIMENT_ID="${EXPERIMENT_ID:-$(_cfg_experiment_id_default)}"
SIMULATION_CONFIG_FILENAME="${SIMULATION_CONFIG_FILENAME:-api_simulation_default.json}"

# Cheap, safe, already-proven defaults (sim257's own real scale, item 103's
# verification dispatch) -- override for a real production-scale run (e.g.
# NUM_SEEDS=1000 NUM_GENERATIONS=10, item 71's own proven ceiling).
NUM_GENERATIONS="${NUM_GENERATIONS:-3}"
NUM_SEEDS="${NUM_SEEDS:-1}"

# "true"/"false" (FastAPI bool query parsing). Set "" to omit (server default).
RUN_PARCA="${RUN_PARCA:-true}"

DESCRIPTION="${DESCRIPTION:-}"

# Generic composite-specific passthrough (item 93: injected_processes/
# variants; item 105: composite_id) -- real, working, JSON-body field, merged
# into the resolved SimulationConfig without overriding any named param
# above. Empty object omits it entirely (byte-identical to today's behavior
# for every existing caller). Example:
#   EXTRA_PARAMS='{"injected_processes": {"swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"}}}'
EXTRA_PARAMS="${EXTRA_PARAMS:-{\}}"

# curl's -G forces EVERY -d/--data-urlencode value (including a later JSON
# body) into the query string -- confirmed empirically, not assumed: `curl -G
# --data-urlencode "foo=bar" -d '{"a":1}' url` produces
# `url?foo=bar&{"a":1}`, mangling the JSON body into the query string instead
# of sending it as a real POST body. So the query string is built by hand
# below (jq's own @uri filter does the real percent-encoding) and a plain,
# -G-free POST sends it -- the only way to combine real query params with a
# real JSON body in one curl call.
_urlenc() { jq -rn --arg v "$1" '$v|@uri'; }

workflow() {
  local qs="simulator_id=$(_urlenc "${SIMULATOR_ID}")"
  qs+="&experiment_id=$(_urlenc "${EXPERIMENT_ID}")"
  qs+="&simulation_config_filename=$(_urlenc "${SIMULATION_CONFIG_FILENAME}")"
  qs+="&num_generations=$(_urlenc "${NUM_GENERATIONS}")"
  qs+="&num_seeds=$(_urlenc "${NUM_SEEDS}")"
  if [[ -n "${RUN_PARCA}" ]]; then
    qs+="&run_parca=$(_urlenc "${RUN_PARCA}")"
  fi
  if [[ -n "${DESCRIPTION}" ]]; then
    qs+="&description=$(_urlenc "${DESCRIPTION}")"
  fi
  local args=(-sS -X POST "${VIVA_API_BASE}/api/v1/simulations?${qs}")
  if [[ -n "${EXTRA_PARAMS}" && "${EXTRA_PARAMS}" != "{}" ]]; then
    args+=(-H "Content-Type: application/json" -d "{\"extra_params\": ${EXTRA_PARAMS}}")
  fi
  curl "${args[@]}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  workflow
fi
