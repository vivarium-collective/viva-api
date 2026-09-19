#!/usr/bin/env bash
# pbg-dispatch-baseline.sh — fully remote, fully pbg-native baseline dispatch reference.
#
# Fires the canonical s x g (seed x generation) pbg-native dispatch — v2ecoli's
# lineage_ray_batch composite, wired directly to N ray:-addressed LineageProcess
# nodes running ecoli_baseline's own biology, via the atlantis CLI's
# `composite run` command (item 101/109; viva-api PR#382, v0.9.89+) — NOT
# chain-dispatch, NOT any CD2-strain injection. This is the plain baseline case:
# the reference example every future pbg-native dispatch script should start from.
#
# Every parameter below is EXPLICIT, not left to a CLI default — this script is
# meant to be read as the full, qualified request shape, not just "run it and
# see." Every default value shown IS the atlantis CLI's own current default
# (confirmed against app/cli.py's composite_run command at merge time,
# viva-api commit a38d6432, and re-confirmed unchanged by PR#390/v0.9.90 -- that
# release only touched the analysis handler and simulation_service_ray.py, not
# app/cli.py) — spelled out here so nothing is implicit.
#
# As of v0.9.90 (viva-api PR#390 / v2ecoli PR#673), the auto-triggered
# post-completion analysis job also correctly reads this composite's real
# hive-partitioned parquet output -- no separate manual analysis step needed.
#
# Usage:
#   ./pbg-dispatch-baseline.sh                     # build a fresh simulator, fire with all defaults
#   SIMULATOR_ID=110 ./pbg-dispatch-baseline.sh     # reuse an already-built simulator
#   NUM_NODES=4 N_SEEDS=8 ./pbg-dispatch-baseline.sh
#
# Requires: AWS SSO session active, smsvpctest tunnel reachable at $ATLANTIS_BASE_URL
# (default http://localhost:8080 via sms-proxy.sh -s smsvpctest, or a kubectl
# port-forward — see viva-api/CLAUDE.md Pitfall 4).

set -euo pipefail

VIVA_API_DIR="${VIVA_API_DIR:-$HOME/sms/ecosystem/viva-api}"
ATLANTIS_BASE_URL="${ATLANTIS_BASE_URL:-http://localhost:8080}"

# --- Simulator: sms-ecoli main, fresh unless SIMULATOR_ID is given explicitly.
# Never silently reuse an old build across a session -- the standing lesson from
# this exact project (sim258's near-miss): a stale simulator can silently 404
# on a config, or predate a fix this dispatch exists to exercise.
SIMULATOR_ID="${SIMULATOR_ID:-}"
if [[ -z "$SIMULATOR_ID" ]]; then
  echo "No SIMULATOR_ID given -- building a fresh sms-ecoli simulator from origin/main..." >&2
  BUILD_JSON=$(
    cd "$VIVA_API_DIR" && \
    uv run atlantis simulator latest \
      --repo-url https://github.com/CovertLabEcoli/sms-ecoli \
      --branch main \
      --base-url "$ATLANTIS_BASE_URL"
  )
  SIMULATOR_ID=$(echo "$BUILD_JSON" | grep -o '"database_id": *[0-9]*' | head -1 | grep -o '[0-9]*')
  echo "Built simulator_id=$SIMULATOR_ID" >&2
fi

# --- Every composite_run parameter, explicit -- no reliance on unstated CLI defaults.
EXPERIMENT_ID="${EXPERIMENT_ID:-pbg-native-baseline-$(date +%Y%m%d-%H%M%S)}"
COMPOSITE_ID="${COMPOSITE_ID:-v2ecoli.composites.lineage_ray_batch}"   # CLI default
NUM_NODES="${NUM_NODES:-2}"                                            # CLI default
N_SEEDS="${N_SEEDS:-2}"                                                # lineage_ray_batch's own default
N_GENERATIONS="${N_GENERATIONS:-1}"                                    # lineage_ray_batch's own default
BASE_SEED="${BASE_SEED:-0}"                                            # lineage_ray_batch's own default
CACHE_DIR="${CACHE_DIR:-out/cache}"                                    # lineage_ray_batch's own default
OUT_DIR="${OUT_DIR:-}"                                                 # empty = deployment-standard location
                                                                        # (recommended -- this is also what the
                                                                        # auto-triggered analysis job reads from,
                                                                        # both before AND after the v0.9.90 fix)
EMITTER="${EMITTER:-both}"                                             # lineage_ray_batch's own default
N_WORKERS="${N_WORKERS:-}"                                             # empty = fall through to the
                                                                        # cluster-derived RAY_SHARDS_DEFAULT
                                                                        # (real per-node vCPUs x NUM_NODES) --
                                                                        # DO NOT set a concrete int here unless
                                                                        # deliberately capping concurrency; see
                                                                        # reference-composite-param-default-
                                                                        # shadows-env-var-pool-sizing.md
MAX_DURATION_PER_GEN="${MAX_DURATION_PER_GEN:-3600}"                   # lineage_ray_batch's own default (seconds)
TIME_STEP="${TIME_STEP:-1.0}"                                          # lineage_ray_batch's own default (seconds)
MEDIA="${MEDIA:-minimal}"                                              # lineage_ray_batch's own default
STEPS="${STEPS:-36000}"                                                # CLI default -- total simulated seconds
                                                                        # requested for the whole run
                                                                        # (N_GENERATIONS x MAX_DURATION_PER_GEN
                                                                        # is the real ceiling this must cover)
DESCRIPTION="${DESCRIPTION:-Fully remote pbg-native baseline dispatch (item 109 reference)}"

echo "Dispatching:" >&2
echo "  experiment_id=$EXPERIMENT_ID  simulator_id=$SIMULATOR_ID  composite_id=$COMPOSITE_ID" >&2
echo "  num_nodes=$NUM_NODES  n_seeds=$N_SEEDS  n_generations=$N_GENERATIONS  base_seed=$BASE_SEED" >&2
echo "  cache_dir=$CACHE_DIR  out_dir=${OUT_DIR:-<deployment-standard>}  emitter=$EMITTER" >&2
echo "  n_workers=${N_WORKERS:-<cluster-derived RAY_SHARDS_DEFAULT>}" >&2
echo "  max_duration_per_gen=$MAX_DURATION_PER_GEN  time_step=$TIME_STEP  media=$MEDIA  steps=$STEPS" >&2

cd "$VIVA_API_DIR"
CMD=(uv run atlantis composite run "$EXPERIMENT_ID" "$SIMULATOR_ID"
  --composite-id "$COMPOSITE_ID"
  --num-nodes "$NUM_NODES"
  --seeds "$N_SEEDS"
  --generations "$N_GENERATIONS"
  --base-seed "$BASE_SEED"
  --cache-dir "$CACHE_DIR"
  --emitter "$EMITTER"
  --max-duration-per-gen "$MAX_DURATION_PER_GEN"
  --time-step "$TIME_STEP"
  --media "$MEDIA"
  --steps "$STEPS"
  --description "$DESCRIPTION"
  --tag "item109" --tag "baseline-reference"
  --base-url "$ATLANTIS_BASE_URL"
)
[[ -n "$OUT_DIR" ]] && CMD+=(--out-dir "$OUT_DIR")
[[ -n "$N_WORKERS" ]] && CMD+=(--n-workers "$N_WORKERS")

"${CMD[@]}"
