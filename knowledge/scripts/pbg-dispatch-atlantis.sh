#!/usr/bin/env bash
#
# pbg-dispatch-atlantis.sh - process-bigraph-native multiseed/multigeneration
# dispatch (item 101/109), fired through the real `atlantis composite run`
# CLI command instead of a raw curl call. Sibling to pbg-dispatch.sh (curl-
# based) -- both fire the exact same logical request; proven byte-for-byte
# identical (this session, 2026-09-03): the same params through both paths
# produced an IDENTICAL JSON request body, verified with a hermetic,
# MockTransport-based comparison (viva-api's own httpx client mocked at the
# transport, no live server involved), not just eyeballed.
#
# `atlantis composite run` (viva-api app/cli.py, backlog item 101/109 CLI
# work): a new top-level `composite` command group, distinct from the
# existing `compose` group (item 98's OMEX/PBG/SBML file-upload family) --
# `atlantis composite run` dispatches ANY registered process_bigraph
# composite via the SAME multi_node_dispatch mechanism this script's own
# sibling curl script uses, not a new/different mechanism. See
# docs/plan/design-pbg-native-for-jim.md and viva-api's own Sphinx docs
# (docs/source/architecture/pbg-native-composite-dispatch.rst,
# docs/source/guides/composite-dispatch.rst) for the full design reference
# and CLI user guide.
#
# Requires the atlantis CLI to be runnable -- from the viva-api checkout:
#   uv run atlantis composite run --help
# This script assumes `atlantis` resolves on PATH (see viva-api's own docs
# for shell alias setup); if it doesn't in your shell, replace `atlantis`
# below with `uv run --project <path-to-viva-api> atlantis`.
#
# Prerequisite (not run by this script): a tunnel to the target env must
# already be up, e.g.
#   cd ../sms-cdk/scripts && AWS_PROFILE=stanford-sso AWS_DEFAULT_REGION=us-gov-west-1 \
#     ./sms-proxy.sh -s smsvpctest
#
set -euo pipefail

VIVA_API_BASE="${VIVA_API_BASE:-http://localhost:8080}"

SIMULATOR_ID="${SIMULATOR_ID:-97}"
EXPERIMENT_ID="${EXPERIMENT_ID:-item101-real-100x10-smoke}"

COMPOSITE_ID="${COMPOSITE_ID:-v2ecoli.composites.lineage_ray_batch}"
NUM_NODES="${NUM_NODES:-16}"
N_SEEDS="${N_SEEDS:-100}"
N_GENERATIONS="${N_GENERATIONS:-10}"
BASE_SEED="${BASE_SEED:-0}"
CACHE_DIR="${CACHE_DIR:-/app/v2ecoli/out/cache}"
EMITTER="${EMITTER:-both}"
MAX_DURATION_PER_GEN="${MAX_DURATION_PER_GEN:-3600.0}"
TIME_STEP="${TIME_STEP:-1.0}"
MEDIA="${MEDIA:-minimal}"
STEPS="${STEPS:-36000}"

# out_dir (item 109): unset (default) omits the flag, so lineage_ray_batch's
# own composite-level default applies unchanged. Pass a real s3:// URI to
# have every lineage's own parquet + xarray emitters write directly there
# instead -- see docs/plan/design-pbg-native-for-jim.md §6. NOTE: leaving
# this unset also means the deployment-standard automatic post-completion
# analysis job (see the architecture doc's own "Automatic post-completion
# analysis" section) will find real data -- setting OUT_DIR here reproduces
# the same real interaction gap verify-pbg-dispatch.sh exists to avoid.
OUT_DIR="${OUT_DIR:-}"

# n_workers: set to a real int to override; set to the empty string ("") to
# OMIT the flag entirely, letting the composite's own None default fall
# through to the cluster-derived RAY_SHARDS_DEFAULT env var. Matches
# pbg-dispatch.sh's own ${VAR-default} (no colon) convention deliberately --
# bash's `:-` treats an explicitly-empty value the same as unset, which would
# silently defeat the whole point of being able to pass N_WORKERS="" to omit.
N_WORKERS="${N_WORKERS-100}"

# Optional generic escape hatch, threaded straight to `atlantis composite
# run`'s own --params flag -- a raw JSON object merged OVER every named flag
# above. Empty (default) adds nothing. See viva-api's own composite-dispatch
# guide for when this is the right tool (any composite_id other than
# lineage_ray_batch, or a field this script has no named flag for).
EXTRA_COMPOSITE_PARAMS="${EXTRA_COMPOSITE_PARAMS:-}"

# ATLANTIS_BIN is a single string, split on whitespace into a real argv array
# below -- NOT quoted as one token -- so a multi-word override (e.g.
# `ATLANTIS_BIN="uv run --project ../viva-api atlantis"`, the header comment's
# own suggested fallback when `atlantis` isn't on PATH) actually runs `uv`
# with 4 real arguments instead of failing with "command not found" against
# one literal 5-word binary name. A real bug, caught by actually running this
# script, not just eyeballing it.
ATLANTIS_BIN="${ATLANTIS_BIN:-atlantis}"
read -r -a ATLANTIS_CMD <<< "${ATLANTIS_BIN}"

workflow() {
  local args=(
    composite run "${EXPERIMENT_ID}" "${SIMULATOR_ID}"
    --composite-id "${COMPOSITE_ID}"
    --num-nodes "${NUM_NODES}"
    --seeds "${N_SEEDS}"
    --generations "${N_GENERATIONS}"
    --base-seed "${BASE_SEED}"
    --cache-dir "${CACHE_DIR}"
    --emitter "${EMITTER}"
    --max-duration-per-gen "${MAX_DURATION_PER_GEN}"
    --time-step "${TIME_STEP}"
    --media "${MEDIA}"
    --steps "${STEPS}"
    --base-url "${VIVA_API_BASE}"
  )
  if [[ -n "${OUT_DIR}" ]]; then
    args+=(--out-dir "${OUT_DIR}")
  fi
  if [[ -n "${N_WORKERS}" ]]; then
    args+=(--n-workers "${N_WORKERS}")
  fi
  if [[ -n "${EXTRA_COMPOSITE_PARAMS}" ]]; then
    args+=(--params "${EXTRA_COMPOSITE_PARAMS}")
  fi
  "${ATLANTIS_CMD[@]}" "${args[@]}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  workflow
fi
