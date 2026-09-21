"""The environment every ``run_pbg.py`` invocation on the CD2 baseline / lineage paths runs under.

A leaf: two constants and the reasons for them. It sat at module level in
``simulation_service_ray.py``; the ensemble, chain and multi-node composite mechanisms all build
their commands from it, and a mechanism that has become a strategy object cannot import the service
(``docs/plan-core.md`` P2.1, PR 9). Moved verbatim, comments included.
"""

from viva_api.simulation.ray.image_paths import SIM_OUT_DIR, V2ECOLI_CORE_BUILDER, V2ECOLI_DIR

# ecoli_baseline.baseline()'s injection branch (taken whenever injected_processes
# is passed) does `from scripts._compare.inject import (...)` -- a bare absolute
# import that only resolves when V2ECOLI_DIR (which DOES contain scripts/, copied
# in by sms-ecoli's Dockerfile `COPY . .`) is on sys.path. Every run_pbg.py
# invocation below runs it via an absolute /tmp path, which makes CPython put
# /tmp on sys.path[0] instead of the cwd -- the `cd {V2ECOLI_DIR}` alone doesn't
# fix this; PYTHONPATH does. Found live 2026-09-01 (backlog item 93): a real
# chain-dispatch run with a non-empty injected_processes failed
# ModuleNotFoundError('scripts') despite the cd already being correct.
# PBG_REQUIRE_OUTPUT=1: on the CD2 Ray/baseline dispatch a run that produced no
# emitted store is always a failure, so run_pbg.py must exit non-zero instead of
# reporting success on the final_state.json fallback alone (audit §2.4 / P0-3).
# PBG_MIN_GLOBAL_TIME: the EFFECT half of the output guard (viva-api #395 / #375 §3e).
# PBG_REQUIRE_OUTPUT proves an emitted store EXISTS; this proves the generation
# actually RAN. A swap campaign that collapses to one tick (#375 §3d/§3e; the
# item-103 test measured "global_time never exceeded 1.0 across 10 chained
# generations") still writes a non-empty 1.pq and passes the presence check — and
# that is exactly the mode #387 can expose (a nested swap that used to be dropped
# now reaches the run). A real whole-cell generation advances hundreds-to-thousands
# of seconds of global_time, so a floor well above one tick (1.0) and far below one
# generation catches the collapse with no false-fail. Conservative and tunable; only
# set on this CD2 baseline/lineage path, NOT the generic compose path (which can run
# legitimately short composites).
PBG_MIN_GLOBAL_TIME = 10.0
PBG_RUNNER_ENV = (
    f"PBG_RESULTS_DIR={SIM_OUT_DIR} PBG_CORE_BUILDER={V2ECOLI_CORE_BUILDER}"
    f" PYTHONPATH={V2ECOLI_DIR} PBG_REQUIRE_OUTPUT=1 PBG_MIN_GLOBAL_TIME={PBG_MIN_GLOBAL_TIME}"
)
