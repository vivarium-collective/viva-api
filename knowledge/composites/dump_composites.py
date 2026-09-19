"""Dump the REAL, actual composite documents for both dispatch mechanisms,
built by calling the real registered composite-generator functions directly
(the same call run_pbg.py's own _resolve_document makes), against a real
downloaded ParCa cache (commit 4da4e43, a real complete cache; which dispatch
originally built it is unrelated to which dispatch's own params this script
reproduces below).
"""
import json

CACHE_DIR = "/tmp/item109-logs/real-cache"


def _default(o):
    return f"<non-serializable:{type(o).__name__}>"


def dump(doc, path):
    with open(path, "w") as f:
        json.dump(doc, f, indent=2, default=_default, sort_keys=False)
    print(f"wrote {path} ({len(json.dumps(doc, default=_default))} bytes serialized)")


def main():
    from process_bigraph.composite_generator import apply_core_extensions
    from process_bigraph.composite_spec import discover_specs, get as get_spec
    from v2ecoli.core import build_core

    discover_specs()

    # --- 1. pbg-native: dispatch database_id=288's own EXACT verbatim overrides
    # (pulled live from GET /api/v1/simulations/288 -- not approximated), merged
    # with lineage_ray_batch()'s own registered defaults for every field 288 did
    # not override (see that composite_generator's own decorator for the current
    # canonical default values -- re-check them fresh if this script's own output
    # ever stops matching the checked-in JSON, rather than trust this comment).
    #
    # Calls build_lineage_ray_batch_document() DIRECTLY -- the pure dict-builder
    # lineage_ray_batch()'s own body calls after prewarm_lineage_pool(); confirmed
    # by reading prewarm_lineage_pool's own source that it only mutates `core` as
    # a Ray-pool-sizing side effect and never touches the document, so this is
    # byte-identical to what to_document() would return, without the real local-
    # only ray.init()/working-dir friction that call path hits (reproduced
    # directly: UnicodeDecodeError inside ray's own .gitignore parsing on a bare
    # `to_document()` call from a fresh checkout, 2026-09-04).
    from v2ecoli.workflow.batch_lineage_ray import build_lineage_ray_batch_document
    doc1 = build_lineage_ray_batch_document(
        n_seeds=1,  # dispatch 288's real override
        n_generations=1,  # dispatch 288's real override (matches the registered default too)
        base_seed=0,  # registered default (288 did not override)
        cache_dir=CACHE_DIR,  # registered default is "out/cache" -- CACHE_DIR is the sanctioned
        # local substitution (288's own real container path isn't reproducible outside it)
        out_dir="",  # registered default (288 did not override) -- resolves via resolve_out_dir();
        # locally (no PBG_RESULTS_DIR/VIVARIUM_WORKBENCH_SWEEP_DIR set) this becomes
        # "out/batch_baseline", NOT the real container's own resolved value
        experiment_id="item109-j3-pbgnative-mechanics",  # dispatch 288's real override
        emitter="both",  # registered default (288 did not override)
        max_duration_per_gen=3600.0,  # registered default
        time_step=1.0,  # registered default
        media="minimal",  # registered default
        variants=None,  # registered default (288 did not use variants)
        injected_processes={  # dispatch 288's real override, verbatim
            "fork_repo": "",
            "add_processes": [],
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "exclude_processes": ["exchange_data"],
        },
        config_overrides=None,  # registered default
        emitter_arg=None,  # registered default (288 did not override) -- produces no key at all
        # in the resulting document, not a null one; confirmed empirically, not assumed
    )
    dump(doc1, "/tmp/item109-logs/real-pbg-native-composite.json")

    # --- 2. chain-dispatch: ecoli_baseline, real params matching _seed_generation_command ---
    core2 = build_core()
    spec2 = get_spec("v2ecoli.composites.ecoli_baseline.ecoli_baseline")
    assert spec2 is not None, "ecoli_baseline not registered"
    core2 = apply_core_extensions(spec2, core2)
    overrides2 = {
        "n_seeds": 1,
        "n_generations": 1,
        "stop_at_division": True,
        "cache_dir": CACHE_DIR,
        "out_dir": "s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/chain-dispatch-real-example/seed-0000/",
        "experiment_id": "chain-dispatch-real-example",
        "analyses": "none",
        "parallel": "",
        "seed": 0,
        "initial_generation_index": 0,
        "initial_carry_state_path": "",
        "daughter_state_out_path": "s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/chain-dispatch-real-example/daughter-states/seed=0000/generation=0000.json",
    }
    doc2 = spec2.to_document(overrides=overrides2, core=core2)
    dump(doc2, "/tmp/item109-logs/real-chain-dispatch-composite.json")

    print("DONE")


if __name__ == "__main__":
    main()
