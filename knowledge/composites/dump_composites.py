"""Dump the REAL, actual composite documents for both dispatch mechanisms,
built by calling the real registered composite-generator functions directly
(the same call run_pbg.py's own _resolve_document makes), against a real
downloaded ParCa cache (commit 4da4e43, the exact cache dispatch 282 used).
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

    # --- 1. pbg-native: call build_lineage_ray_batch_document() DIRECTLY --
    # (the pure dict-builder lineage_ray_batch()'s own body calls after
    # prewarm_lineage_pool(); it takes no `core` and touches no Ray at all --
    # bypasses a local-dev-only ray.init()/uv-working-dir friction that has
    # nothing to do with the document's real content. Byte-identical to what
    # to_document() would return, since prewarm_lineage_pool is a pool-sizing
    # side effect on an out-of-band actor runtime, not a document mutation.)
    from v2ecoli.workflow.batch_lineage_ray import build_lineage_ray_batch_document
    doc1 = build_lineage_ray_batch_document(
        n_seeds=2,
        n_generations=1,
        base_seed=0,
        cache_dir=CACHE_DIR,
        experiment_id="item109-out-dir-s3-verify",
        emitter="both",
        max_duration_per_gen=3600.0,
        time_step=1.0,
        media="minimal",
        out_dir="s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/item109-out-dir-verify/",
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
