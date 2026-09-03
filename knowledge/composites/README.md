# Real, actually-dispatched composite documents — pbg-native vs. chain-dispatch

Both files in this directory are **real output**, not hand-written illustrations. Each was
produced by calling the exact Python function the real dispatch path calls at runtime
(`process_bigraph.composite_spec`'s `to_document()` machinery, the same call
`viva_api/compose/run_pbg.py`'s own `_resolve_document()` makes on every real dispatch — see
`../plan/design-pbg-native-for-jim.md` §"The shared entrypoint" for the full trace), run locally
against a real, complete ParCa cache (commit `4da4e43`, downloaded verbatim from
`s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/ray-parca-cache/4da4e43/` — the exact
cache a real dispatch, `database_id=282`, used and succeeded against on 2026-09-03).

## Files

- **`pbg-native-lineage-ray-batch-composite.json`** — `v2ecoli.composites.lineage_ray_batch`, built
  with the exact params dispatch 282 used (`n_seeds=2, n_generations=1, base_seed=0, emitter=both,
  media=minimal`), except `out_dir`/`cache_dir` point at this reproduction's own local paths.
  **Regenerated 2026-09-03** against the `feat/item109-lineage-ray-batch-injection-exposure` branch
  (v2ecoli PR #663) to show the now-real `injected_processes` capability — the `swap_processes`/
  `exclude_processes` block on each node is the exact real content dispatch `database_id=288` used
  on real GovCloud infra (`configs/cd2/run2_j3_injected_metabolism.json`), not a hypothetical
  example. See `../plan/design-pbg-native-for-jim.md` §11a for the full trace.
- **`chain-dispatch-ecoli-baseline-composite.json`** — `v2ecoli.composites.ecoli_baseline`, built
  with the exact params `SimulationServiceRay._seed_generation_command` constructs for one real
  chained generation job (`n_seeds=1, n_generations=1, stop_at_division=True, analyses=none,
  parallel=""`), which is what item 103's `stop_at_division` fix (viva-api PR #369, `v0.9.84`) routes
  through — the real, current shape of every chain-dispatch generation since that fix, not the
  pre-fix "plain single-cell, no division gate" shape.

## How to reproduce this yourself

```bash
# 1. A real ParCa cache -- either download a real one (fastest, matches real infra exactly):
aws s3 sync "s3://smsvpctest-shared-sharedbucket60d199d6-abfvwv0day91/ray-parca-cache/<commit>/" \
  /tmp/real-cache/ --profile stanford-sso --region us-gov-west-1
# ...or build one locally: cd v2ecoli && uv run v2ecoli-parca --mode fast --out /tmp/real-cache

# 2. Run the dump script (see ../plan/design-pbg-native-for-jim.md Appendix A for the full source)
cd v2ecoli && uv run python dump_composites.py
```

Both documents are exactly what a real dispatch builds and runs — nothing here is illustrative or
simplified. The only fields that differ from an actual GovCloud dispatch are `cache_dir`/`out_dir`
(local paths here vs. real `s3://` URIs there — see each design doc for the real values a live
dispatch uses).
