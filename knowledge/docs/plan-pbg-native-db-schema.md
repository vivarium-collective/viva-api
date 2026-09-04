# Plan: retrieving a pbg-native dispatch's real composite document

Written in response to Alex's own framing (verbatim, from the `/goal` procedure doc): `POST
/api/v1/simulations` is deliberately the single canonical dispatch endpoint for every client
(atlantis CLI, ptools, workbench UI) and every dispatch shape (legacy v1 single-cell, chain-dispatch,
pbg-native/`multi_node_dispatch`) — proof the mechanism is simulator-agnostic. But the `simulation`
table only ever stores the **request** (config/params); the actual **resolved process-bigraph
composite document** (the real dict `Composite()` gets constructed from, with real `ray:` addresses)
is built fresh, in-memory, inside the ephemeral container, and is never persisted anywhere. Wanted:

```bash
simulation_id=$(atlantis composite run <args>)
document=$(atlantis composite get "$simulation_id")
```

This need is not hypothetical — this session hit it directly. `viva-api/knowledge/composites/*.json`
are hand-reproduced local approximations (a script calling the same composite-generator function
locally against a downloaded ParCa cache), not captured from a real dispatch, because no capture
mechanism exists. That reproduction already drifted from reality once (params blended from two
different real dispatches, a missing `emitter_arg`, a stale `out_dir`) — this plan exists so that
never has to happen again.

## 1. Is there already a way to get this? — No, checked directly

- `GET /api/v1/simulations/{id}` (`viva_api/api/routers/sms.py:369`) returns the `Simulation` pydantic
  model, backed by `ORMSimulation` — its `config: JSONB` column holds the request (including
  `multi_node_dispatch.params`), never the resolved document.
- `run_pbg.py::_resolve_document()` (`viva_api/compose/run_pbg.py:313`) is the ONLY place the real
  document is constructed (`spec.to_document(overrides=overrides or {}, core=core)`), and it hands the
  result straight to `Composite(document, core=core)` a few lines later in `run()` — nothing writes it
  to disk or S3 today.
- No other table (`hpcrun`, `parca_dataset`, `analysis`, `worker_event`) has any field shaped to hold
  this either.

## 2. Recommended approach: write it to S3, alongside every other output — no new table, no migration

**`simulation.config` is already `JSONB`** (`viva_api/simulation/tables_orm.py:207`) — schema-less at
the Postgres level. But the cleaner answer doesn't even need a new key in it: the resolved document
can ride the **exact same S3 sync mechanism every other artifact already uses**, keyed by the same
`experiment_id` already sitting in the DB row.

**Write side — one new line, one existing mechanism.** `run_pbg.py::run()` already does, in order:
`results_dir.mkdir(...)` → `_resolve_document()` returns `document` → `Composite(document, core=core)`
→ `composite.run(steps)` → writes `final_state.json` to `results_dir`. `results_dir` (`SIM_OUT_DIR`,
`viva_api/simulation/simulation_service_ray.py:132`, `= {V2ECOLI_DIR}/.pbg/runs/phase0-xarray`) is
already synced to S3 by `ray-batch-entrypoint.sh`'s own periodic + final sync (`RAY_OUT_DIR` →
`RAY_OUT_S3`, confirmed live this session on a real dispatch: "node 0: periodic output sync
.../phase0-xarray -> s3://.../vecoli-output/<experiment_id>/"). Adding one write, right after
`_resolve_document()` returns:

```python
(results_dir / "composite_document.json").write_text(json.dumps(document, default=str))
```

requires zero new infrastructure — the file lands in the same sync sweep as `final_state.json`, at the
same deterministic prefix, automatically.

**Read side — reuse `RayLayout`, the exact primitive the existing outputs-download path already
uses.** `get_simulation_outputs` (`viva_api/common/handlers/simulations.py:1294`) already resolves a
simulation's S3 location purely from `simulation.config.experiment_id` — zero new bookkeeping — via
`data_layout.RayLayout.experiment_prefix(experiment_id)` (`viva_api/common/storage/data_layout.py:70`)
for the Ray/`multi_node_dispatch` backend specifically (`_stream_s3_tar_gz_ray`, line 1512). A new
handler follows the identical shape: look up the simulation, get `experiment_id`, build
`RayLayout.experiment_prefix(experiment_id)`, `GET` the single `composite_document.json` object via
`get_file_service()` (the same file service `_stream_s3_tar_gz_ray` already uses), return its parsed
JSON. Far simpler than the existing tar.gz streaming logic — one small file, not an archive.

**New endpoint**: `GET /api/v1/simulations/{id}/document` (`viva_api/api/routers/sms.py`, next to
`get_simulation`/`get_simulation_data`), handler `get_simulation_composite_document` in
`viva_api/common/handlers/simulations.py`. 404 if the simulation doesn't exist, 404 if no
`composite_document.json` exists at that prefix (chain-dispatch/legacy v1 simulations never write
one — this is additive, not a behavior change for anything that doesn't use it).

**CLI**: `composite get <simulation_id>` in `app/cli.py`, alongside `composite_run` (same
`composite_cli` Typer group). Hits the new endpoint, prints via `display_json` — matches
`composite_run`'s own established output pattern exactly (see `app/cli.py:1242`).

### Why this beats a new table/column

- **Zero Alembic migration** — directly resolves Alex's own stated worry ("the alembic migration
  would kind of suck balls"). Nothing changes in Postgres at all.
- **Zero new write-path plumbing from the container back to viva-api** — no callback, no new API call
  the container has to make under a job's own IAM role. It just writes a file where it already writes
  other files, using S3 permissions it already has.
- **Matches this project's own "reuse existing patterns first" rule literally** — the exact same
  primitive (`RayLayout.experiment_prefix`) already used for outputs, reused for the document too,
  not a parallel mechanism.

## 3. Real, named limitation — not silently glossed over

This only works when the dispatch resolves its output location to the deployment-standard
`RayLayout.experiment_prefix(experiment_id)` — i.e., `out_dir` was left unset (the common case, and
the recommended default per `reference-composite-param-default-shadows-env-var-pool-sizing.md`'s own
sibling guidance). **If a caller explicitly overrides `out_dir` to a custom S3 URI**, the document (and
every other output) lands there instead, and `GET .../document` would 404 even though the run
succeeded. This is the exact same shape of gap already documented for the auto-analysis mechanism
(`design-pbg-native-for-jim.md`'s own `out_dir`-doesn't-follow-to-auto-analysis note) — worth fixing
both together later (e.g. persist the resolved `out_dir` itself into `simulation.config` at submit
time, so a custom override is still discoverable), but out of scope for this specific ask.

## 4. Scope boundary

This plan covers `multi_node_dispatch`/pbg-native dispatches only (the ask's own explicit scope,
`v2ecoli.composites.lineage_ray_batch` and any future composite reached the same way). Chain-dispatch's
own per-generation jobs use a different, seed-scoped S3 layout (`RayLayout.seed_results_prefix`) and a
fundamentally different "one document per generation-job" shape rather than one document per campaign
— extending this same idea there is a separate, later question, not assumed answered by this plan.

## 5. Not built — this is the plan, implementation is separate

Per the `/goal`'s own explicit "plan mode" instruction, nothing above has been implemented. See backlog
item (number assigned in the same pass this plan was written) for the tracked, not-yet-started
implementation work: (1) the one-line write in `run_pbg.py::run()`, (2) the new handler + route, (3)
the new CLI subcommand, (4) regression tests for all three, (5) re-verify the `out_dir` limitation
above empirically before calling it done.
