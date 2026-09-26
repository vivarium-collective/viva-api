- **2026-09-26** — **P4a-2, slice 1: the trace feeder is core's, behind `OwnerResolver` and `DatasetStore`.**
  **The sequence.** The dataset code was scored against the three gates core enforces (the
  vocabulary guard's AST scan · a count of `Any` · imports of the application that are not shims):
  `dataset_registry.py` 291 lines, 4 terms (all in `_producer`), 0 `Any`, 3 imports;
  `dataset_walk.py` 400, 3 terms (all in `classify` and the bundle layout), 0, 4;
  `handlers/datasets.py` 360, 6 terms, 0, 5; `routers/datasets.py` 202, 4 terms, 2, 6;
  `event_ingest.py` 637, 0 terms, 15, 3. None passes today, and the blocker is the same in each: the
  three producer foreign keys — the application's tables — which is exactly what the Protocols are
  for. Four slices, one concern each: (1) the trace feeder behind `OwnerResolver` + `DatasetStore`
  (this entry); (2) the walk behind `ArtifactClassifier` + `WalkSource` (`classify`,
  `parse_artifact_name` and the analysis-claims-a-bundle lookup are the application's); (3) the reads
  as `/viva/v1/datasets` with core's own record model and the `viva-v1-datasets` capability,
  `/api/v1/datasets` a dated facade (D14); (4) the ingest hook into `viva_core/events/`. The plan's
  kind vocabulary (`table | figure | store | log`) is not a code move — every row carries today's
  kinds — and waits for the datasets-shaped `sms.js` (P5b).
  **What moved.** `viva_core/datasets/registry.py` is `dataset_registry.py` verbatim: validation, the
  coordinate, display names, tags, the subject, `available = not error`, the skip-and-count loop.
  Three shapes replace the application's types: `ArtifactEvent` (payload, baggage, span, and the
  coordinate axes and label the application promotes from baggage — the engine never names those
  keys), `RunContext` (run id, label, tags, the subject the data is OF), and `DatasetStore.upsert`.
  `viva_core/datasets/models.py` carries `OwnerRef = (owner_kind, owner_id)` — the P4a-1 rule spoken
  by core from day one, the owner kinds being the owning tables' names as #790's `dataset_owner`
  spells them — `DatasetWrite`, `UpsertAction`, and a structural `DatasetRecord` the application's
  DTO already satisfies.
  **What stayed, and why the old name is an adapter, not a shim.** `viva_api/simulation/dataset_registry.py`
  keeps `register_datasets(events, hpc_run=, simulation=, db=)` — the signature the ingester and the
  seven Postgres tests call — and hands core `SmsOwnerResolver` (the `_producer` body: a ParCa cache
  belongs to the simulation's ParCa dataset, a baggage `analysis_id` that names a real analysis wins,
  else the run's own reference), `SmsDatasetStore` (`viva_api/simulation/dataset_store.py`: owner
  kind → producer column, the only place the translation lives) and `DATASET_KINDS`. A `sys.modules`
  shim needs the old name's API to BE the new module's, and this one takes `HpcRun`, `Simulation`
  and `DatabaseService`; the P3d-3 precedent (a hook module at the old name) applies.
  **Found, not changed.** Rows record `attributes["hpcrun_id"]`; the key is the application's table
  name inside a core string. Kept verbatim because every row on dev carries it; renaming it is a
  data decision for the record model (slice 3) — deferred list.
  **Contradiction with the plan's order.** P4a lists the owner-ref expand (P4a-1, #790) before the
  move; #790 is open and checkpoint F has not run, so nothing here writes `owner_kind` /
  `owner_id` columns. Core speaking the pair anyway is what makes that harmless: when #790 lands the
  adapter dual-writes in one line, and nothing in core changes.
  **Proof:** `tests/core/test_datasets_registry.py` (8: the rules on an in-memory store and a scripted
  resolver, including "a store failure that is not a refusal propagates"); the application's seven
  Postgres tests unchanged; the standalone gate, the vocabulary scan and the no-`Any` glob pass with
  `viva_core/datasets/` inside. `make check` clean twice; full suite green.
