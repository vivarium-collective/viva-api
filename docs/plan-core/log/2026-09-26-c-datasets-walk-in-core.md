- **2026-09-26** — **P4a-2, slice 2: the walk is core's, behind `ArtifactClassifier` and `WalkSource`.**
  **What moved.** `viva_core/datasets/walk.py` is `dataset_walk.py` verbatim: the one listing per
  source, bundles as the root's child directories, "nothing consumable is not a bundle", rows
  registered from the listing alone (never an object opened, viva-api#673/#675), a walk never
  downgrading an event row, an event row whose object is back flipped available, vanished objects
  and vanished bundles marked, `WalkResult` and its merge, `split_s3_uri`. Two seams replace the
  application's knowledge: **`ArtifactClassifier.classify(relative) -> Artifact | None`** — which
  paths under a bundle are datasets, their kind, and the view / protocol / coordinate the name
  encodes (an `Artifact` is what a classifier reads off a path; the display name and the
  `source.coordinate` are built from it in core, unchanged) — and **`WalkSource`** — a `root_uri`,
  an `owner` for bundles nobody claims, a `label`, `tags`, a `subject`, and `claimant(bundle_uri)`.
  The old `_producer_for_bundle` is the claimant call plus the `unclaimed` count, in core.
  `DatasetStore` gained `list_under(uri_prefix)` (every row under a prefix, available or not — the
  application's store pages through `DATASET_LIST_MAX_LIMIT` behind it) and `set_available`.
  **What stayed.** `viva_api/simulation/dataset_walk.py` keeps the naming convention — `PTOOLS_DIR`,
  `VIZ_DIR`, `REPORT_NAME`, `SCALES`, `ArtifactName`, `parse_artifact_name`, `classify` (the three
  domain terms the gate counted) — as `SmsArtifactClassifier`; `analyses_root_uri` (the emitter's
  `out_uri`, else the Ray layout) and "the analysis run whose `result_uri` is the bundle directory
  claims it" as `SimulationWalkSource`; and the two entry points, `reconcile_simulation(simulation,
  db=, file_service=, storage_bucket=)` and `register_bundle(items, bucket=, bundle_key=, producer=,
  simulation=, db=, tags=, existing=)`, with the signatures the scheduler's tick, `scripts/walk_*`,
  the CD2 importer and the tests call. `dataset_store.owner_ref` is the inverse of slice 1's
  `producer_ref`, for the importer that still speaks producer columns. Not a `sys.modules` shim,
  for the slice-1 reason. `handlers/analyses.py` keeps importing `parse_artifact_name` from here.
  **Found, not changed.** `attributes["analysis_dir"]` — the bundle directory's name under a key
  that says what the application's bundles are. Kept verbatim (every walked row on dev carries it);
  same deferred item as `hpcrun_id` (slice 1), for the record model in slice 3.
  **Proof:** `tests/core/test_datasets_walk.py` (9: an in-memory store, a listing double, a one-rule
  classifier and a scripted source — a bundle's datasets from the listing alone, a claimed bundle
  and the move when a claim appears, twice changes nothing and an event row is never downgraded,
  gone objects / gone bundles / a returning event row, another bucket skipped without a listing, tags
  and a store refusal in `register_bundle`); the application's walk, scripts and router tests
  unchanged (310 passed with `tests/core`). `make check` clean twice; full suite green.
