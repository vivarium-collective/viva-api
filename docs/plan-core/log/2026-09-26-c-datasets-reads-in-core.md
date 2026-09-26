- **2026-09-26** — **P4a-2, slice 3: `/viva/v1/datasets` is served by core, with core's record model on the P4a-1 columns; `/api/v1/datasets` is a dated facade.**
  Jim's ruling (a): #790 first, then this. It is the last of the four slices and the one the
  columns were for.
  **The record.** `viva_core/datasets/models.py` gains `Dataset` — `id`, `uri`, `kind`, **`owner_kind`
  / `owner_id`**, **`producer_job_id` / `trace_id`**, then the fields every row has had (`view`,
  `display_name`, `size_bytes`, `sha256`, `attributes`, `tags`, `source`, `available`, the two
  timestamps) — `DatasetPage` and `DatasetQuery`. Not one producer column: whose a dataset is, is
  the owner-ref; the run that wrote it is a job id and a trace; the three foreign keys are the
  application's storage of the same facts and do not appear on core's surface. **The store split in
  two:** `DatasetWriter` (upsert, `list_under`, `set_available` — all the feeders need, and all
  their in-memory doubles implement) and `DatasetStore(DatasetWriter)` adding `get`, `page`,
  `count`, `add_tags`, `attribute_values`, `tag_counts`. (`page`, not `list`: a method named `list`
  shadows the builtin inside its own class body and mypy stops resolving `list[...]` there.)
  **The routes.** `viva_core/api/routers/datasets.py` — the six reads, verbatim from the
  application's router and handlers bar the producer-id filters, which are `owner=<kind>:<id>` in
  core; `source` is `<kind>:<ref>` or a JSON fragment, the `sim:` shorthand stays the application's.
  Included by **`build_core_router`**, so a standalone core and the application that includes it
  serve it alike and both OpenAPI documents carry it (the union test demanded that; the interim
  compose / env-worker mounts are the exception, not the rule). The services come from a new
  **`DatasetServices`** group on the container (store, the deployment's kind vocabulary, the file
  service and its one bucket, the kinds never streamed); a core with none answers **503 by name**
  on every route, `/health` says `datasets: false`, and **`viva-v1-datasets`** is a *service*
  capability probed at request time — core's own probe on its container, restated in the
  application's `CAPABILITY_REGISTRY` so `/core/v1/capabilities` and `/viva/v1/capabilities` agree
  (a test pins that they do).
  **The application's side.** `SmsDatasetStore` answers the reads over `DatabaseService`, and the
  owner filter is two new clauses in `_dataset_filter_clauses` on the #790 columns — the SMS store
  grows, core stays `Any`-free. `DatasetDTO` gains the four columns (additive; the smoke `contract`
  reports an added key and accepts it). `/api/v1/datasets` is a **dated facade (D14, M4)** in its own
  docstring: shapes and producer-id filters unchanged; `/{id}/provenance` stays there until core has
  a job record (P4b). Core's query parsing (`viva_core/datasets/queries.py`) is the handler's,
  moved; the handler imports it back and keeps only its shorthands.
  **The deferred list, settled with the record.** (1) `attributes["hpcrun_id"]` — **gone from new
  rows**: the trace feeder now writes `producer_job_id` (= the run row) and `trace_id` through
  `DatasetWrite`, which `upsert_dataset` accepts; `span_id` stays in the attributes because a span
  has no column. Existing rows keep their attribute, and `Dataset`'s docstring says which rows carry
  which. (2) `IngestResult.hpcrun_id` → **`run_id`** in core (two test constructions in the
  application changed with it). (3) `attributes["analysis_dir"]` — **kept**: it is the walk's
  vocabulary as data (every walked row on dev carries it and the smoke `contract` recorded it), not
  a construct the guard objects to; renaming it is a data
  migration plus a contract change, and belongs with the kind vocabulary (P5b), not here.
  **Both OpenAPI documents regenerated** (plain `make spec` in this worktree, which has no `.dev_env`):
  the diff is the datasets routes and schemas, `version` (`main`'s 0.9.158) and the known-moving
  `created_at` example — no paths leaked.
  **Found while testing.** `create_core_app()` registers *its* container as the process-wide provider,
  so a core test that boots one leaves a later application test finding a standalone core's empty
  container (`/viva/v1/datasets` → 503). `tests/core/test_datasets_routes.py` saves and restores the
  provider around each test, the way `test_core_app_boots` does for one of its own.
  **Proof:** `tests/core/test_datasets_routes.py` (6: a page with its total under every filter,
  malformed filters as the caller's, one row / pickers / tags, content streamed and the three
  refusals, a core with and without a store); `tests/api/test_viva_datasets_routes.py` (1, Postgres:
  the family served from the table with the owner-ref off the #790 columns and the producing job
  on a traced row, the facade unchanged beside it with the four columns, provenance still SMS-only,
  both capability routes advertising); the application's dataset tests unchanged bar the
  `producer_job_id` assertion; the health dicts in five tests gain `datasets`. `make check` clean
  twice (new files intent-to-added first); full suite green.
