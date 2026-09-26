- **2026-09-26** — **P4a-2, slice 4: the ingest hook is core's, behind `EventStore` and `ArtifactRegistrar`; its fifteen `Any` retired.**
  **Does it depend on slice 3?** No. The ingester reads runs, events and spans and never a dataset
  record; its one dataset touch is slice 1's feeder, which it now reaches through
  `ArtifactRegistrar`. Slice 3 (the record model, the reads) stays on hold for the #790 ruling.
  **What moved.** `viva_core/events/ingest.py` is `event_ingest.py` verbatim: JSON-lines parsing
  with the older top-level identity keys tolerated, span folding (a `span.end` without its start
  still creates the span), the per-depth `stage` rendering, progress folding, the span tree, the
  events-URI-to-key check against the one bucket the file service is bound to, and the per-tick
  loop — size-skip of unchanged objects, the object cap, idempotent inserts, open spans closed as
  `unknown` on a terminal run, and a failed registration withholding the cursor so the next tick
  re-reads. `viva_core/events/models.py` is `SimulationEvent` / `SimulationSpan` / `SpanTree`
  verbatim: the names are kept because they are schema components of the application's OpenAPI
  documents, and the open maps are `dict[str, object]`, which pydantic renders exactly as
  `dict[str, Any]` did (`additionalProperties: true`) — the SMS document is byte-unchanged, which
  `tests/core/test_two_openapi_documents.py` pins. Three seams: **`IngestRun`** (id, trace, the
  prefix the row recorded, the prefix the dispatcher would derive — the application knows the
  template — and liveness: terminal / end time / last event), **`EventStore`** (`events_cursor`,
  `list_spans`, `insert_events`, `upsert_spans`, `close_open_spans`, `update_progress`) and
  **`ArtifactRegistrar.register(events)`**. `settings` is an `object` read with `getattr`, as
  `events_env` already does — that alone retired most of the `Any`; the rest were `object` guards.
  **What stayed.** `viva_api/simulation/event_ingest.py` keeps `ingest_run_events(hpc_run, simulation,
  file_service, db, settings)` and `is_ingest_candidate(hpc_run, settings)` — what the scheduler and
  the tests call — and hands core `ingest_run()` (the row view, with `events_s3_prefix(settings,
  experiment_id)` as the default), `SmsEventStore` (over `DatabaseService`: `hpcrun_event`,
  `hpcrun_span`, the progress columns of `hpcrun`) and `SmsArtifactRegistrar` (over
  `dataset_registry.register_datasets`, looked up at call time so the existing patch point holds).
  `DISPATCH_COMPONENT = "viva_api.dispatch"` stays here: it names the application. Every pure
  function is re-exported, so the handlers, the CLI and the database service's own
  `parse_timestamp` import are untouched; `viva_api/simulation/models.py` re-exports the three
  models under their names. Adapter at the old name, not a `sys.modules` shim — the slice-1 reason.
  **Ripple.** With `payload` / `baggage` no longer `Any`, one application test read
  `baggage["experiment_id"].startswith(...)` on an `object` (now `str(...)`), and slice 1's
  `ArtifactEvent.payload` / `.baggage` became `dict[str, object]` too — a payload as read off the
  wire is open, and every field the registry uses was already `isinstance`-checked.
  **Found, not changed.** `IngestResult.hpcrun_id` and the log lines' "run" wording: the field
  name is the application's table inside core, same family as `attributes["hpcrun_id"]` (slice 1);
  kept verbatim because `tests/simulation/test_scheduler.py` constructs it — deferred list. The
  three promoted axes (`generation`, `variant`, `lineage_seed`) are model fields in core now; they
  are the coordinate keys slice 1 already hard-codes, not domain terms by the guard.
  **Proof:** `tests/core/test_events_ingest.py` (8: an in-memory store, an object-map file service
  and a scripted registrar — a pass stores, folds, reports and hands artifacts on; an unchanged
  object is skipped without a read and a rewritten one re-read; a failed registration withholds the
  cursor and the retry lands; a terminal run closes its spans; the four skip reasons; the candidate
  rule; parsing with an older stream); the application's ingest, registry, real-trace, scheduler,
  handler and CLI tests unchanged bar the one `str(...)` (389 with `tests/core`). `make check` clean
  twice; full suite green.
