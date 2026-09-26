- **2026-09-26** — **P4b, slice 1: the JobStore seam lands, reshaped per D15 — #776 kept where it was right, changed where the plan had moved.**
  Jim's rulings (2026-09-26, "go with your recommendations"): Q1 `Job.backend`; Q2 a
  `JobReader` / `JobStore` split with **no `put`** until P7, the oracle seeding through a
  callable; Q3 leaving a terminal status raises; Q4 the application's `/viva/v1/jobs` is a
  composite store with table-prefixed ids (slice 3); Q5 `task.trace_id` + `task.events_prefix`
  as one small migration at checkpoint G (slice 4); Q6 `ck_dataset_producer` relaxed on the DB
  checkpoint after G (slices 5–6); Q7 a superseding PR of our own, eagmon's commit cherry-picked
  first so his authorship survives.
  **Kept from #776 (eagmon):** `Job` as a frozen dataclass on `viva_core.models.JobStatus` — the
  thing that keeps P7's collapse small; the async Protocol; the conformance oracle as a reusable
  function the adapter tests run; the cancel-as-a-fold property, both phases (#709 / #710).
  **Reshaped:** `owner_kind` is a **string** (core names only its own kinds — `OWNER_TASK`,
  `OWNER_ENV_WORKER`, `OWNER_CAMPAIGN`; an application's are its table names, #790's rule, and an
  application cannot extend a core enum); `owner_id` is a string, never `None`; `external_id`
  became **`external_job_ids`** (the live handles) beside **`companion_job_ids`** (what must die
  with the job, cancelled first — today a filter in `cancel_companion_jobs`, now a field);
  **`backend`** joined the record because the scheduler's ticks select on `job_backend` as much as
  on `job_type` (active / local / Nextflow / analysis queries) and the cancel fold needs it to
  build a `JobId`; `trace_id` too (the ingester and the datasets' `producer_job_id` want it).
  **`JobReader`** (`get` → `Job | None`, `by_owner`, `by_kind_status(statuses, kind=, backend=)`)
  is what every adapter implements; **`JobStore(JobReader)`** adds `set_status` (raises
  `JobIsTerminal` on leaving a terminal status) and `set_external_job_ids`. **No `put`**: an
  interim adapter over `hpcrun` cannot insert a `Job` (the row needs a `JobId`, a job type and a
  reference the record does not carry), so the application's dispatchers keep inserting their own
  rows until P7's `core.job` store is the first to create records through the seam. The
  in-memory store moved to **`viva_core/tasks/testing.py`** with a guard that nothing outside
  `tests/` imports it (Jim's review, the #414 family); `runtime_checkable` dropped.
  **The oracle** (`tests/core/jobstore_oracle.py`): `assert_job_reader_conformance(store, seed,
  owner_kind=, other_kind=)` and `assert_job_store_conformance(store, seed, owner_kind=)`, where
  `seed` is how the TEST puts a row in — the in-memory store seeds itself, an adapter test seeds
  through the table's own writer on the Postgres testcontainer. Runs here on memory as the fast
  specification check; the adapters run it on Postgres (slices 2–3), which is the real test.
  **Nothing wired**: no container field, no route, no caller. Slices 2–4 (the SMS adapters, the
  env-worker adapter + `/viva/v1/jobs` reads + `viva-v1-jobs`, tasks as jobs) follow one at a time.
  **Proof:** `tests/core/test_jobstore.py` (5: the in-memory store type-checks as both Protocols;
  the oracle on memory; one shape for three owners; the cancel fold in both phases, companions
  first; the fence, an AST scan of `viva_core` / `viva_api` / `app` / `scripts`); the standalone
  gate, the vocabulary scan and the no-`Any` glob pass with `viva_core/tasks/` inside. `make check`
  clean (second run); `tests/core` 288 passed; full suite 2440 passed, 63 skipped.
