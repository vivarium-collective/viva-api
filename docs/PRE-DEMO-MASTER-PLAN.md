# Pre-Demo Master Plan — v2ecoli `baseline` on smscdk

**Demo:** T+10h. **Scope:** gate 7 only — v2ecoli `baseline` runs to completion on
smscdk and its output is readable. **Target:** `sms-api-stanford` (prod).
**Basis:** `docs/PRE-DEMO-PLAN.md` (Alex), narrowed to the demo critical path and
corrected against empirical findings below.

## Scope decision — what we cut

Alex's plan defines MVP as gates 1–6 with gate 7 ("the v2ecoli demo") layered on
top. **For a 10-hour runway that ordering is inverted.** Gate 7 is the demo; gates
1–6 are the platform story. Cut list, all deferred post-demo:

| Cut | Was | Why it's safe to cut |
|---|---|---|
| Gate 1 — non-v2ecoli fixture workspace | Phase 1 / §7 | Proves generality; nobody watches it tomorrow. The fixture doesn't exist yet — unestimated authoring work. |
| Gate 3 — characterization surfacing | Phase 3 | Pure UI polish. |
| Gate 5 — unified run form, drop `run_parca` | Phase 4 | **Highest regression risk in the whole plan** (touches the study model). Zero demo value. |
| Gate 2 — transport `/api/` base-path fix | Phase 2 | **Conditional — see Open Question.** Only needed if the demo is driven through the workbench UI. |
| Gate 4 — persistence/rehydration | Phase 4 | A demo run is watched live; it doesn't need to survive a reload. |
| Gate 6 — allowlist enforcement | already landed | Already done in Phase 1; no further work. |

Phases 2–5 of Alex's plan are **entirely deferred**. Nothing in vivarium-workbench
is on the critical path unless the Open Question resolves to "UI".

## Empirical baseline (verified today, not assumed)

- Prod `sms-api-stanford`: `api` = `sms-api:0.9.23`, up 19h. `workbench` = `0.3.1`.
  `ptools` = `0.5.9` (intentional). `alembic-migrate` last completed 6d ago.
- All four Stanford overlays (app + `-db-migration`, both namespaces) are **in
  sync at `0.9.23`** — no pre-existing drift. Good starting point.
- Branch `fix/compose-batch-driver-swap` is **still at version `0.9.23`** — the
  version was never bumped, so it collides with what's already deployed.
- `RAY_MNP_QUEUE` **is** populated from CDK stack output by each overlay's
  `secrets.sh` (`:185-194`) → the Ray compose backend will register. Not a blocker.
- v2ecoli local `HEAD = a08e20bd8`, clean, pushed to `origin/main`, and an ECR
  image `v2ecoli:a08e20bd84da94743952d209bc39db4cc8837ea9` exists (pushed 07-14).
- v2ecoli `pyproject.toml:55-56` pulls `pbg-emitters[parquet]` + `[xarray]` →
  the plan's "image already carries the emitters" claim **holds**.
- 3 analysis pods in `ImagePullBackOff` on prod — pre-existing, unrelated, ignore.

## Confirmed blockers

Four blockers. **B2, B3 and B4 are not in Alex's plan** — B2 and B4 are places the
plan explicitly declares a gap "dissolved" or handled.

### B1 — compose image tag resolves to a tag that doesn't exist · 5 min
`compose_ray_image_tag` defaults to `"latest"` (`config.py:261`) and is set in **no
overlay**. ECR repo `v2ecoli` contains only per-commit SHA tags — **there is no
`latest`**. Every compose job would fail at image pull.

**Fix:** set `COMPOSE_RAY_IMAGE_TAG=a08e20bd84da94743952d209bc39db4cc8837ea9` in the
`sms-api-stanford` overlay. Matches v2ecoli's pushed HEAD, so the running code and
the served workspace agree.

**Also decide:** change the `config.py` default off `latest` — a default that can
never resolve is a trap for the next deploy. Recommend defaulting to empty and
failing loudly at submit.

### B3 — the generic core is missing v2ecoli's registered types · ~1h
`run_pbg.py:_build_core()` registers `register_types(allocate_core())` plus the
three pbg-emitters links. v2ecoli's own `build_core()` (`v2ecoli/core.py:39-102`)
additionally registers:

- `ECOLI_TYPES` (`core.register_types(ECOLI_TYPES)`, `:42`) ← **the critical one**
- `RAMEmitter`, `SQLiteEmitter` from `process_bigraph.emitter` (`:49-51`)
- `KetchupEstimator`, `KetchupDynamicEstimator` (`:81-82`)
- `BiRDTransportProcess`, `BiRDTransportHours` (`:91-96`)
- `REPORT_CARD_STEPS` (`:102`)

Process *addresses* (`local:…`) resolve dynamically via importlib, so those may be
fine — but registered **types** do not. If `baseline`'s document references any
`ECOLI_TYPES` type, it fails to resolve at runtime.

**Fix (generalized, not v2ecoli-shaped):** have `run_pbg.py` prefer the workspace's
own core builder when one is available, falling back to the generic core. The
compose container *is* the workspace image, so `v2ecoli.core:build_core` is
importable inside it. Introduce an env var, e.g.
`PBG_CORE_BUILDER=v2ecoli.core:build_core`, set per-deployment alongside
`COMPOSE_RAY_IMAGE_TAG`. This keeps the "any workspace" property — each workspace
names its own builder — while unblocking gate 7. It is strictly more native than
hardcoding v2ecoli, and cheaper than reproducing `ECOLI_TYPES` generically.

### B2 — the ParCa cache staging was dropped by the driver swap · ~1–2h
Alex's §3.10 declares this gap "**inherited, not built … Gap dissolved.**"
**That is incorrect.** The staging is real but lives in the *ensemble* path:
`SimulationServiceRay` passes `stage_s3=cache_s3, stage_dir=PARCA_CACHE_DIR` into
`_submit_mnp` (`simulation_service_ray.py:503-504`).

`ComposeSimulationServiceRay.submit_simulation_job` calls the **same**
`_submit_mnp` (`compose/simulation_service_ray.py:105-112`) and **omits both
arguments**. The driver swap replaced the very seam the staging hung on.

Consequence: `baseline`'s `cache_dir` (`baseline.py:533`, consumed by
`load_cache_bundle`) points at an unpopulated directory → baseline cannot start.

**Fix:** pass `stage_s3=RayLayout.parca_cache_uri(commit)` and
`stage_dir=PARCA_CACHE_DIR` from the compose path. The machinery already exists;
this is wiring, not new capability.

**Open design point:** the compose request carries no commit to key the cache by.
Cheapest correct answer for the demo: derive it from `compose_ray_image_tag` (which
*is* the commit). Note this in code as a demo-time shortcut, not a design.

**Precondition — VERIFIED PASSING (2026-07-21).** The cache exists at
`s3://smscdk-shared-sharedbucket60d199d6-2u7sguagdihi/ray-parca-cache/a08e20bd84da…/`
— 6 objects, 253.3 MiB, written 2026-07-20, including `initial_state.json`,
`simData.cPickle`, and `sim_data_cache.dill`. No ParCa run is required; B2 stays a
wiring fix and the schedule holds.

It is also the **only** cached commit, and it coincides exactly with v2ecoli's
`HEAD`, its pushed `origin/main`, and the newest ECR image tag. So deriving the
commit from `compose_ray_image_tag` resolves correctly today. Still log as debt:
with a second cached commit the derivation becomes ambiguous.

### B4 — baseline's parquet output lands outside the synced directory · ~1h
Not mentioned anywhere in Alex's plan. `baseline.py:618-622` declares its emitter
with `out_dir` **deliberately omitted**: "the emitter step resolves it to
`<workspace>/.pbg/parquet-runs`". But the compose job syncs `RAY_OUT_DIR`
(`/tmp/pbg_out`, `compose/simulation_service_ray.py:37`) to S3.

So the run succeeds, the emitter resolves, parquet is written — to a path that is
never synced. S3 receives only `final_state.json`, and `observable_reader.py` has
nothing to read. **This silently defeats gate 7's acceptance criterion**, and it
fails in the most expensive way possible: after a full successful run.

**Fix (pick one):**
1. Have `run_pbg.py` apply the workspace's own emitter override
   (`set_parquet_emitter_override` / `set_emitter_override`, imported at
   `baseline.py:57-58`) to point `out_dir` at `RESULTS_DIR`. Native, uses the
   mechanism the workspace already exposes. **Preferred.**
2. Extend the compose command to also sync `<workspace>/.pbg/parquet-runs`.
   Cruder, workspace-shaped, but a smaller diff if (1) fights back.

### B5 — version not bumped · 15 min
Branch is `0.9.23`, identical to what's deployed. Per CLAUDE.md, reusing a tag
makes "did the rollout pull new bits?" unanswerable. Cut **0.9.24** and bump:
`sms_api/version.py`, `pyproject.toml`, and the `sms-api` `newTag` in all four
Stanford overlays — app **and** both `-db-migration` overlays. Migration
`e5a7c9d10f21` only runs if the db-migration overlay carries it.

## Sequencing (10h, with checkpoints)

Ordered so the **cheapest disqualifying fact surfaces first**.

**T+0:00 — Pre-flight (30 min, blocking).** Do this before writing any code.
1. Confirm a ParCa cache exists in S3 at `parca_cache_uri(a08e20bd…)`. **If absent,
   stop and re-plan** — B2 becomes a multi-hour ParCa run and the demo shape must
   change (see Contingency).
2. Re-run the sms-api test suite to a clean baseline (today's background run
   produced no output — rerun it).
3. Confirm `e5a7c9d10f21` chains off the current alembic head and that the
   `db_reconcile` fingerprint marker for it exists (CLAUDE.md flags this as a
   recurring miss).
4. Decide the Open Question below.

**T+0:30 — Code the four fixes (~3h).** B1 (config/overlay) → B3 (core builder) →
B4 (emitter out_dir) → B2 (cache staging). Order matters: B1/B3/B4 are independent
and cheap; B2 is the one that may expand.

**T+3:30 — Local verification (1h).** `make check` + full pytest. Add a test that
the compose submit path passes `stage_s3`/`stage_dir` (B2 regression) and one that
`_build_core` honours `PBG_CORE_BUILDER` (B3).

**T+4:30 — Rehearse on `sms-api-stanford-test` (1.5h).** See recommendation below.
Build `0.9.24`, apply, roll, **grep the live pod for a marker unique to the fix**
(CLAUDE.md Pitfall 1), run the migration Job, then submit a **short** baseline run.

**T+6:00 — Promote to `sms-api-stanford` (1h).** Identical tags. Migration Job
first, then app rollout.

**T+7:00 — Live end-to-end on prod (1.5h).** Submit baseline, watch Batch status,
confirm parquet lands in S3 and reads back.

**T+8:30 — Buffer (1.5h).** Do not spend it in advance.

## Recommendation: rehearse on stanford-test first

You said prod-direct. I'd push back once, then defer to you.

This change carries an Alembic migration against prod RDS **and** a brand-new
compute backend, on a day with no room to debug prod. Alex's own plan says land on
stanford-test first, then promote. The overlays are currently tag-identical at
`0.9.23`, so a rehearsal is nearly free: same manifests, same commands, one extra
build-free `kubectl apply`. It costs ~1.5h of a 10h budget and converts "unknown
prod failure at hour 9" into "known-good promotion at hour 6".

The plan above assumes rehearsal. Drop the T+4:30 block to go prod-direct and bank
the 1.5h — at the cost of prod being where you discover blocker #5.

## Decisions (settled)

- **Demo is driven through the workbench UI.** A CLI-only demo is a non-starter,
  so vivarium-workbench IS on the critical path and there is no CLI fallback.
- **Prod-direct.** No stanford-test rehearsal; trial runs happen on
  `sms-api-stanford` itself.
- **Divide and conquer** across Alex + Jim (+ me), per the split below.

## Workbench: verified live, and the scope is HALF what the plan says

Probed the running prod workbench pod (`workbench-67f7cbbf68-mglcf`, image 0.3.1).

### ✅ Gate 2 (transport) is ALREADY FIXED — cut it
The live page serves
`__DASH_CONFIG__ = { mode: "local-server", basePath: "/workbench" }` **with the
`__BASE_PATH__` shim injected**. `_base_path_shim` (`lib/report.py:416-436`)
globally patches `fetch`/`EventSource`/`XMLHttpRequest` and its prefix list
(`:424`) includes `/api/`.

**Alex's §1 ("Run → 404") and §2 root-cause-#1 are STALE** — they describe the
pre-0.3.1 world. sms-api 0.9.23 + workbench 0.3.1 already shipped the fix. The
~40 root-absolute `/api/` call sites across `study-detail.js` / `source-switch.js`
/ `github-login.js` are all covered by the global shim; §3.3's per-call-site
`apiUrl()` refactor is unnecessary and should be dropped, not just deferred.

### ⚠️ NEW BLOCKER B6 — the UI would run baseline LOCALLY, in the workbench pod
`run_target_for` (`lib/run_core.py:18-21`) returns `"deployment"` **only if
`.viv-build.json` exists** in the workspace, else `"local"`. Verified on prod:
`/workspace/.viv-build.json` **does not exist**. And `composite_test_run_views.py:94`
calls `invoke_run(...)` **without a `target=` argument**, so it resolves to `local`.

So even with SP-D wired, the UI's Run button spawns a local subprocess inside the
lightweight workbench pod — for a full E. coli model that fails or takes forever.
**This is the single biggest threat to the UI demo and it is nowhere in the plan.**

Fix, cheapest first:
1. Stamp `.viv-build.json` into the prod workspace → `run_target_for` returns
   `"deployment"` for everything. Arguably *semantically correct*: prod's
   workspace IS a materialized image build. Caveat: routes ALL runs remote.
2. Pass an explicit `target="deployment"` from `composite_test_run_views` (the
   `invoke_run` signature already accepts it). Slightly more code, no global
   behaviour change. **Preferred if time allows.**

### ✅ `n_steps` is already plumbed — much smaller than §3.5 claims
`composite_test_run_views.py:73` already reads `steps = int(body.get("steps") or 5)`
from the request body and writes it into `request.json:109`. The UI sends it and
the default is **5, not 2700** — the composite-test-run is a *characterization*
run. The only gap is that `invoke_run` is called with a hardcoded `n_steps=0`
(`:95`); the remote path needs `steps` threaded to the compose submit.

The "baseline default_n_steps=2700 is too long to demo" risk is therefore
**largely retired** — the run length is caller-controlled.

### Release state (2026-07-21 18:23)
`v0.3.2` released, image built and available. **Does NOT contain A/B6/C** — verified
at tag `b814ef8`: SP-D still stubbed, `invoke_run` still called without `target=`.
0.3.2 is a prerequisite for the dev deploy, not the demo-enabling release; that needs
0.3.3. Each workbench iteration costs ~11 min of release-triggered build (~15 GB image).

Follow-up (not blocking): `Dockerfile:36` `ARG V2ECOLI_REF=main` is unpinned and the
workflow passes no build-args, so a rebuild bakes whatever v2ecoli `main` points at.
0.3.2 baked the correct `a08e20bd…` only because main still sits there — the next
merge breaks agreement with the ECR runner image and ParCa cache. Same for
`PBG_PTOOLS_REF` (`:67`).

### Remaining workbench work (revised)
1. Wire `invoke_run` deployment target → `remote_run.run_remote` (SP-D). ~1.5h
2. Force/select the deployment target (B6). ~0.5h
3. Thread `steps` into the remote submit. ~0.5h

**~2.5h, down from ~3.5h**, and the transport work (~1h) is gone entirely.

## Divide and conquer — the split

Seam: the workbench calls sms-api over `POST /compose/v1/simulation/run`. Nothing
else crosses. **Freeze this contract in writing before either side codes:**
1. Exact submit payload — field names, where `steps` lives, what identifies the doc.
2. `COMPOSE_RAY_IMAGE_TAG = a08e20bd84da94743952d209bc39db4cc8837ea9` — both sides agree.
3. Status shape the workbench polls + the 404 "submission failed" case (§3.13).

| Owner | Scope |
|---|---|
| Alex | vivarium-workbench: SP-D wiring, target resolution (B6), `steps` threading |
| Jim | sms-api: B1–B5, build, migration, prod deploy |
| Claude | either side; sms-api recommended (all four blockers traced to exact lines) |

**Two non-negotiable coordination rules:**
- **Deploy is serialized.** sms-api must land on prod *before* the workbench points
  at it. Parallel development, sequential deployment.
- **One person owns `kubectl` on prod.** Two people rolling deployments in
  `sms-api-stanford` on demo eve is how you get a mystery outage — and with
  prod-direct there is no second namespace to fall back to.

Estimated wall-clock with the split: **~7h**, restoring real buffer.

## Risks carried into the demo

- **Run length.** `baseline`'s `default_n_steps=2700` (`baseline.py:611`). A demo
  run must use a small `-n`. Confirm a short run produces readable parquet —
  emitters sometimes only flush on interval boundaries. **Verify during rehearsal,
  not on stage.**
- **B2's commit-keying shortcut** (deriving the commit from the image tag) is
  demo-grade, not design-grade. Log it as tech debt before it calcifies.
- **No auth on `/compose/v1/*`** (Alex §8) — unchanged posture, tunnel-gated.
- **ALB `Target.Timeout` flake** (CLAUDE.md Pitfall 4) — if the CLI hangs, fall
  back to `kubectl port-forward`. Know this before the demo, not during it.
- **Prod compose has never run.** Every compose code path is first-execution on
  prod today. The rehearsal is what buys this risk down.

## Corrections to `PRE-DEMO-PLAN.md`

For Alex — these should land in the source plan regardless of demo outcome:

1. **§3.10 is wrong.** "ParCa cache write/stage hand-off is inherited … Gap
   dissolved" — the staging is not inherited by the compose path; the driver swap
   dropped the `stage_s3`/`stage_dir` arguments (B2).
2. **§3.9 / Phase 1 is incomplete.** Wiring the emitter core was necessary but not
   sufficient: baseline's emitter resolves `out_dir` to the workspace, outside the
   S3-synced results dir (B4). The output contract isn't closed.
3. **Phase 1's "deviation from plan (better)"** — targeting a prebuilt image — is
   in tension with §4C's N3 decision to run the workspace's pushed `origin@HEAD`.
   Today they coincide (image tag == HEAD) by luck. Worth stating explicitly which
   one is authoritative.
4. **`compose_ray_image_tag`'s `latest` default cannot resolve** against a
   per-commit-only ECR repo (B1).
5. Gate 7 should be re-ranked first in §9 for any time-boxed delivery.
