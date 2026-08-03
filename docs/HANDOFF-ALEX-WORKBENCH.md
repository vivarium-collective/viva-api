# Handoff → Alex: vivarium-workbench, pre-demo

**Demo:** T+10h (2026-07-22). **Goal:** v2ecoli `baseline` runs on smscdk **from the
workbench UI**, prod `sms-api-stanford`. UI is mandatory — there is no CLI fallback.

You own **vivarium-workbench**. Jim owns **sms-api** (4 blockers, all traced). This
brief is written so you can start immediately without a sync call.

Source plan: `sms-api/docs/PRE-DEMO-PLAN.md` (yours). Corrections + evidence:
`sms-api/docs/PRE-DEMO-MASTER-PLAN.md`.

---

## 1. Two items in your plan are STALE — do not build them

Verified against the **running prod pod** (`workbench-67f7cbbf68-mglcf`, image 0.3.1),
not against the source tree.

### ✅ Transport / §3.3 / gate 2 — ALREADY FIXED, drop it entirely
The live page serves:

```
__DASH_CONFIG__ = { mode: "local-server", basePath: "/workbench" }
```

…with the `__BASE_PATH__` shim injected. `_base_path_shim` (`lib/report.py:416-436`)
globally patches `fetch` / `EventSource` / `XMLHttpRequest`, and its prefix list
(`:424`) already includes `/api/`.

§1's "Run → 404" and §2's root-cause-#1 describe the **pre-0.3.1** world. sms-api
0.9.23 + workbench 0.3.1 shipped the fix. There are ~40 root-absolute `/api/` call
sites (`study-detail.js`, `source-switch.js`, `github-login.js`,
`investigation-switcher.js`) — **all covered by that single shim**. The per-call-site
`apiUrl()` refactor in §3.3 is unnecessary; it would be ~40 edits that fix nothing
and risk regressions. **Cut, don't defer.**

### ✅ §3.5 steps/duration — mostly already plumbed
`composite_test_run_views.py:73` already does `steps = int(body.get("steps") or 5)`
and writes it to `request.json:109`. The UI sends it; **the default is 5, not 2700**
(composite-test-run is a *characterization* run). Only the remote hand-off is missing
— see item C.

---

## 2. FROZEN CONTRACT — sms-api ↔ workbench

This is settled; build against it and don't renegotiate. Read off the live code
(`sms_api/api/routers/compose.py:107-133`, `lib/sms_api_client.py:167-197`).

```
POST /compose/v1/simulation/run
  multipart/form-data:
    uploaded_file        <- raw .pbg JSON bytes   (field name is REQUIRED)
  query params:
    interval_time: float <- see the trap below    (default 1.0)
    extra_pip_deps: str  <- repeated param, e.g. git+https://…@<sha>
    batch_submission: bool (default false)
  → 200 { simulation_database_id: int, … }
```

**`client.compose_submit(...)` already implements exactly this** — you should not
need to touch `sms_api_client.py`.

### ⚠️ TRAP — `interval_time` IS the steps channel (overloaded name)
The chain is:

```
interval_time (query, float)
  → simulation_request.end_time_point            (compose.py:124)
  → steps = int(sim_request.end_time_point)      (compose/simulation_service_ray.py:100)
  → run_pbg.py -n <steps>
```

So **to run N steps you pass `interval_time=N`.** The name is a lie; it is not a
time interval. No sms-api change is needed to carry steps.

**Hard limit:** `compose.py:121-122` rejects `interval_time` outside `0..1000` with a
400. Baseline's default of 5 is fine; anything ≥1000 (e.g. `default_n_steps=2700`)
will be **rejected at the API boundary**. Cap or validate on your side.

### Other frozen facts
- `COMPOSE_RAY_IMAGE_TAG = a08e20bd84da94743952d209bc39db4cc8837ea9` — v2ecoli's
  pushed `origin/main` HEAD, the newest ECR tag, and the only commit with a ParCa
  cache in S3. Everything converges on this commit.
- Status polling: `client.compose_status`. A **persistent 404 on a run you believe
  you submitted = "submission failed on the deployment"**, not "still running" — do
  not poll it forever (your §3.13).

---

## 3. Your three work items (~2.5h total)

### A. Wire the SP-D stub → `run_remote` · ~1.5h
`lib/run_core.py:35-44` — `invoke_run` raises `RunTargetUnavailable` for
`target == "deployment"`. The client side is **already built**: `remote_run.run_remote`
(`lib/remote_run.py:84-164`) does clean+pushed → `export_composite_pbg` →
`compose_submit` → poll → download, and `git_pip_url` (`:28-81`) does the clean+pushed
check. Replace the raise with a delegation.

Net code should go **down**, per your §3.1. Also converge the legacy dashboard path
(`remote_run_jobs.py` `run_simulation(…, run_parca, …)`) onto it if it's cheap — if
it fights back, **leave it and move on**; it is not on the demo path.

### B. ⚠️ NEW BLOCKER — the UI currently runs baseline LOCALLY · ~0.5h
**This is not in your plan and it is the single biggest threat to the UI demo.**

`run_target_for` (`run_core.py:18-21`) returns `"deployment"` **only if
`.viv-build.json` exists** in the workspace, else `"local"`. Verified on the prod pod:

```
$ ls /workspace/.viv-build.json
ls: cannot access '/workspace/.viv-build.json': No such file or directory
```

And `composite_test_run_views.py:94` calls `invoke_run(...)` **with no `target=`
argument**. So the resolved target on prod is `local` → even with item A done, the Run
button spawns a **local subprocess inside the lightweight workbench pod**. For a full
E. coli model that OOMs or runs forever. It would have failed live, on stage.

Two fixes, cheapest first:
1. Stamp `.viv-build.json` into the prod workspace. Semantically defensible — prod's
   workspace *is* a materialized image build. Caveat: routes **all** runs remote.
2. Pass `target="deployment"` explicitly from `composite_test_run_views`. `invoke_run`
   already accepts the kwarg (`run_core.py:36`). No global behaviour change.
   **Preferred** — and it's the seed of your Phase-4 origin selector without
   committing to the full unification.

### C. Thread `steps` into the remote submit · ~0.5h
`composite_test_run_views.py:95` calls `invoke_run(..., n_steps=0)` — hardcoded zero,
while the real `steps` sits in a local variable at `:73` and goes only into
`request.json` for the local spawn path. Pass it through, and hand it to
`compose_submit` as `interval_time` (see the trap above).

---

## 3b. Release state (updated 2026-07-21 18:23)

**`v0.3.2` is released and its image is built** (`ghcr.io/vivarium-collective/vivarium-workbench:0.3.2`,
build 29857016174, 10m57s). It is the deploy target for the dev site.

**It does NOT contain items A / B6 / C.** The tag points at `b814ef8` (the version
bump); verified at that commit, `run_core.invoke_run` still raises
`RunTargetUnavailable` and `composite_test_run_views` still calls it with no
`target=`. So 0.3.2 ships the loom-vendoring / emitter / packaging work but **the
UI still cannot dispatch a remote run**. Your three items need a further release
(0.3.3).

**Budget ~11 min of image build per workbench iteration** — the build is
release-triggered and the image is ~15 GB. Batch your changes rather than cutting a
release per fix.

**Latent reproducibility hazard (follow-up, not blocking).** `Dockerfile:36` has
`ARG V2ECOLI_REF=main` and the build workflow passes no build-args, so the baked
v2ecoli is whatever `main` points at *at build time*. 0.3.2 happened to bake the
correct `a08e20bd…` only because v2ecoli main still sits there. The next merge to
v2ecoli main makes a rebuild silently disagree with the ECR runner image and the
ParCa cache — both of which are pinned to that commit. `PBG_PTOOLS_REF` (`:67`) is
exposed the same way. Worth pinning both as explicit build-args.

## 4. Coordination rules — non-negotiable

- **Deploy is serialized, not parallel.** sms-api must be on prod *before* the
  workbench points at it. Develop in parallel; deploy in sequence.
- **One person owns `kubectl` on prod.** We are going prod-direct with **no
  stanford-test rehearsal**, so there is no second namespace to fall back to. Two
  people rolling deployments in `sms-api-stanford` on demo eve = mystery outage.
- **Ping Jim before you deploy the workbench image.** Bump the `vivarium-workbench`
  `newTag` in `sms-api/kustomize/overlays/sms-api-stanford/kustomization.yaml`
  (currently `0.3.1`).

## 5. Explicitly OUT of scope (cut for the demo)

Gates 1, 3, 4, 5 from your §9 — the non-v2ecoli fixture workspace, characterization
surfacing (`available_observables` + `timing_summary`), persistence/rehydration, and
the unified run form / dropping `run_parca`. **Do not start Phase 4's study-model
work** — it's the highest-regression-risk item in the plan and has zero demo value.

## 6. Done =

From the workbench UI on prod: Composites tab → Explore → parameterize → **Run**
dispatches to sms-api (**not** a local subprocess), a Batch job appears, status polls
to completion, and results are readable. Small step count (5–20); do **not** demo
2700 steps — and note it would 400 anyway.

## 7. What Jim/Claude are fixing on the sms-api side (FYI — don't duplicate)

- **B1** `compose_ray_image_tag` defaults to `latest`; no such ECR tag exists.
- **B2** ParCa cache staging was dropped by the driver swap — the compose path calls
  `_submit_mnp` without `stage_s3`/`stage_dir`. (Your §3.10 says this gap is
  "dissolved"; it isn't — the staging lived on the ensemble `_sim_command` seam that
  Phase 1 replaced.)
- **B3** `run_pbg.py`'s generic core misses `ECOLI_TYPES` from `v2ecoli/core.py:42`.
- **B4** baseline's ParquetEmitter resolves `out_dir` to `<workspace>/.pbg/parquet-runs`
  — **outside** the S3-synced `RAY_OUT_DIR`, so results never leave the container.
  (Not in your plan; §3.9 closed the core-registration half but not the path half.)
- **B5** version never bumped — branch is still `0.9.23`, same as what's deployed.
