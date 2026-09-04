# Scoping report: triggering pbg-native (`lineage_ray_batch`) dispatch from the vivarium-workbench UI

**Status: planning only.** No code written, no PR opened, no git state touched in `vivarium-workbench` or `viva-api`. Everything below is read from current source (commit-level, not memory) on 2026-09-03.

**Why this exists**: `atlantis composite run` (viva-api CLI, PR #382) is the currently-proven way to fire a pbg-native `lineage_ray_batch` dispatch (item 101/109) — used for every real dispatch this session (253, 255, 282, 283, 287, 288). Proving via the CLI first is deliberate — faster iteration, easier to reproduce, no browser needed. But the CLI is not the end state: once the design is triply verified via the CLI, the same dispatch capability needs to be reachable from the vivarium-workbench web UI. This report scopes that gap.

## 0. The headline, before the detail

The premise this report was scoped against — "item 88 found zero UI dispatch path in vivarium-workbench" — is **stale**. That was true for colony/multi-node dispatch specifically at the time it was checked (a prior session's full client grep found "one hit, a comment, zero real code"). It is no longer the accurate starting point: a real, mature, already-proven remote-dispatch mechanism exists in the Study tab today, complete with auth, async job tracking, and — critically — a **generic, untyped `extra_params` passthrough** built specifically for items 86/88 so a composite-specific dispatch shape is never silently dropped. That passthrough is capable, right now, server-side, of carrying a `multi_node_dispatch` payload in the exact shape `atlantis composite run` already proves works.

**So the real gap is narrower than "build a UI dispatch path from scratch."** It's: (1) composite selection is constrained to whatever the pinned simulator's own build resolves as its default — there's no way to say "dispatch `lineage_ray_batch` specifically" the way `--composite-id` does on the CLI; and (2) no client-side form exists to let a user actually compose an `injected_processes`/`variants`/`num_nodes`/`n_seeds`/`n_generations`-shaped request — the pieces that would populate the passthrough are not exposed as UI controls today.

## 1. Current state, with citations

### 1a. The real, working remote-dispatch chain (Study tab → viva-api)

```
Study tab "Run current spec" (client JS, deployment-pinned session)
  → POST /api/remote-run-submit          (vivarium_workbench/api/app.py:6854)
  → lib.remote_run_views.remote_run_submit()   (remote_run_views.py:297)
  → lib.sms_api_client.SmsApiClient.run_simulation()   (sms_api_client.py:180)
  → POST /api/v1/simulations              (real viva-api call, proven this session
                                            by dispatches 253/255/282/283/287/288)
```

- `remote_run_views.py:297` (`remote_run_submit`) is real, mature infrastructure: a real auth gate (`_run_auth_ok()`), a required `num_generations`/`num_seeds` guard (explicitly documented as "never silently defaulted to 1" — a real incident this guards against), a real 502-not-500 error path (item 51's own fix), and a Runs-tab visibility write (item 84's own fix).
- `sms_api_client.py:180` (`SmsApiClient.run_simulation`) already accepts and forwards `extra_params` verbatim into the real `POST /api/v1/simulations` request body's own `extra_params` key — the exact field name and nesting `multi_node_dispatch` lives under in every real pbg-native dispatch this session fired.
- `vivarium_workbench/lib/study_runs.py:582-598` (inside `run_study_baseline`, verified directly against current source): on a deployment-pinned target, ANY generator-override key beyond `n_generations`/`n_seeds` rides through to `extra_params` generically —
  ```python
  extra_params = {
      k: v for k, v in generator_overrides.items()
      if k not in ("n_generations", "n_seeds")
  }
  ```
  with an explicit comment citing "Backlog items 86/88" and stating "no key here is inspected or special-cased by name." **This means a `multi_node_dispatch` key, if present in `generator_overrides`, already flows through today, with zero new server-side code.**
- `study_runs.py:544-548`: `generator_overrides = params; generator_overrides.update(body.get("overrides") or {})` — `params` is the study's own configured baseline params, and `body["overrides"]` is a **live request-body override** layered on top. A client COULD send `{"overrides": {"multi_node_dispatch": {...}}}` in the same POST body that triggers "Run current spec," and it would reach viva-api unmodified.
- `api/app.py:6854-6861` — `/api/remote-run-submit`'s own FastAPI route signature is `req: Union[dict, None] = Body(default=None)` — **untyped**. No Pydantic model constrains what a caller may send. Nothing server-side needs to change to accept a `multi_node_dispatch`-shaped `overrides` block.

### 1b. The real, but narrower, constraint

`study_runs.py:582-586`'s own comment states the mechanism's real limit precisely: "scoped narrowly to the case that mechanism actually supports: the study's DEFAULT baseline entry (entry is `baseline[0]`, no explicit `?composite=` override)... `remote_run_submit` has no way to select a specific composite — it dispatches whatever the pinned simulator's own build contains." A non-default `requested` composite falls through to `launch_into_study`'s own 409, not a remote dispatch.

**Consequence**: today, this path can only remote-dispatch `lineage_ray_batch` specifically if a study's own `baseline[0]` entry is configured to target that composite. There is no per-request `--composite-id`-style override the way `atlantis composite run`/`multi_node_dispatch.composite_id` provides.

### 1c. The compose (`/compose/v1`) client is real but stale relative to items 98/102

`sms_api_client.py:248-311` (`compose_submit`) wraps `POST /compose/v1/simulation/run` (multipart file-upload) — signature (`pbg_bytes, extra_pip_deps, interval_time, filename`) has **no `num_nodes`, `simulator_id`, or `compute_backend`**. Those three were added by items 98/102 (`ComposeSimulationRequest`, `ComposeDocumentSubmission`) — this client was never updated. No client method exists at all for `POST /compose/v1/simulation/run-document`.

`compose_check`/`compose_status`/`download_compose_results` exist and are current. `run_core.py:46`/`run_runner.py:806` reference this compose path for a DIFFERENT case (a single-cell composite exported as `.pbg` and uploaded) — separate from the Study-tab baseline-dispatch chain and from `lineage_ray_batch`'s multi-node needs.

Separately (established the same day, elsewhere this session): `/compose/v1/simulation/run-document` takes a raw, already-built process-bigraph `document`, not a `composite_id`+`params` pair the server resolves — v2ecoli PR #663 registered `LineageProcess` for the `ray:` protocol directly in `build_core()` specifically so a raw document with `ray:LineageProcess` addresses could resolve there, but nothing today actually builds and submits such a document for `lineage_ray_batch`. Every real dispatch this session used the `composite_id`+`params` shape (`extra_params.multi_node_dispatch`) via `/api/v1/simulations` instead — the same shape 1a's passthrough already carries. Building a raw document client-side (whether from a CLI or a browser) would require v2ecoli installed locally at the exact right commit; the `composite_id`+`params` shape avoids that by resolving server-side, in the correctly-pinned environment. This is a real reason to prefer extending the 1a path over adopting `run-document` as the UI's target — flagged as an open question below (§4.3), not decided here.

### 1d. What could NOT be confirmed to exist: a live client-side form for `multi_node_dispatch`-shaped input

A search for the client JS function a server-side comment names (`study-detail.js:_dispatchRemotePinned`) did not locate it under that name anywhere in `static/`. The comment may be stale, or the file/name has moved. No client-side JS was found building a request body containing `multi_node_dispatch`, `injected_processes`, `variants`, `num_nodes`, or similar keys anywhere. **The server-side plumbing (1a) is real and sufficient; the client-side UI to populate it with pbg-native-specific fields does not appear to exist yet.** This needs a live browser click-through to confirm before treating it as fully settled — source-reading alone can miss a dynamically-constructed request body.

## 2. The gap, itemized

| # | Gap | Where | Size |
|---|---|---|---|
| G1 | No `--composite-id`-equivalent selector on the remote-dispatch path | `remote_run_submit`/`study_runs.py` | Medium — real design decision needed |
| G2 | No client-side UI control to compose `n_seeds`/`n_generations`/`num_nodes`/`injected_processes`/etc. as `overrides.multi_node_dispatch` | `static/` (JS, not located) | Medium-large — a real form, named fields + raw-JSON escape hatch mirroring `atlantis composite run --params` |
| G3 | `SmsApiClient.compose_submit` missing `num_nodes`/`simulator_id`/`compute_backend`; no `run-document` client method | `sms_api_client.py` | Small — mechanical |
| G4 | Unconfirmed: does the Study tab's params UI allow a free-form key, or is it strictly schema-driven off the composite's own declared `parameters={}`? | UI, needs live verification | Investigation, not a build item — do first |
| G5 | Long-running-job UX — likely NOT a new gap (existing `/api/remote-run-poll` already handles long chain-dispatch campaigns per item 51) but confirm the same path is reused for MNP dispatches specifically | `remote_run_views.py` polling | Verification only |

## 3. Recommended minimal-viable design (not a decision — for review)

Reuse-first, per this project's own convention — do not build a new dispatch mechanism, the existing Study-tab chain already reaches viva-api correctly and generically.

1. **Resolve G4 first** (cheap) — determines whether G2 is "add schema-known fields" or "add a raw-JSON escape hatch."
2. **G1** — likely a `composite_id`/`extra_params` field the UI can set per-dispatch, mirroring `atlantis composite run`'s own `--composite-id`/`--params`, rather than requiring a study to be permanently "about" `lineage_ray_batch`. Real product decision needed — flagged, not resolved.
3. **G2** — mirror `atlantis composite run`'s already-proven parameter surface exactly: named fields for the common params, raw-JSON box for `injected_processes`/`variants`/`config_overrides`/`emitter_arg`. Same design item 98/PR #382 already validated for the CLI.
4. **G3** — mechanical, low risk, independent, ship any time.
5. Reuse existing Runs-tab/`/api/remote-run-poll` machinery rather than building new status UI.

## 4. Open questions needing a real decision before building

1. Does `lineage_ray_batch` dispatch belong on the Study tab's existing baseline-run mechanism, or as its own dedicated "Composite Run" panel decoupled from the Study/`spec.yaml` model?
2. Is `_run_auth_ok()` sufficient for MNP dispatch's real-dollar cost, or does it want a stricter gate?
3. Should `/compose/v1/run-document` (unblocked server-side by v2ecoli PR #663's `build_core` registration) be the UI's eventual target instead of `multi_node_dispatch`? Leaning no, per §1c's reasoning (document construction would have to move client-side, which is impractical in a browser) — but not resolved here, since it wasn't the shape this report was scoped to evaluate as the primary path.

## 5. What this report does NOT cover

No code written or modified. No live UI click-through performed — source-reading only; a real browser session exercising the Study tab's params UI would close G4/§1d cheaply before any build work starts. Does not re-litigate `lineage_ray_batch` vs. `ecoli_baseline` composite choice for a given CD2 run.
