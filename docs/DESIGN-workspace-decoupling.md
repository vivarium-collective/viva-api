# Design — Decoupling the workspace from the base image (multi-workspace staging)

**Status:** POST-DEMO. Not on the 2026-07-22 demo critical path; the demo has a
local backup and is served by `docs/PRE-DEMO-MASTER-PLAN.md`.
**Target environment:** `sms-api-stanford-test` / `smsvpctest` (dev). **Not prod**
until proven there.
**Origin:** Jim, after discussion with Eran and Alex.
**Relationship to prior plans:** this is the infra half of Alex's
`PRE-DEMO-PLAN.md` §5 "Phase 6 — workspace creation/hosting", which that plan
explicitly deferred. It is the enabler for the grand design in its §0: *spin up
sms-proxy and create new workspaces or switch between them, remotely.*

---

## 1. Problem

The deployed workbench is welded to one workspace. Verified on both namespaces:

- `kustomize/base/workbench/workbench.yaml` runs `replicas: 1` against one RWO
  EBS PVC, seeded **once** by a conditional `seed-workspace` init container that
  copies a baked-in v2ecoli from the image.
- `VIVARIUM_WORKBENCH_REMOTE_REPO_URL` is hardcoded to v2ecoli (`:157-158`).
- The workbench already *has* a workspace-switch concept, but it targets local
  filesystem paths (`../my-other-v2ecoli-fork/workspace`) — meaningless in-cluster.

So in AWS there is exactly one workspace, forever, and no way to work against a
fork, a branch, or a second model.

## 2. Goal

Make the PV a **multi-workspace staging area**. Switching workspaces in the hosted
workbench re-points at a child directory of a staging root, seeded on first use.
Edits accumulate durably in AWS and can later be diffed and pushed back to their
origin repos.

### Non-goals (explicitly out)
- Multi-*user* / multi-replica hosting. Stays `replicas: 1`, RWO. (§8)
- Per-workspace pods, EFS/RWX, a workspace registry service, `/workbench/<id>` ALB
  routing. All deferred; this design must not foreclose them.
- Changing how compute is dispatched (that's the compose-on-Batch work).

## 3. Verified current state (measured, not assumed)

Both `sms-api-stanford` and `sms-api-stanford-test`:

| Fact | Value |
|---|---|
| PVC | `workbench-workspace`, **20 Gi**, **RWO**, `gp3-retain` |
| Used | **8.5 G by the single v2ecoli workspace** (~12 G free) |
| Is it a git checkout? | **Yes** — `.git` present, remote `v2ecoli.git`, at `a08e20bd` |
| Runs DB (prod) | `/workspace/.pbg/composite-runs.db` — **still SQLite**; jsonl is newer, undeployed |
| Replicas | 1 |

Two consequences that drive the design:

1. **Capacity is the binding constraint.** At 8.5 G per checkout on a 20 G volume,
   the current PV holds **two workspaces, maybe three**. Any design that copies a
   full repo per workspace hits this wall almost immediately.
2. **`git` in the pod is currently broken for our purposes.** `git -C /workspace …`
   fails with `fatal: detected dubious ownership in repository at '/workspace'`.
   Diff/push-back is impossible until `safe.directory` is configured. Cheap to fix,
   but it is a hard prerequisite for the stated end goal.

## 4. Design

### 4.1 Layout

```
/workbench-root/                     <- the PVC mount (was /workspace)
  ├── vivarium-collective__v2ecoli/  <- staged workspace (a real git checkout)
  │     ├── .git/
  │     └── .pbg/                    <- runs DB + schemas travel WITH the workspace
  ├── myfork__v2ecoli/
  └── .registry.json                 <- staged-workspace index (key -> metadata)
```

The active workspace is a child of `/workbench-root`, not the mount root itself.

### 4.2 Workspace key

The child directory name must disambiguate forks with identical repo names.
**Recommendation:** `<org>__<repo>` (filesystem-safe, human-readable), with branch
and commit recorded as *metadata* in `.registry.json`, not in the path.

Rationale: keying by branch would multiply 8.5 G checkouts per branch, which the
capacity budget cannot absorb. One checkout per repo, switch branches *within* it
via git — that's what a checkout is for. Revisit only if concurrent-branch work
becomes a real need.

### 4.3 Seeding a workspace

Two distinct paths — the description "copy from the docker container if empty"
only covers the first:

- **Baked workspace (v2ecoli):** copy from the image, exactly as the existing
  `seed-workspace` init container does. Preserves `.git` (verified present today).
- **Any other repo:** must be `git clone`d from the pod. This needs network egress
  **and credentials for private repos** (e.g. `CovertLabEcoli/vEcoli-private`).
  The workbench already has a GitHub device-flow login (`github-login.js`) — reuse
  that token rather than introducing a deploy key.

**Recommendation for v1:** support both, but ship the baked path first and treat
clone-from-origin as the second increment. Clone is where the credential and
egress complexity lives.

### 4.4 Runs database

The runs DB is workspace-scoped and lives under the workspace's `.pbg/`, so it
travels with the switch for free — no separate migration step. Note prod is still
SQLite (`composite-runs.db`); the jsonl format is newer and undeployed, so this
design should not assume jsonl is already in place.

### 4.5 Switch flow

1. Resolve the target workspace key.
2. If `/workbench-root/<key>` is absent or empty → seed (§4.3).
3. Re-point the workbench's active workspace at that directory.
4. Re-open the workspace-scoped runs DB.
5. Reconcile against sms-api (§4.6).

**⚠️ Step 3 rests on a known-broken mechanism.** `/api/source/switch` (SP2) is a
documented *half-switch*: it re-points `WORKSPACE` and caches but leaves CWD,
`sys.path`, and `sys.modules` stale (`ARCHITECTURE-DEEP-DIVE.md:223,272`). Today
that's a nuisance; under this design switching becomes the *primary* interaction,
so **fixing the half-switch is in scope here**, not adjacent to it. This is the
single largest piece of unestimated work in the design.

### 4.6 Reconciliation with sms-api

On switch, query sms-api so the local runs DB reflects reality.

**Open — needs a decision (§6).** `/api/v1/simulations` is the *ensemble* endpoint;
composite runs are dispatched through `/compose/v1/*` and are not represented there.
A composite-run reconcile likely needs the compose endpoints, or both.

Semantics to pin down: the join key (`experiment_id`?), whether it's one-way
(sms-api → local) or a merge, and what happens to purely-local runs that were never
submitted remotely (they must not be deleted as "not found upstream").

### 4.7 Push-back

Because staged workspaces are real checkouts, diff and push are native git — no
custom sync layer. Prerequisites:
- `git config --global --add safe.directory '*'` (or per-workspace) in the pod
  image — **currently blocking, see §3**.
- A commit identity. **Recommendation:** the user's GitHub identity via the existing
  device-flow login, so staged edits are attributable to a person rather than to a
  shared service account.

## 5. Capacity plan

The constraint that most shapes v1. Options, cheapest first:

1. **Grow the PVC.** `gp3` supports online expansion; `gp3-retain` means data
   survives. 20 Gi → 100 Gi is a one-line change and buys ~11 workspaces.
   **Recommended for the dev site immediately** — it's the cheapest way to stop
   capacity from distorting the design.
2. **Shallow clones** (`--depth=1`) for non-baked workspaces. Big win: much of
   v2ecoli's 8.5 G is history and binary assets (PDFs, `models/`).
3. **LRU eviction** of staged workspaces with a floor on the active one. Only worth
   building if (1) and (2) prove insufficient.

Ship (1) + (2); defer (3).

## 6. Open decisions

| # | Decision | Recommendation |
|---|---|---|
| D1 | Workspace key: `org__repo`, or include branch? | `org__repo`; branch in metadata (§4.2) |
| D2 | Clone-from-origin in v1, or baked-only? | Baked first, clone second |
| D3 | Which sms-api endpoint(s) reconcile against? | Needs Alex/Eran — compose endpoints likely required |
| D4 | Reconcile = one-way or merge? Local-only runs? | Merge; never delete local-only |
| D5 | Commit identity for push-back | User's device-flow GitHub identity |
| D6 | Grow the PVC now? | Yes — 100 Gi on the dev site |
| D7 | Is fixing the SP2 half-switch in scope? | Yes — it's load-bearing (§4.5) |

## 7. Rollout — `sms-api-stanford-test` only

1. Grow the dev PVC (D6); confirm `gp3` online expansion.
2. Add `safe.directory` to the workbench image (unblocks §4.7).
3. Change the mount to `/workbench-root`; rework `seed-workspace` to seed
   `<root>/<key>` instead of the mount root. **Migration:** the existing 8.5 G
   `/workspace` must be *moved* into `/workbench-root/vivarium-collective__v2ecoli`,
   not re-copied — there isn't room to duplicate it (§3).
4. Land the switch + registry + reconcile.
5. Soak on dev. **Do not promote to `sms-api-stanford` until the demo milestone is
   safely past and the dev site has run real work.**

## 8. Risks

- **Data loss during the mount migration (step 3).** Moving a live 8.5 G workspace
  with insufficient room to copy is the single riskiest operation. `gp3-retain`
  helps; take an EBS snapshot first regardless.
- **The SP2 half-switch is load-bearing and unestimated** (§4.5). If it proves deep,
  it — not the storage layout — is the schedule driver.
- **Single replica is hardened, not relaxed.** RWO + one pod remains an assumption.
  Multi-user later means EFS/RWX and revisiting this layout; the `.registry.json`
  indirection is deliberately the seam where that swap would happen.
- **Credential surface grows** if clone-from-origin lands: a GitHub token in the
  pod that can read private repos.
- **Prod drift.** Dev and prod currently have identical workbench storage shape.
  This design intentionally diverges them for a while; keep the delta written down.
