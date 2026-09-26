- **2026-09-26** — **Checkpoint F passed on dev (0.9.159): the P4a-1 owner-ref expand, the first
  migration since 0.9.149, applied by the reconciler Job before the app rolled.** Two hours after
  F3 (0.9.158, code only). `main` at #790 (`99995eca`) carried the one migration of P4 so far,
  `a4b6c8d0e2f4` (revises `f4c8a2e6d0b3`): `hpcrun.owner_kind / owner_id / output_uri` and
  `dataset.owner_kind / owner_id / producer_job_id / trace_id`, `IF NOT EXISTS`, indexed,
  backfilled from the foreign keys with the writer's own rule (`viva_api/simulation/owner_ref.py`),
  dual-written from now on; the foreign keys stay authoritative until P7. Also on the image, code
  only: P4a-2 slices 1, 2 and 4 (#820–#822, `viva_core/datasets`, `viva_core/events`). Version
  bump #827 (tag `v0.9.159`), both stanford-test overlays together — the `-db-migration` overlay
  runs the reconciler from this tag.

  **The database step, as it ran** (`KUBECONFIG` stanford-test; the plan's order: Job first, then
  the app). Read-only first: `db_reconcile --analyze` inside the 0.9.158 pod said MANAGED at
  `f4c8a2e6d0b3` — its own head, so "nothing pending" from that image's point of view. Then
  `kubectl delete job alembic-migrate` (the leftover was a 0.9.149 Job from checkpoint B2),
  `kubectl apply -k …-db-migration`, `kubectl wait --for=condition=complete` (met). The Job pod
  **`alembic-migrate-75sxk`** on `sms-api:0.9.159` printed its reasoning: `state: managed`,
  `head revision: a4b6c8d0e2f4`, `current revision: f4c8a2e6d0b3`, every older marker `[x]` and
  `[ ] hpcrun.owner_kind column exists`, then `Running upgrade f4c8a2e6d0b3 -> a4b6c8d0e2f4` and
  `✓ database reconciled and upgraded to head`. The 0.9.158 api pod served throughout — additive
  DDL, nullable columns — and the marker list is the new reconciler's (#790 added the entry).

  **The app step.** Applied from a worktree pinned to the #827 merge (`38a8740b`); rollout clean;
  the newest pod (by `creationTimestamp`) is **`api-796884d4fb-ch89v`** on `sms-api:0.9.159`; the
  previous pod drained and was gone within minutes; the workbench pod (`workbench-c8db54b46-6nc8l`,
  2026-09-16) was not recreated. Markers on the new pod: `/app/viva_api/simulation/owner_ref.py`,
  `/app/viva_core/datasets/walk.py`, `/app/viva_core/events/ingest.py` exist;
  `_marker_hpcrun_owner_kind` ×2 in `db_reconcile.py`; `owner_kind` ×3 in `tables_orm.py`. Its
  startup log: `DB_CREATE_ALL=false: not creating simulation tables; the schema belongs to the
  alembic-migrate Job` (twice, simulation and compose), no ERROR. `/health` through the tunnel:
  0.9.159, `db_revision = db_head = a4b6c8d0e2f4`, `db_at_head: true`; the new pod's own
  `--analyze` says MANAGED at head, `[x] hpcrun.owner_kind column exists`.

  **Smoke** (`atlantis smoke run --tier 1 --require-aws`): **13 passed, 1 failed, 3 skipped** — the
  same shape as F3. Tier 0: `version`, **`database` at head `a4b6c8d0e2f4`**, `routes` 103/103,
  `capabilities` (`chain-dispatch, chain-progress, container-jobs, viva-v1-surface`), `relay`,
  `core` (`compose, environments, workers`), `lists` (simulators 209, analyses 797), `events`.
  Tier 1: `task` 63 (278.4 s), `task-fail` 64, `task-repo` 65, `worker`
  (`env-worker-d67b0a7-smoke0c2-…`, through `/viva/v1/env-worker`), `compose` 29 (`1.61051 =
  1.1^5`, 517.9 s). Skipped: the opt-in `build`, `analysis`, `biomodels`. The one failure is
  `contract`, all 8 problems `simulation.config.<key>: removed` — the newest simulation on dev is
  still CD2 composite 1442 and the 0.9.156 baseline was recorded from a Nextflow run; **#823**,
  filed at F3, not a server change (the same 22 operations, the same 8 keys as F3).

  **A run that did not count, recorded so nobody reads it as a regression.** The first smoke
  attempt on 0.9.159 reported `contract` and `relay` as `ReadTimeout` after 1176 s and 2040 s.
  Every local request had gone to `000` while the pod answered `200` in its own log and the ALB
  target was `healthy`: the SSM tunnel's `session-manager-plugin` had hung, alive but deaf (the
  memory's ~70-minute failure, on a process started 2026-09-25T00:51). Killing the plugin let the
  restart loop re-establish it (`/version` in one try; the relay probe 422 in 0.1 s) and the
  second run is the one above. Lesson kept: a smoke `ReadTimeout` on a *read-only* Tier 0 check is
  the tunnel until proven otherwise; gate on `/version` returning JSON before trusting a failure.

  **What F proves and does not.** The reconciler path for a real migration on Stanford dev with
  `DB_CREATE_ALL=false`: Job first, marker-driven, one upgrade, the old image serving through it —
  the same sequence the P-jump runbook prescribes for prod, now exercised once more (the last was
  B2, `f4c8a2e6d0b3`). The backfill's correctness is asserted by #790's own tests, not by smoke;
  no smoke check reads `owner_kind` yet. Not exercised: a downgrade (proven in tests only), the
  P4a-2 code (#820–#822 has no caller on a route yet), a build. Prod is untouched at 0.9.78; the
  P-jump runbook's revision list gains nothing new — `a4b6c8d0e2f4` was already its row "F", and
  the Job log above is what its step 5 will look like.
