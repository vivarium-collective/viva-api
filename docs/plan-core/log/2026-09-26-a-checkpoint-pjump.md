- **2026-09-26** — **The P-jump: Stanford prod `sms-api-stanford` 0.9.78 → 0.9.159 (checkpoint F's image), workbench 0.3.78 → 0.3.85, and the `/viva` ALB rule.**
  Under Jim's go (a: steps 1–6 + smoke T0/T1; b: Tier 2 later; c: the workbench too; d: the
  orphaned `local` RUNNING rows cleaned). In order, each verified before the next:
  (1) #839 merged (`d57ed8fc`): app + db-migration overlays 0.9.159, workbench overlay and
  `ENV_WORKER_MODULE_IMAGE` 0.3.85 (equal to dev's live tag); applied from a worktree pinned there.
  (2) `secrets.sh` **not run**: the live `shared-secrets` user/password/host/port hash-match Secrets
  Manager, so nothing had moved. (3) RDS snapshot **`pre-0-9-159-pjump-2026-09-26`** available
  (19:19Z). (4) The migration Job (0.9.159): MANAGED at `b4d7e9c02a15`, ten `[ ]` markers, ten
  upgrades to `a4b6c8d0e2f4`, `✓ reconciled`; `alembic_version = a4b6c8d0e2f4`; the 0.9.78 pod served
  throughout with zero errors. (5) App rollout: pod `api-54bf449874-54wzj` on 0.9.159, the four markers
  present, `DB_CREATE_ALL=false` honoured for the first time on prod (it was absent), startup check
  `✓ database schema is at the Alembic head`; `/health` `db_at_head: true`; ptools rolled on the
  ConfigMap hash, still 0.9.53. (6) `cdk deploy smscdk-internal-alb --exclusively` from sms-cdk `main`
  (`bf071be`): the diff was exactly `[+] VivaCoreRouteRule` (priority 86 → api); `smscdk-eks`'s
  unrelated ebs-csi addon drift stayed out; `/viva/v1/health` and `/viva/v1/capabilities` answer JSON
  through the front door. (7) Workbench: `Recreate` rollout to 0.3.85; the registry on the PVC is the
  same 170 bytes after; `/workbench` 200.
  (8) Smoke `--tier 1 --require-aws` through a prod tunnel: **12 passed / 2 failed / 3 skipped — both failures expected: `routes` (the client at `main` knows `/viva/v1/datasets` from #829, which is not in 0.9.159 and deploys at G) and `contract` (#823: `simulation.config.*` / dataset-attribute shapes that differ with prod's sample data). Everything that dispatches passed on prod: `task` / `task-fail` / `task-repo`
  (Batch, `188da91`), `worker` (an env worker through `/viva/v1/env-worker`), `compose` (19: `1.1^5`);
  `database` at head, `core`, `relay`, `capabilities`. Tier 2 is later (Jim).
  **(d) needed no SQL:** 0.9.159's orphan reconciler (the #414 fix) closed all 23 orphaned `local`
  rows at startup (ids 218–278, "finished from external state: completed"). The 20 stale `ray` RUNNING
  simulation rows from 2026-07-15 → 08-17 (ids 149, 150, 153, 155, 159, 184, 185, 189–191, 193, 198,
  199, 203, 207–211, 242) were then closed by hand (Jim): FAILED, with an `error_message` saying the
  real outcome is unknown (Batch history expired) and any outputs remain in S3. Prod now has no
  non-terminal `hpcrun` row. Prod now serves `/viva/v1` for the workbench's Class-A operations (the prod flag owed on
  vivarium-workbench#1150). Rollback points: the snapshot; `rollout undo` (0.9.78 tolerates the schema).
