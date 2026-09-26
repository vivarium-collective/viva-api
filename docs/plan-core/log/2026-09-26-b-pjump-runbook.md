- **2026-09-26** — **P-jump runbook, draft (Stanford prod 0.9.78 → 0.9.158+): read-only findings, the
  revision list, the ALB gap, and the order of operations.** Prepared right after checkpoint F3
  (dev on 0.9.158); nothing was applied to prod. Written so that the day it runs, the operator has
  the facts and the gates and only fills in the version and the outcomes. The plan (§4, §8) puts the
  P-jump right after checkpoint F, so the version this runbook deploys is **F's tag, not F3's**: the
  steps hold, the revision list gains F's expand migration at the bottom.

  **What prod is today (read-only, 2026-09-26, `KUBECONFIG=~/.kube/kubeconfig_stanford.yaml`,
  `AWS_PROFILE=stanford-sso`):**
  - Namespace `sms-api-stanford`: `api` = `sms-api:0.9.78` (pod `api-86bcfcb6f4-kx4hw`, running
    since 2026-08-31T23:26Z, 26 days), `workbench` = `vivarium-workbench:0.3.78`, `ptools` =
    `sms-ptools:0.9.53`, `redis`, `haproxy-ssh`. The checked-in overlays agree: app and
    `-db-migration` both `0.9.78`, the workbench overlay `0.3.78` (prod splits the workbench into its
    own overlay so an api-only apply cannot `Recreate` it).
  - `db_reconcile --analyze`, run read-only inside the live pod: **state `managed`, current
    revision `b4d7e9c02a15`**, all eleven of the 0.9.78 image's schema markers `[x]`. That image's
    own head *is* `b4d7e9c02a15`, so the report says "nothing pending" — against the 0.9.158 chain
    there are **nine** pending revisions (below).
  - The live ConfigMaps carry **no `DB_CREATE_ALL`** — the 0.9.78 pod still bootstraps with
    `create_all` at startup (the default). The checked-in `kustomize/config/sms-api-stanford/api.env`
    sets `DB_CREATE_ALL=false`, so the first apply of a current overlay switches the net off:
    from then on only the migration Job makes tables. **Hence the order below is not optional:
    the Job runs before the app rolls.** (A new app started before the Job would not crash — it logs
    an ERROR naming the remedy and reports `db_at_head=false` in `/health` — but every route that
    touches a table only a pending revision creates would 500.)
  - The live prod ConfigMaps carry no `CORE_RUNTIME_IMAGE` and neither does the checked-in prod
    config; dev names `viva-core-runtime:0.1.0`. Prod keeps running tasks and compose in the science
    image until it names one — the plan's P2.3 row says to repeat the one-job trial pull from prod's
    VPC first. Not part of the jump.
  - `ENV_WORKER_MODULE_IMAGE` = workbench `0.3.78` on prod, equal to its workbench tag, as required.
    Dev is on `0.3.85`. Whether the workbench moves in the same window is Jim's call (own overlay,
    own `Recreate`, own outage); the coupling rule holds either way: the two tags move together.

  **The sms-cdk `/viva` gap, confirmed on the listeners (read-only `aws elbv2 describe-rules`):**
  dev's internal ALB (`smsvpc-Inter-m3xXOriqxk2u`, port 80) routes `/api /bigraph-loom /compose
  /core /docs /env-worker /health /home /openapi.json /version /viva /workbench /ws` to the api
  target group; **prod's (`smscdk-Inter-5cwVdmerxIVL`) has every one of those except `/viva` and
  `/viva/*`.** The rule is on sms-cdk `main` (`lib/internal-alb-stack.ts`, commit `972e256`, after
  `c95d737` which added `/compose` and `/env-worker`); it was `cdk deploy`ed to `smsvpctest` and
  never to `smscdk`. Without it a prod `GET /viva/v1/health` through the ALB falls through to
  PTools and answers HTML — the failure `atlantis smoke`'s `core` check exists to catch. The
  prod deploy is `DEPLOY_ENV=stanford … npx cdk deploy` of the internal-ALB stack; run `cdk diff`
  first and read it whole, because the same `cdk deploy` carries whatever else drifted since the
  last prod deploy (the 2026-09-09 lesson: a launch-template change replaces the four task
  compute environments and kills running task-queue jobs — pick an idle window).

  **Every revision prod's database will take** (the chain as of 0.9.158; `alembic history`, oldest
  first; the tag is the first release carrying the file):

  | # | revision | first in | what it does | reversible |
  |---|---|---|---|---|
  | — | `b4d7e9c02a15` | 0.9.79 line (prod's 0.9.78) | **prod is here** — `env_worker_task` | — |
  | 1 | `c7d1f3a9b2e4` | v0.9.111 | `hpcrun.external_job_ids` (#414's fix shape) | yes |
  | 2 | `f76e43d01841` | v0.9.111 | `compose_simulation.analysis_options` | yes |
  | 3 | `d7e2f4a6c8b0` | v0.9.147 | the `task` table (in-region task-run verb, #631) | yes |
  | 4 | `a3b5c7d9e1f2` | v0.9.139 | observability columns + the `hpcrun_event` / `hpcrun_span` tables | yes |
  | 5 | `e3a9c1d70b62` | v0.9.147 | `hpcrun_event.layer` → `component` | yes |
  | 6 | `b2f6d8e0a4c7` | v0.9.147 | enum `jobtypedb` + `'ANALYSIS'` | **no** (enum label; benign) |
  | 7 | `c9a1e3f5b7d2` | v0.9.147 | the `dataset` table, `analysis.source/tags`, `hpcrun.jobref_analysis_id` | yes |
  | 8 | `e7b3c9a1d5f2` | v0.9.149 | `env_worker_task.owner_instance` + index | yes, round-trip proven |
  | 9 | `f4c8a2e6d0b3` | v0.9.149 | `simulator.temporary / label / image_tag` (D11) | yes, round-trip proven |
  | F | *(P4a-1 owner-ref expand, #790)* | 0.9.159 | owner-ref columns on `hpcrun` / `dataset`, backfilled, dual-written | yes — old columns stay authoritative |

  Two revisions on the chain sit **behind** prod's stamp and will therefore **not run** on prod:
  `b9e1d5a3c7f2` (creates the nine tables only `create_all` ever created: the eight `compose_*`
  tables and `analysis`) and `c3f7a1e5b9d4` (reshapes three baseline tables). That is by design
  (#637: both are no-ops on a `create_all` database) and it is fine for prod **because those nine
  tables already exist there** — `--analyze` finds `analysis.n_tp` and `compose_hpcrun.job_id_ext`,
  and prod's compose tables were created on boot at the 0.9.20 deploy. What `upgrade head` cannot
  see is a column that `create_all` never added to a table that existed before it; that is the
  job of the read-only `scripts/db_schema_diff.py` pre-flight, which must say *no blocking drift*.

  **The gated sequence** (each step read-only until step 5; stop at the first surprise):

  1. **Pick the window and the version.** F's tag (0.9.159 if F lands as planned). Confirm the
     prod overlays on `main` carry it: `kustomize/overlays/sms-api-stanford` (`sms-api` only —
     ptools stays `0.9.53`), `sms-api-stanford-db-migration` **equal to it**, and
     `config/sms-api-stanford/shared.env`'s `ENV_WORKER_MODULE_IMAGE` equal to the workbench
     overlay's tag. Check prod for pod-local in-flight work before restarting anything (`hpcrun`
     rows on the `local` backend, relay workers, unsettled `env_worker_task`): a restart drops those;
     Batch jobs survive and polling resumes with the new pod.
  2. **`secrets.sh`** in the prod overlay refreshes the sealed secrets and ARNs from the `smscdk-*`
     stack outputs; diff what it regenerated; commit if anything moved. Verify the refreshed
     `secret-shared` connects before relying on it (the 2026-07-14 lesson: the live ghcr PAT was
     expired, and the refreshed sealed secret fixed it).
  3. **RDS snapshot** of prod's instance, named for the jump (`pre-0-9-159-pjump-<date>`), and
     wait for it to be `available`. It is the rollback for the enum revision and for everything the
     plan marks one-way later; nothing in this jump is one-way except the enum label.
  4. **Read-only pre-flight from a one-off pod of the NEW image** (not the old pod: its module set
     is 0.9.78's) — `kubectl run` with the prod `api` pod's env and service account, or
     `kubectl exec` once the new tag exists in the namespace as a Job pod:
     `python -m viva_api.simulation.db_reconcile --analyze` must say `managed` at `b4d7e9c02a15`
     with head `f4c8a2e6d0b3` (or F's revision) and list the pending nine (ten); then
     `python scripts/db_schema_diff.py` must say *no blocking drift*. Record both outputs in the
     log entry.
  5. **The migration Job, first:** `kubectl delete job alembic-migrate -n sms-api-stanford
     --ignore-not-found` → `kubectl apply -k kustomize/overlays/sms-api-stanford-db-migration` →
     `kubectl wait --for=condition=complete job/alembic-migrate -n sms-api-stanford --timeout=600s`
     → read the Job's log whole: the reconciler prints its reasoning, one line per marker, then
     the upgrade. The 0.9.78 api pod keeps serving throughout: every revision is additive DDL
     (new tables, nullable columns with defaults, an enum label) and none rewrites a row the old
     image reads.
  6. **Roll the app:** `kubectl kustomize kustomize/overlays/sms-api-stanford | kubectl apply -f -`
     → `kubectl rollout status deployment/api -n sms-api-stanford` → marker grep on the **newest**
     pod (by `creationTimestamp`, never `items[0]`) for the version block's markers → the tunnel
     (`sms-proxy.sh -s smscdk`) → `/version` and `/health` (`db_at_head` true, `db_revision` = head).
  7. **`cdk deploy` the `/viva` rule** (step above): `cdk diff` first, read it whole, deploy in the
     same idle window. Then `GET /viva/v1/health` through the tunnel is JSON.
  8. **Smoke, in this order:** `atlantis smoke run --tier 0` (with `database`, `contract`, `core`,
     `capabilities` — note that `contract` compares against the newest simulation's own passthrough
     `config`, #823, so a diff confined to `simulation.config.*` is the sampling, not the server);
     Tier 1 with `--require-aws` (`task`, `task-fail`, `task-repo`, `worker`, `compose` — prod's
     compose runs in the science image, budget ~8 min for the pull); Tier 2 **with the cancel
     checks** (`sim-cancel`, `chain-cancel`, `nextflow-cancel` verify on Batch itself and stop at
     startup without AWS access — that is what `--require-aws` is for). Tier 2 is tens of minutes
     and dollars; it is the jump's proof that every dispatch path still runs on prod's queues.
  9. **The workbench** (if Jim includes it): `kubectl apply -k kustomize/overlays/sms-api-stanford-workbench`,
     a `Recreate` = a full workbench outage; `/workspace` and `/root/.pbg` are on the PVC (checked by
     device number, 2026-08-31) so the registry survives. `ENV_WORKER_MODULE_IMAGE` moves with it.
  10. **Record:** tag nothing (the tag is F's); a log entry `<date>-b-pjump.md` with the analyze
      and schema-diff outputs, the Job log's upgrade lines, the pod name, the smoke counts per tier,
      and the `cdk diff`; then the docs PR folds the ledger row (`P-jump` → done) and the
      `deploy_stanford_prod` memory's live line (api / workbench / DB revision).

  **Rollback**, per step: before 5, nothing to undo. After 5 and before 6, the old image runs
  unchanged against the migrated schema (additive), so rolling forward is the only sensible move;
  `alembic downgrade` to `b4d7e9c02a15` is real for 8 of the 9 (the enum label stays, harmless).
  After 6, `kubectl rollout undo deployment/api` returns to 0.9.78 against the new schema, which it
  tolerates. The snapshot is for the case nobody expects.

  **Deliberately not in this draft:** no `cdk diff` was run (it is read-only but it is prod's
  stack and the parent lane did not ask for it); no one-off pod was started on prod (step 4 is the
  first thing the live runbook does); the workbench decision and the `CORE_RUNTIME_IMAGE` trial
  are Jim's, listed above and not decided here.
