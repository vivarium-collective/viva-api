- **2026-09-26** — **Checkpoint F3 passed on dev (0.9.158): the U2 core-on-SLURM work, U3's overlay
  and the atlantis surface resolver deployed to Stanford, and nothing SMS-facing moved.** Dev ran
  0.9.157 (F2, 2026-09-25); `main` was nineteen PRs ahead, #798–#817, every one from the UConn
  track or a fix found by it. F3 is the proof that a Stanford site takes that batch unchanged:
  code only, no migration, both stanford-test overlay tags bumped in lockstep (#819, tag
  `v0.9.158`, release notes list every PR).

  **Deployed:** `build-and-push.yml` on `main` at `d0a201bc` (the #819 merge); applied from a
  worktree pinned to that commit; `kubectl rollout status` clean; the newest pod (by
  `creationTimestamp`) is **`api-7659b99b85-rtc55`** on `sms-api:0.9.158`. The version block's
  markers all hold on it: `/app/viva_core/compose/simulation_service_hpc.py`,
  `/app/viva_core/storage/factory.py`, `/app/viva_core/lifespan.py`, `/app/viva_core/backends/base.py`,
  `/app/viva_core/compose/build_k8s.py` and `/app/app/surface.py` exist; `self._url(` ×23 in
  `app_data_service.py`; `postgres_user` is not declared in `viva_api/config.py` (inherited from
  `CoreSettings`, #806). The apply also reported `deployment.apps/workbench configured`, but that
  was metadata only — the workbench pod (`workbench-c8db54b46-6nc8l`, 2026-09-16) was not
  recreated; its ReplicaSet generation stayed at 52. `/health` through the tunnel: 0.9.158,
  `db_revision = db_head = f4c8a2e6d0b3`.

  **Smoke** (`atlantis smoke run --tier 1 --require-aws`, Tier 0 + Tier 1 through the SSM tunnel):
  **13 passed, 1 failed, 3 skipped.** Tier 0: `version`, `database` (at head), `routes` (103 of
  103 spec operations served), `capabilities` (`chain-dispatch, chain-progress, container-jobs,
  viva-v1-surface`), `relay` (JSON 404), `core` (services `compose, environments, workers` — the
  #806 shape), `lists` (simulators 209, analyses 797, parca datasets 267, compose simulators 6),
  `events`. Tier 1: `task` 60 (257.8 s, cold fleet), `task-fail` 61, `task-repo` 62, `worker`
  (`env-worker-d67b0a7-smoke06b-…`: generators read, a task completed, stopped — this is the
  first checkpoint where atlantis reached the env-worker surface **through `/viva/v1/env-worker`**,
  the #811 resolver following `viva-v1-surface`), `compose` 28 (`level 1.61051 = 1.1^5`, 517.8 s in
  the science image). Skipped: `build`, `analysis`, `biomodels` — opt-in by design.

  **The one failure is the check, not the server.** `contract` compared all 22 recorded
  operations and reported 8 changes, every one `simulation.config.<key>: removed`
  (`out_dir, cache_dir, time_step, description, lineage_seed, nextflow_dispatch,
  max_duration_per_gen, different_seeds_per_variant`). `resolve_context` samples the **newest**
  simulation; the baseline (dev 0.9.156, 2026-09-25) was recorded from a Nextflow-dispatch run,
  and the newest simulation on dev today is a CD2 campaign composite (1442,
  `sim201-cd2-run4-armD-…`, `multi_node_dispatch` + `HYPERQUEUE` + `carina`). `config` is the
  submitter's passthrough JSON, so the eight "removed" keys are exactly the baseline's config
  keys that this submission does not carry; no server code that shapes `GET
  /api/v1/simulations/{id}` changed between 0.9.157 (where `contract` passed) and 0.9.158.
  Filed as **#823**: treat `config` (and the other passthrough objects) as opaque in the recorded
  shape and re-record. Until that lands, a `contract` diff confined to `simulation.config.*` is
  the sampling. Not fixed here: F3 is a checkpoint, and the check's contract is its own PR.

  **What F3 proves and does not.** It proves the Stanford Batch site is indifferent to the
  UConn track's moves (settings inheritance, the SLURM compose service in core, the storage
  factory, the lifespan, the JobBackend Protocol, the K8s build option defaulting to `sbatch`) and
  that the capability-addressed client works against a site advertising `viva-v1-surface`. It does
  not exercise SLURM (UB/UC do), the `viva-core` image (#804 builds it; this image is unchanged),
  or a build (`build` skipped). Prod is untouched at 0.9.78; the P-jump runbook draft is its own
  entry (`2026-09-26-b-pjump-runbook.md`).
