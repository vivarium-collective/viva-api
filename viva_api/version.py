# NEWEST(FIRST NEXTFLOW): "0.2.11-dev"
# STABLE: "0.2.10-dev"
# PREVIOUS STABLE: "0.2.8"
# LATEST STABLE (old): "0.2.74-dev"
# LATEST STABLE (most recent before hackathon 1): "0.4.8" -> 0.5.9
# LATEST STABLE AFTER HACKATHON FOR AWS: 0.6.0
# 0.6.2 — task-8 parallel S3 downloads + results-cache emptyDir volume
# 0.6.3 — remove PCS/SLURM/FSx from stanford-test, backend guards on legacy endpoints
# 0.7.0 — Atlantis CLI, AWS Batch backend, PCS/SLURM/FSx removal, ComputeBackend enum
# 0.7.1 — fix public_mode default, stale secret ARN, enforce run_parca on Batch
# 0.7.2 — config template fallback for public vEcoli repo, kustomize tag sync, RKE DB migration
# 0.7.3 — generation range and seed filtering for ptools analysis endpoint
# 0.7.4 — top-level DuckDB filters, strip private analyses from embedded template,
#          repo-aware analysis_options defaults (cd1_* only for private vEcoli repo)
# 0.7.5 — S3 streaming download (fix 504), README image, CLI trailing help, docs update
# 0.7.6 — TUI/GUI feature parity, reactive simulator selection, repo dropdown, list sorting
# 0.7.7 — fix analysis filters (generation_range/lineage_seed inside analysis_options),
#          allow arbitrary public vEcoli branches, bump pytest for CVE fix
# 0.7.8 — explicit config validation (no silent fallback), diagnose_sim.py diagnostic tool
# 0.7.9 — ecoli-sources support (--sources flag), remove vecoli dep, dep bumps
# 0.8.0 — harden ecoli-sources sync (org allowlist, path traversal, size limits, manifest validation)
# 0.8.1 — GUI auto-refresh, remove branch allowlist, mount GUI notebook, improve error messages
# 0.8.2 — fix analysis output metadata (partition parsing), all-domain filtering, restore num_seeds
# 0.9.0 — compose (process-bigraph) subsystem, Python 3.13, /compose/v1/ endpoints
# 0.9.1 — fix test_run_analysis to use HPC-available simulator (203ab2a), graceful GitHub cred skip
# 0.9.2 — BioModels integration release
# 0.9.3 — accept v2ecoli repo (RepoUrl allow-list), config-template fallback for Ray/v2ecoli
# 0.9.4 — route simulator build by repo at the upload endpoint (v2ecoli builds on Ray, not the default)
# 0.9.5 — Ray MNP submit: single "0:" node override to match the CDK job def; mask PAT in build logs
# 0.9.6 — Ray parca: hydrate out/cache via build_cache.py so the sim finds initial_state.json
# 0.9.7 — simulation log endpoint: RAY branch (surface summary.json) instead of 500-ing on SLURM SSH
# 0.9.8 — Ray _sim_command: optional two-engine comparison driver (composite/condition/max_generations)
# 0.9.9 — expose composite/condition/max_generations on the run endpoint (comparison submit)
# 0.9.10 — Batch/Nextflow: clear leaked sim_data_path default when run_parca=True
# 0.9.11 — Batch/Nextflow: set sim_data_path=None (not pop) so config.template default is overridden
# 0.9.12 — export-simulator-workspace endpoint (stream a build's repo@commit tarball);
#          reconciles version.py with the ad-hoc 0.9.8-0.9.11 deploy tags
# 0.9.15 — same export endpoint + observables read-path fixes; bumped past the
#          ad-hoc 0.9.13/0.9.14 deploy tags so the next tagged release is the
#          unambiguous high-water mark (supersedes 0.9.12)
# 0.9.16 — Ray: --composite vecoli stages a SEPARATE pristine-upstream ParCa cache
#          (build_upstream_parca.py, serial --cpus 1) instead of the v2ecoli cache
# 0.9.17 — data-layout module centralizes all S3 store/cache paths and closes the
#          reader-vs-downloader drift (#153/#152); comparison knobs validated at the
#          API boundary via Literal query params (#154); recognize
#          CovertLabEcoli/sms-ecoli as a Ray repo + harden repo->backend dispatch
#          to the explicit RepoUrl map (#164); observables endpoint returns 409 for
#          non-Ray runs; vivarium-workbench deploy manifests move into kustomize (#165)
# 0.9.18 — simulation search filter: GET /api/v1/simulations gains optional
#          experiment_id (comma-separated) + tag (predefined bundle, e.g. cd1) query
#          params (union, backwards-compatible), a GET /simulations/tags discovery
#          endpoint, and the atlantis CLI --tag/--experiment-id + `simulation tags` (#163)
# 0.9.19 — self-diagnosing DB reconciliation (viva_api/simulation/db_reconcile.py +
#          scripts/db_analyze.py|db_reconcile.py): adopts legacy create_all-bootstrapped
#          databases into Alembic (stamp matched rev -> upgrade head), upgrades managed
#          ones, builds fresh ones from base, and refuses loudly on an inconsistent
#          schema. The stanford-test alembic-migrate Job now runs the reconciler instead
#          of bare `alembic upgrade head`, so migrations are safe on customer-controlled
#          databases. Reconciling stanford-test also applies the missing 'cancelled'
#          jobstatusdb enum value (a1c3e5f7b9d2, never stamped there under create_all).
# 0.9.20 — tags as data: simulation gains a free-form `tags` JSONB column
#          (migration c1a2b3d4e5f6, GIN-indexed), replacing the hard-coded tag
#          registry. GET /simulations/tags now reflects DB contents; GET
#          /simulations?tag= filters via JSONB containment (unknown tag -> empty
#          200, not 400); POST /simulations/{id}/tags retro-tags; run accepts
#          tags. Atlantis CLI: `simulation run --tag`, `simulation tag <id>`.
#          Tags are site-local data (per-site RDS), fixing the shared-S3 /
#          independent-DB mismatch. Reconciler fingerprint extended for the new
#          revision (frozen once create_all is guarded off in prod).
# 0.9.21 — analysis-result endpoints (read side): generalize the `analysis` table
#          (migration d3f9a1c72b84 adds nullable indexed experiment_id/n_tp/status/
#          result_uri/... ; config JSONB stays authoritative). New GET /analyses
#          (exhaustive list across sims, optional experiment_id/simulation_id
#          filters), GET /simulations/{id}/analyses (per-sim list), and GET
#          /analyses/{id}/data (pure fetch-by-id -> list[TsvOutputFile], same shape
#          as legacy POST /analyses; 409 not-ready, 404 unknown, never computes).
#          scripts/backfill_analysis_results.py records READY rows for existing S3
#          analysis dirs (both nestings). n_tp sampling + nonblocking submit are a
#          separate future track. Reconciler fingerprint extended (analysis.n_tp).
# 0.9.23 — pin vivarium-workbench 0.3.1 (bigraph-loom base-path fix). The wiring
#          explorer's "test run" posted a root-absolute /api/composite-test-run;
#          bigraph-loom is a third-party bundle the workbench serves but does not
#          render, so it never received the workbench's base-path URL shim. Under
#          `serve --base-path /workbench` the call escaped the prefix and matched
#          the ALB's /api/* rule -> routed to THIS service, which 404'd it
#          (`POST /api/composite-test-run -> 404` in the api log). The workbench
#          now injects the shim into the loom's HTML entry (workbench #476),
#          covering both the prefixed and the unprefixed /bigraph-loom/* paths.
# 0.9.24 — generic compose-on-Batch made actually runnable. ComposeSimulationServiceRay
#          gains the ParCa cache staging the driver-swap had dropped (it passes
#          stage_s3/stage_dir to _submit_mnp exactly as the ensemble sim path does, keyed
#          by the image tag = workspace commit); run_pbg.py can build the WORKSPACE's own
#          core via PBG_CORE_BUILDER (the generic core registers only process-bigraph base
#          types + pbg-emitters links, so documents referencing workspace-registered types
#          — v2ecoli's ECOLI_TYPES — could not resolve); and run_pbg.py now redirects every
#          emitter's out_dir/out_uri into the results dir the entrypoint syncs to S3
#          (v2ecoli's baseline omits out_dir on purpose, resolving it to
#          <workspace>/.pbg/parquet-runs — real output that never left the container).
#          compose_ray_image_tag loses its "latest" default: that ECR repo is per-commit
#          and has no such tag, so the default could only resolve to a nonexistent image;
#          unset now fails at submit naming the setting. Deployed to stanford-test only.
# 0.9.25 — fix: stage run_pbg.py to S3 instead of heredoc-embedding it in the Batch
#          command. The B3/B4 additions grew the runner to 7933 bytes, pushing the
#          compose container-override command to 8199 — over AWS Batch's 8192 limit
#          ("Container Overrides length must be at most 8192"), so every compose job
#          FAILED at dispatch. The runner is now `aws s3 cp`'d in like the document,
#          keeping the command a few hundred bytes regardless of runner size. Caught
#          by a live smoke test on stanford-test; unit test now guards the 8192 limit.
# 0.9.26 — fix: compose job status froze at QUEUED. list_running_hpcruns polled
#          RUNNING-only, so once the monitor marked a Batch job QUEUED (Batch
#          RUNNABLE/STARTING) it dropped out of the polling set and never advanced —
#          stuck at QUEUED forever even after the Batch job SUCCEEDED and results
#          landed in S3. Now polls every NON-TERMINAL state so a job traverses
#          queued->running->completed. Found by the same stanford-test smoke test.
# 0.9.30 — skipped here on purpose: already used (untagged) on the deploy trunk
#          (feat/multi-generation-dispatch, the branch actually live on
#          sms-api-stanford prod, diverged from main by the sms_api->viva_api
#          rename). Not retroactively reconciled onto main today - see
#          backlog/memory "viva-api deploy-trunk reconciliation debt". Skipping
#          the number here avoids two different tags/releases both claiming 0.9.30.
# 0.9.31 — fix: Ray-native standalone analysis (_run_standalone_analysis_ray_native)
#          never wrote config["analysis_options"], which ORMAnalysis.to_dto()
#          unconditionally reads (AnalysisConfigOptions requires experiment_id) -
#          every GET /analyses/{id} for a Ray-native analysis 500'd with a raw
#          KeyError. Fixed to match the shape the legacy Batch/SLURM producers
#          already write (experiment_id + each analysis domain spread as a
#          top-level key).
__version__ = "0.9.31"
