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
# 0.9.19 — self-diagnosing DB reconciliation (sms_api/simulation/db_reconcile.py +
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
# 0.9.30 — set V2ECOLI_SIM_DATA on submit_ray_native_analysis()'s K8s Job spec
#          (gap #1) so the DuckDB/cd1 analysis suite can resolve sim_data for an
#          S3 sweep (resolve_sim_data only globs a co-located pickle for LOCAL
#          sweep paths). Closes the deploy gap on this branch specifically —
#          main already had this fix (PR #207); this branch (the real Stanford
#          deploy trunk) did not.
# 0.9.31 — skipped here on purpose: used on `main` for the same underlying fix
#          (cherry-picked onto this branch instead of merging main's full
#          sms_api->viva_api rename). Skipping avoids two different tags/
#          releases both claiming 0.9.31 for genuinely different commits.
# 0.9.32 — fix: Ray-native standalone analysis config missing analysis_options
#          (cherry-pick of main's 0.9.31 fix onto this deploy trunk — see that
#          release's notes for the full root cause). ORMAnalysis.to_dto() was
#          unconditionally reading config["analysis_options"], which this
#          producer never wrote, so GET /analyses/{id} 500'd for every
#          Ray-native analysis.
# 0.9.33 — fix: run_standalone_analysis()'s default ptools_* module set (used
#          whenever --modules is omitted) nested ptools_rna/ptools_rxns/
#          ptools_proteins under "multiseed", but those modules are registered
#          scale="single" in v2ecoli/sms-ecoli's ANALYSIS_REGISTRY. Every
#          default-modules dispatch failed with "is scale='single', not
#          'multiseed'" — live-reproduced against a completed pilot simulation,
#          5 separate K8s Job attempts over 22h, all Failed. Default now nests
#          under "single".
# 0.9.34 — fix: unify ray_num_nodes / compose_ray_num_nodes into a single
#          ray_num_nodes setting. Both the ensemble sim path (simulation/
#          simulation_service_ray.py) and the compose path (compose/
#          simulation_service_ray.py) submit through the SAME shared
#          SimulationServiceRay._submit_mnp() -- compose is a thin wrapper
#          around it, not a separate subsystem -- but each read an independent
#          node-count setting. The CDK-side 24-node capacity scale-up
#          (sms-cdk#29) only ever updated compose_ray_num_nodes, silently
#          leaving the actually-used ensemble sim path stuck at ray_num_nodes=4.
#          Live-reproduced: the real 1000x10 baseline job ran on 4 nodes
#          instead of 24 (~14-15 min/gen vs ~8 measured at low contention on
#          the same instance type), extrapolated ~24-27h total, had to be
#          killed. One setting now, can't drift apart again.
# 0.9.35 — fix: Ray-native analysis status polling used the full s3://<bucket>/...
#          result_uri directly as S3FilePath.s3_path, which is documented (and
#          FileServiceS3 relies on it) as BUCKET-RELATIVE -- the bucket is
#          resolved separately from settings, so the full URI double-prefixed
#          the bucket into the key and, via Path()'s slash-collapsing, mangled
#          "s3://" into "s3:/". The constructed key never matched a real S3
#          object, so the manifest-exists check silently 404'd forever.
#          Live-reproduced: atlantis analysis status kept reporting "running"
#          20+ minutes after the K8s pod had genuinely completed with a valid
#          manifest already in S3. New data_layout.key_from_uri() strips the
#          s3://<bucket>/ prefix before constructing S3FilePath.
# 0.9.36 — the multi-generation batch dispatch (previously a hardcoded CLI
#          script, scripts/run_batch_baseline_ray.py) now builds a process-
#          bigraph document and runs it through the SAME generic run_pbg.py
#          runner the compose-on-Batch path already uses, instead of shelling
#          out to a v2ecoli-specific script (backlog items 26/27 — the two Ray
#          job-submission paths never had duplicated submission code, only a
#          duplicated job COMMAND; this closes that gap too, since there is no
#          longer a second execution mechanism to unify). run_pbg.py gains a
#          --composite-id/--overrides mode (process_bigraph.composite_spec
#          resolution, same as vivarium_workbench.lib.pbg_export already uses)
#          alongside its existing static-file mode. Also fixes a real bug this
#          surfaced: the multi-gen dispatch never threaded the real
#          experiment_id through — every batch's zarr/parquet output was
#          silently stamped with the literal "batch_baseline" regardless of
#          the actual request.
# 0.9.37 — fixes V2ECOLI_BASELINE_COMPOSITE_ID: was "v2ecoli.composites.
#          ecoli_baseline", missing process_bigraph.composite_spec's own
#          f"{fn.__module__}.{name}" id scheme's trailing ".ecoli_baseline"
#          (the composite's decorator name=). Every 0.9.36 multi-gen dispatch
#          failed with "no composite registered as
#          'v2ecoli.composites.ecoli_baseline'" — never caught by the unit
#          tests (they mock the whole registry), only by a real pilot
#          dispatch against live GovCloud (2026-08-06). Also strengthens the
#          two ray_backend tests that asserted this id in the constructed
#          command: the old assertion checked a substring that the WRONG
#          value also satisfies (it's a prefix of the real id), so it could
#          never have caught this regression either.
# 0.9.38 — 0.9.37 was STILL wrong: a second real pilot dispatch failed again,
#          identical error, with the now-correctly-SHAPED id. Root cause:
#          "ecoli_baseline" doesn't exist anywhere in sms-ecoli (the deployed
#          simulator image) — confirmed via git show/git grep directly
#          against commit e38f742, not the separate local v2ecoli checkout,
#          which does have an ecoli_baseline.py but is NOT a mirror of what's
#          actually in the image. sms-ecoli's real multi-gen composite is
#          v2ecoli/composites/batch_baseline.py (name="batch_baseline"),
#          whose declared parameters match this dispatch's overrides dict
#          exactly. Renamed V2ECOLI_BASELINE_COMPOSITE_ID ->
#          V2ECOLI_BATCH_BASELINE_COMPOSITE_ID = "v2ecoli.composites.
#          batch_baseline.batch_baseline" (old name was itself misleading —
#          said BASELINE, pointed nowhere real). Same two ray_backend tests
#          updated to the real id (still exact-match, not substring).
# 0.9.39 — Array-jobs-for-canonical dispatch: the batch_baseline multiseed x
#          multigeneration sweep (n_seeds>1, n_generations>1, no composite
#          override) now submits as an AWS Batch ARRAY job -- N independent
#          single-seed children (AWS_BATCH_JOB_ARRAY_INDEX), no Ray cluster
#          -- instead of an MNP Ray cluster. Verified directly against the
#          deployed sms-ecoli source (never assumed from memory): base_seed
#          is a real batch_baseline parameter, and n_seeds=1 deterministically
#          takes v2ecoli's existing sequential no-Ray code path
#          (_resolve_parallel), so an array child never needs Ray at all.
#          New _submit_array/_array_sim_command/_ensure_array_job_def in
#          simulation_service_ray.py (the last mirrors _ensure_mnp_job_def:
#          verified against the real AWS Batch API that plain container jobs
#          can't override the image via containerOverrides either, same
#          limitation as MNP). New ray_array_queue/ray_array_job_definition
#          settings. ParCa stays on MNP unchanged (single deterministic
#          computation, no seed-parallelism); phase0/comparison-ensemble
#          paths stay on MNP unchanged (they genuinely fan out via Ray
#          actors). A single-seed batch_baseline request also stays on MNP
#          (AWS Batch array jobs require size>=2, and there's no parallelism
#          to gain from Array-izing one seed anyway). Companion sms-cdk PR
#          adds the RayArrayJobDef job definition + batch-array-entrypoint.sh
#          -- see the ray-vs-batch-array-jobs-investigation decision: Array
#          jobs for canonical, Ray-MNP stays for colonies.
# 0.9.40 — fix _submit_array's dependsOn: real AWS Batch rejected the array pilot's
#          first live dispatch with "Job Id cannot be set when dependency type is
#          SEQUENTIAL" -- _submit_array had copy-pasted _submit_mnp's dependsOn
#          shape ({"jobId": jid, "type": "SEQUENTIAL"}) verbatim, but SEQUENTIAL
#          is invalid alongside an explicit jobId for a job that also sets
#          arrayProperties (which every array submission does). Fixed to a plain
#          {"jobId": jid} dependency (no "type"). _submit_mnp is untouched --
#          the MNP path has real successful dispatch history with the SEQUENTIAL
#          shape and was never in question. The mocked unit test for the array
#          path had asserted the buggy shape as correct (classic green-mock-as-
#          go-signal: the mock never validates against AWS's real API rules) --
#          strengthened to assert the correct type-less shape, with a comment
#          explaining why so it can't be silently "simplified" back.
# 0.9.41 — analysis auto-triggers from the dispatch DAG (backlog item 24). The Ray
#          backend never read config.analysis_options and submitted no analysis at
#          all, so a completed remote simulation produced zero cd1_*/ptools_*
#          artifacts until somebody ran `atlantis simulation analysis <id>` by
#          hand -- which defeats the "everything triggered through the Workbench"
#          bar. submit_ecoli_simulation_job now submits a THIRD Batch job for the
#          multi-generation batch_baseline sweep, dependsOn the sim job, running
#          the model image's own S3-native scripts/run_standalone_analysis.py
#          (-> v2ecoli.workflow.analysis_runner.run_analyses, the SAME function
#          the composite's inline flush calls) over the landed sweep. So the
#          pipeline is now one Batch dependency DAG, parca -> sim -> analysis:
#          no poller, no webhook, no external watcher.
#          The composite's own inline flush stays disabled ("analyses": "none")
#          on purpose and is NOT the mechanism: the canonical dispatch is an
#          Array job of N single-seed children with no shared filesystem, so an
#          inline flush would run the cross-seed scales against 1/N of the sweep,
#          N times over. The whole-sweep analysis is a gather node by nature.
#          Modules come from the simulation's own analysis_options when the
#          caller set any; otherwise the composite's own "applicable" keyword,
#          which the model image expands with its own ANALYSIS_REGISTRY (sms-api
#          has none) -- see the companion sms-ecoli PR adding that keyword to
#          run_standalone_analysis.py. Every auto-triggered analysis is recorded
#          in the same `analyses` table as a hand-triggered one, so
#          GET /simulations/{id}/analyses and GET /analyses/{id}/status resolve
#          it; a submission failure lands as a FAILED row rather than vanishing
#          (the sim job is already running by then, so raising would orphan it).
#          _submit_mnp gains an optional depends_type: the analysis node waits on
#          an ARRAY parent id, which AWS Batch rejects under SEQUENTIAL; the
#          ParCa -> sim edge keeps its live-verified SEQUENTIAL shape untouched.
# 0.9.42 — backlog item 33 REWORKED from per-generation-array "wave" dispatch to
#          individual per-seed AWS Batch job chains, matching vEcoli-private's own
#          fully-asynchronous per-seed Nextflow execution (Alex's explicit decision:
#          "it must be a true v2 analogy of vEcoli-private"). The wave design made
#          every seed wait at every generation boundary; this doesn't -- seed 5 can
#          be on generation 8 while seed 800 is on generation 1, throttled only by
#          available compute.
#          submit_chain_dispatch_job (new, replaces submit_wave_dispatch_job/
#          submit_next_wave) submits ParCa + EVERY seed's full G-generation
#          dependsOn chain upfront -- N*G individual MNP (num_nodes=1) jobs,
#          TPS-paced below the account-wide 50 TPS SubmitJob cap (_SubmitJobPacer,
#          proactive + real elapsed-time-based, not a fixed sleep guess) with real
#          retry-on-throttle (botocore "standard" retry mode on a dedicated client
#          for this loop only). _seed_generation_command (replaces _wave_sim_command)
#          is simpler than the design it replaces: seed + generation are both known
#          at SUBMISSION time, so the whole --overrides payload is static -- no
#          AWS_BATCH_JOB_ARRAY_INDEX, no lookup table, no container-start shell/
#          python3 merge step at all.
#          WHY MNP, not a "singleton array job": confirmed directly against
#          sms-cdk's batch-array-entrypoint.sh and AWS's own job_env_vars.html that
#          NEITHER shipped entrypoint supports a genuinely standalone job --
#          batch-array-entrypoint.sh hard-requires AWS_BATCH_JOB_ARRAY_INDEX (only
#          set for array children, and arrayProperties.size has a hard floor of 2 --
#          no size-1 array exists), and a true per-seed dependsOn chain needs each
#          generation to be its own job with its own id anyway (array children can't
#          dependsOn each other). MNP num_nodes=1 is the one already-proven
#          standalone-job mechanism (ParCa/analysis already use it) -- reused as-is,
#          no sms-cdk change. _submit_mnp gains an optional retry_strategy override
#          (restores per-job retry on the MNP job definition, which -- unlike the
#          Array job definition -- declares none of its own; matches the Array job
#          def's own already-tuned attempts=2) and an optional batch_client override
#          (lets the bulk submission loop use its own retry-configured client
#          without changing any other existing call site's behavior).
#          FLAGGED, NOT SILENTLY ABSORBED: the MNP queue's compute environment
#          (RayBatchOnDemandCE, confirmed against sms-cdk/lib/ray-batch-stack.ts) is
#          ON-DEMAND ONLY, unlike the Array job definition's Spot-tolerant queue --
#          a real cost-shape difference from the superseded design that the
#          retry_strategy override can't fix (Spot pricing is a compute-environment
#          property, not a submission-time parameter). Left open for a companion
#          sms-cdk change, documented prominently in submit_chain_dispatch_job's own
#          docstring rather than silently ignored.
#          JobScheduler.update_wave_jobs/_advance_wave -> update_chain_campaigns/
#          _advance_chain_campaign: no "advance to next generation" step needed at
#          all now (Batch's own dependsOn already does that) -- just "has every
#          seed's chain reached a terminal state," then submit_campaign_analysis
#          (new, thin wrapper over the unchanged _submit_analysis_job) with NO
#          native dependsOn, since by construction everything it depends on already
#          finished by the time the poller fires.
#          ORMHpcRun.wave_index/wave_seed_indices -> chain_n_generations/
#          chain_final_job_ids (migration f2b8e4a6c9d1 amended in place -- still
#          unmerged, nothing deployed against the old names); ONE HpcRun row now
#          tracks a whole campaign (each seed's own last successfully-submitted job
#          id), not one row per generation. n_seeds >= 2 is no longer required
#          (that floor was AWS Batch's own array-size minimum, moot once nothing is
#          an array job). RayLayout.wave_state_uri/wave_state_prefix ->
#          daughter_state_uri/daughter_state_prefix (pure rename, same S3 layout).
# 0.9.43 — POST /api/v1/simulations returns in seconds for a chain-dispatch
#          campaign of ANY size. submit_chain_dispatch_job issues n_seeds *
#          n_generations individual AWS Batch SubmitJob calls, TPS-paced --
#          ~10,000 calls and ~15 minutes of wall time for the canonical 1000x10
#          shape -- and submit_ecoli_simulation_job awaited all of it INLINE,
#          inside the single HTTP request. Found during a real production
#          dispatch on the smscdk GovCloud deployment (2026-08-14): the calling
#          client (vivarium-workbench, 30s HTTP timeout) gave up long before the
#          loop finished and reported a FAILED dispatch to the user, while
#          viva-api went right on submitting the real, AWS-billed campaign. The
#          obvious response to being told it failed -- retry -- would have
#          started a second, duplicate, paid campaign on top of the first. Only
#          caught by reading kubectl logs on the live pod.
#          The one call site now goes through _submit_chain_dispatch_background,
#          which hands the UNCHANGED submit_chain_dispatch_job coroutine to the
#          LocalTaskService the service already uses for the other multi-minute
#          operation it owns (submit_build_image_job's DooD image build) and
#          returns its JobId.local(...) immediately. No new machinery: every
#          backend service shares ONE process-wide LocalTaskService, so
#          get_job_status -- and therefore GET /simulations/{id}/status --
#          already resolves that id (RUNNING while submitting, FAILED if the
#          submission loop crashes), and cancel_job already routes LOCAL ids to
#          LocalTaskService.cancel, making a still-submitting campaign
#          cancellable for free. submit_chain_dispatch_job itself is untouched
#          and still synchronous for its direct callers (unit tests, the
#          real-AWS integration test).
#          A placeholder HpcRun row is committed synchronously before returning
#          so an immediate status poll has something real to read. It leaves
#          BOTH chain_n_generations and chain_final_job_ids None on purpose:
#          list_active_chain_campaigns discriminates on chain_n_generations IS
#          NOT NULL alone, and get_chain_campaign_result([]) is terminal with
#          zero successes by definition -- so setting either would have
#          _advance_chain_campaign mark the campaign FAILED on the next poll
#          tick, recreating the very false-failure this release removes, this
#          time inside viva-api. The background task is gated on that row being
#          committed, so the real campaign row it inserts at the end always
#          outranks the placeholder in get_hpcrun_by_ref's ORDER BY id DESC
#          lookup; the reverse order would report a whole campaign COMPLETED the
#          moment submission finished, with every real job still queued.
# 0.9.44 — fix V2ECOLI_BATCH_BASELINE_COMPOSITE_ID: stale after an upstream v2ecoli
#          composite consolidation (v2ecoli #373, folded composites/batch_baseline.py
#          into ecoli_baseline.py's baseline()) finally reached sms-ecoli via PR #56
#          on 2026-08-16 (the same sync that carried backlog item 52's wall-time
#          fix). A real pilot dispatch (sim 152) failed both seeds identically:
#          "no composite registered as 'v2ecoli.composites.batch_baseline.
#          batch_baseline'" -- confirmed via the actual CloudWatch job logs, not
#          assumed. Re-verified the new id directly against the deployed sms-ecoli
#          image at the real built commit (c44b69a, build 63) via git show/git grep
#          -- never the separately-diverged local v2ecoli checkout, same discipline
#          the 0.9.38 incident (see above) already established. New id:
#          "v2ecoli.composites.ecoli_baseline.ecoli_baseline". baseline()'s real
#          signature is a strict superset of the old params except one rename:
#          base_seed -> seed (renamed in _seed_generation_command's overrides
#          dict). Backlog item 55.
# 0.9.45 — backlog item 40: POST /api/v1/simulations/{id}/cancel raised a real
#          Postgres InvalidTextRepresentationError writing the terminal job status
#          ("CANCELLED"). Root cause: ORMHpcRun.status has no values_callable, so
#          SQLAlchemy's default Enum type binds a Python enum.Enum member by its
#          NAME (upper-case), not its .value -- confirmed directly against a real
#          Postgres 15 database built from this repo's own Alembic migration chain
#          (not create_all, which always reflects the current model and so can
#          never catch a migration-shaped defect like this one). The real,
#          migration-produced jobstatusdb enum never had upper-case 'CANCELLED'
#          (the prior migration, a1c3e5f7b9d2, added the wrong case: lower-case
#          'cancelled', which the app never writes) and never had 'PENDING' at
#          all, despite list_active_hpcruns/list_active_chain_campaigns (the
#          chain-dispatch campaign poller) both binding it against this same
#          column -- the identical failure, already live, not hypothetical. New
#          migration 44335812e447 adds both real values. Also corrects
#          db_reconcile.py's a1c3e5f7b9d2 LEGACY-fingerprint marker, which checked
#          ONLY the lower-case spelling and was therefore permanently False for
#          any create_all-bootstrapped database (upper-case 'CANCELLED' from the
#          Python enum's .name, never lower-case) -- empirically confirmed this
#          misclassified a fresh create_all database as INCONSISTENT, refusing to
#          auto-reconcile; now accepts either spelling, and a new marker covers
#          44335812e447 per this repo's own fingerprint-maintenance contract.
#          Investigated (not fixed, per backlog item 53's own explicit scope):
#          whether this fix alone makes POST /cancel a real campaign-wide
#          cancel for a chain-dispatch campaign. It does not -- cancel_simulation
#          -> SimulationServiceRay.cancel_job acts on exactly ONE JobId
#          (HpcRun.job_id / job_id_ext), which for a chain campaign's own HpcRun
#          row is the ParCa job's id (see submit_chain_dispatch_job's final
#          insert_hpcrun call), never chain_final_job_ids and never a dependsOn
#          walk. None of a campaign's real N*G per-seed-per-generation jobs are
#          touched. Item 53's walk-back-through-dependsOn design remains the
#          correct fix for that, deliberately not implemented here.
# 0.9.46 — backlog item 61: chain-dispatch simulation output had no
#          analysis-consumable history data -- every seed/generation only ever
#          wrote final_state.json, zero parquet/zarr, because a ParquetEmitter
#          built deep inside a composite's step factory was never flushed
#          before the run_pbg.py driver exited (its __del__ finalizer is
#          explicitly best-effort, not a guarantee). Fixed by calling
#          viva_emitters.ParquetEmitter.flush_all_in_composite() after
#          composite.run(steps), mirroring v2ecoli's own
#          composites/_helpers.py::flush_parquet() -- same call, reused, not
#          reinvented. Directly unblocks item 1: every cd1_*/ptools_* analysis
#          module is an Analysis subclass and routes through the
#          DuckDB/history-parquet path this now actually populates.
# 0.9.47 — backlog item 61 (real fix): PR #251's flush fix never mattered
#          because v2ecoli's ecoli_baseline/batch_baseline composites eagerly
#          construct their default ParquetEmitter *inside* to_document(),
#          resolving a workspace-relative out_dir before run_pbg.py's own
#          document-mutation redirect ever ran. Fixed by using v2ecoli's own
#          real override hook (set_parquet_emitter_override(), built via
#          parquet_vecoli() -- the same preset the eager path itself uses,
#          not a bare dict, which was empirically found to drop the
#          variant/lineage_seed/generation/agent_id hive-partition columns)
#          before the document is built, not after. Verified against a real,
#          non-mocked local composite run (real process_bigraph, v2ecoli,
#          viva_emitters, ParCa cache) -- produced a real history/*.pq file
#          with all 206 real biology columns intact.
# 0.9.49 — backlog item 6: real per-seed aggregate progress for a chain-dispatch
#          campaign. New GET /simulations/{id}/chain-progress, reusing
#          SimulationServiceRay.get_chain_campaign_result (the SAME data
#          get_simulation_status already computes and collapses into one
#          coarse phase) to expose seeds_total/succeeded/failed/in_progress
#          instead. Read-only, no DB writes -- only JobScheduler's own poll
#          loop transitions a campaign row. (0.9.48 is sms-cdk PR #37's
#          companion viva-api PR #256, from a parallel session -- bumped past
#          it here to avoid a version collision between the two open PRs.)
# 0.9.50 — backlog item 65: _submit_mnp routes a standalone (numNodes=1)
#          submission to settings.ray_mnp_standalone_queue instead of
#          ray_mnp_queue, when configured. Chain-dispatch's per-seed-per-
#          generation jobs and ParCa are numNodes=1 with no inter-node
#          traffic to protect, but were paying the full concurrency cost of
#          ray_mnp_queue's cluster-placement-group compute environment for
#          nothing -- confirmed live, stuck at 1 concurrent job with ~1000
#          more ready. Automatic per-call routing keyed on num_nodes, no
#          call-site changes; empty (default) = unchanged behavior, safe to
#          deploy before the standalone queue exists (sms-cdk PR #37).
#          Renumbered from this branch's original 0.9.48 -> 0.9.50: item 6
#          (PR #257) merged first and independently claimed 0.9.49, so this
#          lands second in real merge order.
# 0.9.51 — backlog item 71 (viva-api PR #1 of item 68's V2 non-Nextflow
#          chain-dispatch redesign): new plain, standalone AWS Batch
#          container-type job shape (_ensure_container_job_def/
#          _submit_container, sibling of the existing MNP path -- extracted
#          the shared stage/output/log env-list construction into
#          _stage_out_env, parameterized by prefix, since RAY_*/CONTAINER_*
#          can't literally share one env list). submit_parca_job and the
#          analysis DAG node (_submit_analysis_job, via
#          submit_campaign_analysis) migrate to it -- neither has real
#          inter-node traffic to protect, matching 0.9.50's own reasoning for
#          chain-dispatch's per-seed jobs (migrated in a later phase, not this
#          PR). New empty-default settings ray_container_queue/
#          ray_container_job_definition; both raise a clear RuntimeError
#          naming the setting if referenced before being configured (matches
#          this file's own compose_ray_image_tag precedent) rather than
#          submit a doomed job -- so this PR is inert pre-deploy, exactly like
#          0.9.50's ray_mnp_standalone_queue. Does NOT touch _submit_mnp,
#          submit_ecoli_simulation_job's inline MNP submission, or
#          submit_chain_dispatch_job's per-seed loop -- those stay on the MNP
#          path in this PR; chain-dispatch's own migration + the DB schema
#          change are a separate, later PR (item 71's Phase 4), gated on this
#          one validating first.
# 0.9.52 — backlog item 71 Phase 4 (V2 non-Nextflow chain-dispatch redesign,
#          PR #2 of 2): replaces native AWS Batch dependsOn chaining for
#          chain-dispatch's N*G per-seed generation jobs with app-level
#          incremental submission, the actual fix for item 68's scaling stall
#          (the upfront-dependsOn design never triggered Batch's own compute-
#          environment scaling reconciliation at real backlog size, confirmed
#          via CloudTrail showing zero scaling API activity). submit_chain_
#          dispatch_job now submits ONLY ParCa (migrated to container-type,
#          matching 0.9.51's ParCa/analysis migration) and writes an initial
#          per-seed tracking row; generation submission moves entirely into
#          JobScheduler's existing 30s poll loop (_advance_chain_campaign,
#          rewritten), which submits exactly one generation per seed at a
#          time, only once the previous one (or ParCa, for generation 0) is
#          confirmed SUCCEEDED -- app-level gating instead of native
#          dependency chains, also submitting as container-type jobs
#          (SimulationServiceRay.submit_chain_generation/_batch, new). Three
#          new nullable HpcRun columns (migration 71a5478673a8):
#          chain_current_job_ids/chain_current_generation (per-seed, JSONB)
#          and chain_parca_done (bool). chain_final_job_ids keeps its existing
#          shape but is now written INCREMENTALLY as each seed's chain
#          resolves, not all at submission time -- get_simulation_status and
#          get_simulation_chain_progress (backlog item 6) both updated to
#          read the new fields correctly (the old re-derive-terminal-from-
#          chain_final_job_ids logic would have falsely reported "terminal"
#          for whatever partial subset of seeds had resolved so far, unable
#          to distinguish e.g. "3 of 1000 seeds done" from "campaign
#          complete" -- a real bug this migration had to avoid introducing,
#          not carry forward).
#          Race-condition hardening: DatabaseService.advance_chain_campaign
#          (new) wraps each campaign's whole per-tick read-decide-write in a
#          Postgres pg_advisory_xact_lock keyed on the campaign's own
#          HpcRun.id, so two overlapping ticks against the same campaign
#          (e.g. a rolling restart briefly running two pods) can never both
#          act on the same stale state -- defense-in-depth on top of the
#          existing replicas:1 pin, cheap, no schema change.
#          Backlog item 53 (chain-dispatch campaign-wide cancellation) folded
#          in, backend-only: cancel_simulation now walks a campaign's
#          chain_current_job_ids and terminates each seed's current job
#          (SimulationServiceRay.cancel_chain_campaign, new) -- structurally
#          simpler than item 53's original walk-back-through-dependsOn
#          design, which the per-seed model makes unnecessary (at most one
#          in-flight job per seed at any time, directly actionable). Reuses
#          cancel_job's existing terminate_job call unchanged, already
#          validated by item 53's own empirical testing to work correctly
#          across every non-terminal Batch state.
# 0.9.53 — bump the K8s-native standalone-analysis Job's (run_standalone_analysis,
#          simulation_service_k8s.py) hardcoded memory request/limit from 2Gi/4Gi
#          to 6Gi/10Gi. A real multiseed sweep (item 71 b2, 40 seeds x 10 gens x
#          12 modules) OOMKilled at 4Gi -- a multiseed analysis holds every seed's
#          data in memory at once by design, so the old fixed limit (sized only
#          for v2ecoli's heavier import surface vs. the legacy script, never for
#          sweep scale) doesn't scale with campaign size. Real node headroom
#          checked first (both smsvpctest cluster nodes <15% memory-requested;
#          t3.xlarge allocatable ~14.4Gi each) before picking the new values.
# 0.9.54 — run-simulation-workflow gains an optional extra_params passthrough
#          (backlog items 86/88): a composite-agnostic fallback layer for
#          params with no dedicated named parameter (e.g. a composite's own
#          fork/injection or multi-node-dispatch knobs), merged into the
#          resolved config via setdefault so it can only fill gaps, never
#          override a key the endpoint's own named parameters already set.
#          Additive/backward-compatible -- absent extra_params, behavior is
#          byte-for-byte unchanged (regression-tested).
# 0.9.55 — backlog item 88: generic multi-node process-bigraph composite dispatch
#          (colony is the validating case, not hardcoded). New
#          submit_ecoli_simulation_job routing branch + _submit_multi_node_composite/
#          _multi_node_composite_command, reusing _ensure_mnp_job_def/_submit_mnp/
#          stage_runner and the EXISTING generic run_pbg.py runner unchanged -- no
#          new CDK job def, no new entrypoint module, no process-bigraph change.
#          Cross-node Ray attach needs zero new code either (empirically confirmed):
#          ray-batch-entrypoint.sh already exports RAY_ADDRESS on the head, and
#          process-bigraph's own RayProtocolRuntime fallback already calls bare
#          ray.init(), which already respects RAY_ADDRESS from the environment.
# 0.9.56 — backlog item 88: a completed multi-node composite dispatch now joins
#          the same auto-triggered "Analysis flush" chain-dispatch campaigns
#          already get, via a deliberately SEPARATE, additive path -- new
#          JobScheduler.update_multi_node_jobs/_advance_multi_node_job (wired
#          into the poll loop alongside, never replacing, update_chain_campaigns),
#          new DatabaseService.list_active_multi_node_composites/
#          finalize_multi_node_job (an atomic conditional UPDATE, not a Postgres
#          advisory lock -- a single row's status transition, not a multi-field
#          per-seed read-decide-write, needs no more than that), new
#          SimulationServiceRay.submit_multi_node_analysis. New nullable
#          hpcrun.multi_node_composite_id column (migration 9c2e6b1f4a73) is the
#          discriminator, mutually exclusive with chain_n_generations by
#          construction -- proven disjoint against a real Postgres database, not
#          just by reading the two WHERE clauses (see
#          test_chain_dispatch_and_multi_node_polling_are_mutually_disjoint).
#          _submit_multi_node_composite now records its own HpcRun row (mirrors
#          submit_chain_dispatch_job's identical existing pattern) -- zero
#          changes to the generic run_simulation_workflow handler.
#          run_pbg.py's generic runner (viva_api/compose/run_pbg.py) gains a
#          second generalization: when a document's own emitter has nothing for
#          _redirect_emitters to redirect (a plain in-memory emitter, e.g.
#          colony's default), gather and persist its history to
#          emitter_history.json -- generic, not colony-specific; a document
#          with a real file-backed emitter is completely unaffected (this only
#          runs when nothing else already shipped output). This is what makes a
#          real analysis (not just the final-snapshot final_state.json) possible
#          for ANY multi-node composite dispatched this way.
# 0.9.57 — GET .../observables/index now fails loudly (409) for a chain-dispatch
#          campaign or a multi-node composite dispatch (e.g. colony) instead of
#          falling through to RayLayout.seed_store_uri's flat "v2ecoli_seed{NN}.zarr"
#          convention -- a store neither dispatch shape ever writes. Both are still
#          Ray backend, so the existing guard (backend-type only) let them through
#          silently. _ray_seed_store_uri_or_error now also checks
#          HpcRun.chain_final_job_ids/multi_node_composite_id (the same fields used
#          elsewhere to distinguish these shapes) and points the 409 at
#          GET /analyses/{id}/status, the endpoint that actually serves that output.
#          Found live investigating a real cplong bug report on smsvpctest: the
#          phase0 (plain 1-seed/1-gen) dispatch shape was writing its own store to
#          the wrong S3 layout entirely (fixed upstream in v2ecoli/sms-ecoli, not
#          this repo) -- this release hardens the same endpoint against the two
#          OTHER dispatch shapes it was never meant to serve, so a future
#          misdirected call fails clearly instead of confusingly.
# 0.9.58 — env-worker lifecycle endpoints + worker pod spec (image-as-worker,
#          vivarium-workbench#942 / REFACTOR-PLAN §2A.8). /env-worker/v1/workers
#          runs a simulator's PREBUILT image as the workbench's env worker: a Job
#          from ecr:<commit>, the worker module staged in from the workbench image,
#          two emptyDir volumes, and the worker dials back to the caller. Hosted
#          builds no venv on the PVC at all.
# 0.9.59 — wire the env-worker service at startup. 0.9.58 shipped the router and
#          the service but never called set_env_worker_service(), so every
#          /env-worker/v1 call answered 503 ("not configured") — the guard doing
#          its job, with nothing behind it. Found on dev launching a real worker.
# 0.9.60 — env-worker fixes found by the first real launches on dev: the router's
#          workspace default shadowed env_worker_workspace_path (so every worker
#          ran a path absent from its pod and silently fell back to a GLOBAL
#          generator scan); Job names collided per commit; a 409 surfaced as 500.
# 0.9.61 — two real bugs in the analysis-result read endpoints, found live by
#          cplong90 2026-08-27 re-testing #283: GET /analyses/{id}/data returned
#          200 [] for a real, completed, non-empty analysis (fetch_analysis_data
#          passed the full s3://<bucket>/... result_uri straight into
#          S3FilePath, whose s3_path is bucket-relative -- Path()'s slash-
#          collapsing mangled "s3://" into "s3:/", so the listing silently
#          matched nothing; the SECOND time this exact bug class has hit this
#          file, the first being 2026-08-05's key_from_uri fix for
#          handle_get_ray_analysis_status's manifest lookup -- now routed
#          through the same key_from_uri, via a new shared
#          _list_analysis_result_files helper so both call sites can't drift
#          apart again). GET /analyses/{id}/plots 500'd unconditionally for
#          every Ray/K8s-backend analysis -- handle_get_analysis_plots only
#          ever implemented the legacy SLURM local-filesystem path; added
#          handle_get_ray_analysis_plots, an S3-backed implementation mirroring
#          fetch_analysis_data's (now-fixed) pattern, filtered to .html.
#          Also clarified the chain-dispatch/multi-node 409's error message
#          (#283): "Use GET /analyses/{id}/status" read as the SIMULATION id,
#          not the separate analysis id space -- now spells out
#          GET /simulations/{id}/analyses as the lookup step first.
# 0.9.79 — a legacy config's swap_processes/add_processes/exclude_processes/
#          variants/parca_options.new_genes now reach the chain-dispatch
#          path (submit_chain_dispatch_job + JobScheduler's per-tick seed
#          advance), not just the composite-comparison MNP path -- found via
#          a real GovCloud dispatch (backlog item 93) whose requested KPI
#          column was completely absent from output because the canonical
#          (composite=None, generations>1) dispatch shape routes to
#          submit_chain_dispatch_job, never _sim_command's own multi-gen
#          branch, which was the only place this passthrough previously
#          existed (and was unreachable from that call site regardless).
#          Same class of fix as the extra_params mechanism (0.9.5x era,
#          backlog items 86/88); byte-for-byte unaffected when a config sets
#          none of these fields.
# 0.9.80 — fix: every run_pbg.py invocation now sets PYTHONPATH=V2ECOLI_DIR.
#          ecoli_baseline.baseline()'s injection branch (taken whenever a
#          composite dispatch carries injected_processes -- 0.9.79's own
#          swap_processes/add_processes/exclude_processes passthrough, and
#          item 88's multi-node/colony composite path) does
#          `from scripts._compare.inject import (...)`, a bare absolute import
#          that only resolves when the repo root (which DOES contain
#          scripts/, copied in by sms-ecoli's own Dockerfile `COPY . .`) is on
#          sys.path. Every call site invokes the runner via an absolute
#          /tmp/run_pbg.py path, which puts /tmp on sys.path[0] instead of the
#          cwd -- `cd {V2ECOLI_DIR}` alone never fixed this. Found live
#          2026-09-01 (backlog item 93's own independent swap_processes
#          verification): a real chain-dispatch run with a non-empty
#          injected_processes failed ModuleNotFoundError('scripts') despite
#          the cd already being correct, and 0.9.79's fix genuinely reaching
#          the container. All 3 run_pbg.py call sites in
#          simulation_service_ray.py (the multi-gen batch path,
#          _seed_generation_command, _multi_node_composite_command) now share
#          one PBG_RUNNER_ENV constant instead of duplicating the env string,
#          so this class of drift can't happen at 2 of 3 sites again. The
#          separate /compose/v1/* full-emit path (a different, unverified
#          code shape not used by item 93/96's own dispatch route) is
#          deliberately out of scope here.
# 0.9.83 — fix: _parca_command() now forwards parca_options.bundle_overrides to
#          v2ecoli-parca's own --bundle-overrides flag, same class of gap as
#          0.9.79's new_genes passthrough, missed in that pass. Found live by
#          cplong90 (sms-ecoli#184 / viva-api#365): the stored request carried
#          bundle_overrides correctly, but ParCa built from defaults only and
#          the overrides manifest's keys were absent -- job 252/dataset 150
#          failed outright ("This new_genes_data subdirectory is invalid"),
#          and a second finding (dataset 145, jobs 242-245) showed new_genes
#          itself can silently drop with the job still reporting SUCCEEDED --
#          working hypothesis is that dataset predates 0.9.79's fix, not a
#          live regression (both real call sites -- submit_chain_dispatch_job
#          and the composite-comparison ensemble path -- independently
#          re-verified at this commit to correctly thread new_genes through).
#          Byte-for-byte unaffected when a config doesn't set bundle_overrides.
# 0.9.84 — fix: _seed_generation_command() now sets stop_at_division=True,
#          unconditionally, on every chain-dispatch generation job (backlog
#          item 103). Without it, n_seeds=1/n_generations=1/no stop_at_division
#          made ecoli_baseline.baseline()'s own dispatch gate (n_seeds>1 or
#          n_generations>1 or stop_at_division) evaluate False on every single
#          chain-dispatch generation, routing through the plain, non-division-
#          gated single-cell build the composite's own docs call "NO
#          division-stop" -- each job ran for exactly 1 simulated second
#          (the hardcoded -n 1) regardless of generation_index, and
#          initial_carry_state_path/daughter_state_out_path (this method's own
#          checkpoint/resume fields) were silently never consumed, since they
#          only apply inside the gated branch. Confirmed empirically in real
#          campaign 171 production output (item 71's own flagship "1000x10 in
#          48 minutes" dispatch): generation 0, 5, and 9 of the same lineage
#          were MD5-identical files, global_time never exceeded 1.0 across 10
#          chained "generations" -- every chain-dispatch campaign since the
#          v2ecoli composite-id unification (v2ecoli#373, 2026-07-25) almost
#          certainly produced the same degenerate repeated-snapshot data, not
#          real multi-generational lineages. stop_at_division=True routes
#          through the SAME LineageProcess machinery item 101's own
#          lineage_ray_batch composite uses, via ecoli_baseline.baseline()'s
#          existing batch/lineage branch (Option A, issue #495) -- that
#          branch's own checkpoint/resume handling (lineage.py) is real,
#          already correctly built for exactly this "one generation per
#          process invocation" caller shape, and was simply never wired up;
#          no other change was needed. Unconditional, not caller-controlled --
#          there is no legitimate chain-dispatch generation that should not
#          stop at division.
# 0.9.85 — fix: _mnp_node_vcpus() now retries (3 attempts, short backoff) on
#          an empty/missing describe_job_definitions result instead of giving
#          up on the first call. A freshly-registered MNP job definition (this
#          method always runs right after _ensure_mnp_job_def registers one)
#          can briefly come back empty due to AWS eventual consistency --
#          confirmed live 2026-08-25 on a commit's first-ever multi-node
#          dispatch (item 101, sim255): the identical job definition, queried
#          again a few minutes later, returned correctly. Previously this
#          silently left RAY_SHARDS_DEFAULT unset on exactly that first
#          dispatch, capping the Ray actor pool at os.cpu_count() (observed:
#          16 concurrent workers instead of the real 256-vCPU ceiling).
#          Byte-for-byte unaffected once a job definition is already visible
#          (the common case, resolves on the first attempt as before).
# 0.9.86 — feat: chain-dispatch's composite-id is now caller-selectable
#          (backlog item 105), not hardcoded to
#          V2ECOLI_BATCH_BASELINE_COMPOSITE_ID. A new optional
#          ``composite_id`` extra field on SimulationConfig (same
#          extra="allow" shape as injected_processes/variants) threads
#          through _seed_generation_command / submit_chain_generation /
#          submit_chain_generation_batch, re-derived fresh from
#          Simulation.config on every JobScheduler tick (same restart-safe
#          pattern as injected_processes/variants, item 93). Omitted or
#          None preserves the exact previous behavior for every existing
#          caller. Exists because CD2 Run 1 (K4)'s real composite,
#          reactor_bird_coupled, gained the same injected_processes/variants
#          shape (v2ecoli #648) but had no way to ever be selected as a
#          chain-dispatch target -- this is the reachability half of that
#          gap; reactor_bird_coupled's own capability half was v2ecoli's.
#
# 0.9.87 -- scripts/build_new_gene_cache.py (v2ecoli's own new-gene induction-
#           LEVEL script, the "other half" of ParCa's new_genes presence/
#           absence flag) is now remotely reachable (backlog item 105).
#           _parca_command preserves the raw parca_state.pkl.gz in the synced
#           cache dir (previously discarded); new
#           _build_new_gene_cache_command / submit_new_gene_cache_job mirror
#           submit_parca_job, staging in a commit's plain cache and writing a
#           variant-labeled derived one (cache_s3_uri gains the same variant
#           kwarg _upstream_cache_s3_uri already had); cache_variant threads
#           through job_scheduler.py exactly like composite_id (item 105a).
#           New standalone POST /parca/new-gene-cache endpoint triggers it --
#           deliberately not auto-wired into JobScheduler's own state
#           machine (would need a new DB-level JobType member, left for a
#           follow-up). All additive; every existing caller unaffected.
__version__ = "0.9.87"
