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
#
# 0.9.88 -- fixes a real bug in 0.9.87's own POST /parca/new-gene-cache: its
#           precondition check (get_hpcrun_by_ref(ref_id=parca_dataset_id,
#           job_type=PARCA) == COMPLETED) can NEVER resolve for a real
#           chain-dispatch-originated ParcaDataset -- only the legacy
#           SLURM-only run_parca handler ever inserts that HpcRun shape;
#           chain-dispatch (what Runs 1/2/3 actually use) tracks its ParCa
#           phase on the Simulation's own HpcRun (chain_parca_done), never a
#           matching ParcaDataset one. Found live, during this session's own
#           verification pass, before any real dispatch was fired against
#           it -- would have 409'd on every real input. Removed the broken
#           check entirely, matching this class's own established pure-
#           passthrough philosophy (injected_processes/variants/
#           composite_id: none pre-validated here, a bad reference fails
#           loudly downstream instead). 4 new handler-level tests
#           (tests/common/handlers/test_simulations_handler.py) cover the
#           real gap the original PR's tests missed -- they mocked the
#           service layer, never this handler's own DB-facing logic.
#
# 0.9.89 -- `atlantis simulation run-pbg-native` (backlog item 101/109): a new
#           CLI command for the pbg-native multiseed/multigeneration dispatch
#           (N real ray:LineageProcess nodes in one composite), giving it a
#           real first-class client alongside `simulation run` (chain-
#           dispatch) instead of only a hand-rolled curl script
#           (pbg-dispatch.sh). Fires the identical POST /api/v1/simulations +
#           extra_params.multi_node_dispatch request that script builds.
#           Along the way, fixed a real, previously-unexercised body-shape bug
#           in app_data_service.py's own submit_run_workflow: the route
#           declares TWO separate Body(...) params (analysis_options,
#           extra_params), which FastAPI nests under their own keys
#           (`{"analysis_options": ..., "extra_params": ...}`) -- the old
#           client sent analysis_options bare/unwrapped instead, a latent bug
#           that never surfaced because no existing caller set BOTH at once.
#           Verified live: a real smoke dispatch's stored config showed
#           multi_node_dispatch reaching the server intact, then cancelled.
# 0.9.92 -- carry a caller's extra injected_processes keys (e.g. cache_dir)
#           through injected_processes_from_config's rebuild instead of
#           reconstructing only {swap_processes, add_processes,
#           exclude_processes, fork_repo} (viva-api#392, follow-up to #385/
#           #387). Without cache_dir a fork-free swap's resolve_injections()
#           spec-building has no ParCa bundle path, so the swapped-in process
#           mounts with an empty config and crashes at tick 0 well away from
#           the real cause -- root-caused precisely by cplong90 on #387's own
#           thread, independently confirmed live by jcschaff. The four
#           canonical keys are still normalized defaults layered on top, not
#           replaced -- flat/legacy-shape output is byte-identical.
# 0.9.93 -- injected_processes_from_config resolves swap_processes/
#           add_processes/exclude_processes PER FIELD instead of choosing the
#           whole nested-vs-flat shape once (viva-api#401, found by cplong90
#           while measuring #387's own effect). The either/or choice meant a
#           nested submit setting only swap_processes silently dropped a
#           config's own flat add_processes/exclude_processes -- observed live
#           (sim 296, mecillinam_wellmixed.json): a nested metabolism swap
#           dropped all 4 of the config's own add_processes, unreported. Each
#           field now resolves independently, nested winning on a real
#           conflict; a real conflict is logged (a caller's override, not a
#           silent one). Same root cause as #392 above -- both are
#           consequences of RECONSTRUCTING the block instead of merging into
#           it, cplong90's own framing on #401.
# 0.9.94 -- fix: run_pbg.py's effect check (#395/#398, PBG_MIN_GLOBAL_TIME) now
#           also checks LineageProcess's own per-generation `duration` (summed
#           across every summary.generations entry found in final_state.json),
#           not just the composite's top-level global_time. A chain-dispatch/
#           pbg-native generation only advances the OUTER composite's clock by
#           the single external run(interval) tick it was invoked with --
#           LineageProcess's own docstring: "the inner composite's global_time
#           RESTARTS at 0 each generation" -- so a real, multi-thousand-second
#           division reads back as global_time~=1.0, indistinguishable from a
#           genuine one-tick collapse under the old check. Found live: sms-
#           ecoli#210, dispatch 297 -- chain-dispatch's metabolism-redux swap
#           genuinely divided at t=2527s (confirmed via the real CloudWatch
#           log) but the job still exited 1, effect-check false positive. Both
#           dispatch mechanisms share this runner, so both are fixed by the
#           one change. Non-lineage composites are byte-for-byte unaffected
#           (no summary.generations shape found -> falls back to global_time
#           exactly as before).
#           0.9.99 -- JobScheduler/ComposeJobMonitor.get_hpcrun_by_correlation_id
#           no longer caches a miss (viva-api#416). It was @alru_cache, which
#           keeps a successful None forever; every dispatch path submits to the
#           backend BEFORE inserting the HpcRun row, so a worker event that
#           arrived first poisoned its correlation_id for the pod's life and
#           every later WorkerEvent for that run was dropped ("No HpcRun found
#           ... Skipping event") while the row sat running. Hits (immutable
#           ids) are still cached; a miss is re-asked next time. Found by the
#           in-memory-state survey done for #414.
#           0.9.102 -- _submit_multi_node_composite (the generic multi-node
#           process-bigraph composite path, e.g. lineage_ray_batch/colony) no
#           longer builds a plain ParCa cache unconditionally when a caller
#           sets cache_variant. A variant cache is meant to already exist
#           (POST /parca/new-gene-cache, #378, or an external bridge sync);
#           with no existence check, a fresh commit's own cache_variant slot
#           silently got a stock/un-perturbed cache instead, indistinguishable
#           from the real one short of inspecting cache_version.json by hand.
#           Root-caused live from Dispatch 339:Run 1 / Dispatch 340:Run 2 both
#           resolving to stock caches (sms-ecoli#210). Now checks the staged
#           S3 prefix first: existing content skips the ParCa job entirely
#           (composite submits directly, no dependsOn); missing content fails
#           loud with a ValueError instead of fabricating a substitute.
#           cache_variant=None (every other existing caller) is byte-for-byte
#           unaffected.
#           0.9.103 -- Run 1's real missing-output fix (Alex's Option 1
#           decision, 2026-09-06): a new dispatch path, mbp_dispatch, invokes
#           v2ecoli's own run_mbp_tracked.py remotely (e.g. the
#           reactor-bird-coupled-batch-multigen variant) with
#           V2E_STUDIES_ROOT pointed under SIM_OUT_DIR, the one directory the
#           Ray entrypoint actually syncs to S3. Fixes Dispatch 322's own
#           real symptom: reactor_bird_coupled ran cleanly for ~3h with zero
#           retrievable output because its parquet landed under the image's
#           own REPO_ROOT/studies instead. A prior declared-emitter fix
#           attempt (v2ecoli#700) is structurally impossible for this
#           composite -- 3 of 6 real paths live under agents/0/, not
#           top-level, so _merge_emit_paths silently drops them regardless of
#           what's declared; run_mbp_tracked.py's own runtime emitter is the
#           one mechanism confirmed (locally) to carry all six and survive
#           division. Single-container job (matching this composite's own
#           non-ray:-distributed nature). cache_variant support mirrors the
#           #437 guard from the start (the standing parity-check discipline,
#           applied at build time): a variant cache must already exist,
#           checked via S3 existence before submitting anything.
#           0.9.104 -- ParcaOptions now declares include_violacein_reactions
#           (bool | None). A real, tracked config (configs/
#           pathway_expression_carina_final.json, CD2 Run 4) sets this field
#           and used to fail SimulationConfig validation outright with
#           extra_forbidden -- the exact class of gap new_genes/
#           bundle_overrides were before they were declared (items 93/104).
#           Genuinely consumed by v2ecoli's own injection pipeline (library/
#           inject.py), not by anything viva-api's own dispatch command
#           construction builds; None (the default) preserves v2ecoli's own
#           auto-detect fallback for every existing caller.
#           0.9.105 -- _mbp_tracked_command/_submit_mbp_tracked_dispatch (item 116)
#           gain 7 new optional params (seed, cells_per_agent,
#           initial_glucose_mM, initial_ammonium_mM, injected_processes,
#           reactor_config, aeration_schedule) -- Dispatch 370's own request
#           only exercised variant/max_generations; Chris's real CD2 Run 1
#           coupled-arm spec (sms-ecoli#210, 2026-09-06) additionally needs a
#           per-lineage --seed (hive-partitions the parquet output; a
#           mismatched/repeated seed across dispatches silently MERGES rather
#           than erroring -- real data loss, not just a missing feature) and 3
#           file-path arguments the runner enforces as absolute, given here as
#           paths relative to V2ECOLI_DIR and resolved once server-side. All 7
#           omitted (every existing caller) is byte-for-byte unaffected.
# 0.9.107 -- fix: run_simulation_workflow now force-assigns experiment_id
#            (config_data["experiment_id"] = unique_experiment_id), never
#            setdefault (backlog item 117). A config template's own baked
#            experiment_id used to win via setdefault's no-op -- harmless for
#            a one-off dispatch, but any config reused/copied across runs
#            (real, observed behavior) silently collides every such dispatch
#            onto ONE S3 prefix, each exiting 0 and looking healthy. Real
#            incidents: Dispatch 339 (Run 1) was submitted from a copied Run 2
#            config whose own baked experiment_id was never re-pointed, so its
#            real output landed under Run 2's folder (independently confirmed
#            by cplong90, sms-ecoli#235, precise repro); Dispatch 340 had the
#            mirror problem. unique_experiment_id already embeds the caller's
#            own `experiment_id` query param as a substring, so a caller's
#            naming intent survives the fix -- only a config file's own baked
#            value stops winning. The DB's own SimulationRequest.experiment_id
#            (a separate, top-level field) was ALREADY always unique_
#            experiment_id; this makes config.experiment_id -- what a
#            dispatch actually writes its S3 output under -- agree with it,
#            closing the one real place they could diverge. New hermetic
#            regression test (no Docker needed) drives run_simulation_workflow
#            end to end with a config baking a stale experiment_id and asserts
#            the two now agree and neither is the stale value.
# 0.9.111 -- _submit_multi_node_composite and _submit_mbp_tracked_dispatch now
#            tag every submitted job with CacheVariant (the requested variant
#            name, or "stock" when cache_variant is omitted). Chris (cplong90,
#            sms-ecoli#210) flagged that omitting cache_variant resolves to the
#            stock per-commit cache with nothing on the job itself to show
#            which cache actually ran -- the real cause on his side was his own
#            dispatch generator silently dropping the field, already fixed
#            there, not a viva-api defect. This tag makes the resolved choice
#            visible directly on the AWS Batch job (describe-jobs) instead of
#            requiring a manual decode of the staged S3 path. No behavior
#            change; every existing caller's dispatch is unaffected.
# 0.9.112 -- viva-api#448: analysis for both chain-dispatch (_submit_analysis_job
#            / _analysis_command) and multi-node-composite (submit_multi_node_
#            analysis / _multi_node_analysis_command) dispatches now threads
#            cache_variant into V2ECOLI_SIM_DATA -- without it, a candidate
#            strain's analysis silently read the plain per-commit STOCK
#            simData (the multi-node path never staged a cache locally at all,
#            so it fell through to the image's own stock knowledge-base
#            build). None (every caller before #448) is byte-for-byte
#            unaffected. 6 new regression tests.
# 0.9.113 -- _parca_command gains an rnaseq_source passthrough (ParcaOptions.
#            rnaseq_source -> v2ecoli-parca's own --rnaseq-source flag), threaded
#            through submit_chain_dispatch_job and run_simulation_workflow's
#            comparison-ensemble path, same shape as the existing new_genes/
#            bundle_overrides passthroughs (items 93/104). Real, confirmed gap:
#            at least one live bundle_overrides manifest (sms-ecoli's own
#            rung5-lambda-075/overrides.tsv, part of the CD2 Run 2 J3/K4
#            translation-efficiency rebuild recipe) is a documented no-op
#            without this flag -- "READ BY NOTHING... the scenario silently
#            becomes its own control" -- the same silent-wrong-build failure
#            class new_genes/bundle_overrides were declared to close. Default
#            None is byte-for-byte unaffected. 4 new regression tests.
# 0.9.114 -- item 106/#166 chassis-provenance thread (v2ecoli#735, Eran's own
#            interface handoff): _parca_command now non-fatally copies the
#            chassis-provenance sidecar (parca_state.provenance.json) alongside
#            parca_state.pkl.gz into PARCA_CACHE_DIR (a pre-#735 v2ecoli image
#            never writes it, so `|| true` keeps every dispatch working
#            unchanged until it does). ParcaOptions.require_clean_chain (and a
#            sibling multi_node_dispatch.require_clean_chain field) opt a
#            dispatch into emitting the unprefixed V2E_REQUIRE_CLEAN_CHAIN=1
#            env var v2ecoli's own verify_cache_version reads directly --
#            threaded through submit_ecoli_simulation_job's comparison-
#            ensemble/phase0 path and _submit_multi_node_composite (the actual
#            mechanism Run 2/Run 4 dispatch through). Default False emits
#            nothing on every path -- most existing callers don't pass
#            v2ecoli's own sources= yet (v2ecoli's own PR 3), so requiring this
#            unconditionally would hard-fail all of them, including Run 4's
#            already-built new-gene caches. This is viva-api's own PR 4 of
#            Eran's 4-PR chassis-provenance split (PRs 1+2 v2ecoli core, PR 3
#            sms-ecoli hooks, both his). 7 new regression tests.
# 0.9.116 -- NEVER SHIPPED. Originally reserved by this same PR (feat/item-
#            run3-lineage-debug-division); #486 and #487 merged first and
#            claimed 0.9.117/0.9.118 while this PR was still open, so this
#            entry moved to 0.9.119 below (its own second renumber -- it was
#            0.9.115 before that, displaced by Jim's #481/#483). The number
#            0.9.116 is intentionally never used; left as a documented gap
#            rather than reused, so history stays traceable to what was
#            actually built at each real release.
#           0.9.117 -- ParcaOptions.bundle_overrides (items 93/104/106) now accepts a
#           list of paths, not only a single string -- _parca_command emits
#           one `--bundle-overrides PATH` flag per entry, IN ORDER (matches
#           SourceBundle.__init__'s own docstring/type hint, action="append").
#           Real, confirmed gap: sms-ecoli's own declared recipe for
#           rebuilding the CD2 J3 candidate chassis (cd2-pnnl-01-bundle-
#           scenarios/sims/run_scenarios.sh, scenario rung5_lam075) stacks
#           TWO overrides in one v2ecoli-parca invocation (--new-genes
#           violacein_gfp --bundle-overrides .../vio-gfp/overrides.tsv
#           --bundle-overrides .../rung5-lambda-075/overrides.tsv --rnaseq-
#           source experimental) -- a single-string field could only ever
#           carry one of the two layers, so no remote dispatch could
#           reproduce this recipe at all. strain_from_config's own _norm
#           helper updated to join a list (",") rather than silently return
#           None for it, so *_EXPECT_BUNDLE_OVERRIDES keeps reflecting the
#           real strain request instead of regressing to the pre-item-104
#           silent-drop failure mode for this one input shape. A bare string
#           (every existing caller) is byte-for-byte unaffected. Superseded a
#           speculative bundle_manifest_path/build_combined_bundle_manifest
#           design (sms-ecoli#278's own combined-manifest script) considered
#           first -- reading sms-ecoli's actual declared recipe directly
#           showed the real gap was list support, not a merged base manifest;
#           #278 remains real, merged sms-ecoli infra, just not what this
#           fix needed. 7 new regression tests (_parca_command flag assembly
#           x4, strain_from_config x2, single-string/list equivalence x1).
# 0.9.118 -- _submit_multi_node_composite's `steps` (item 105/#166, the
#            K4-canary "under-run" empty-emit bug: sms-ecoli#166 comment
#            5579146363, eagmon) now computes a real `required_run_interval`
#            (`n_generations * max_duration_per_gen`, the composite's own
#            documented contract -- Composite.run(steps) takes TOTAL
#            SIMULATED TIME, not a tick count) whenever `params` sets
#            `n_generations`, and clamps to `max(explicit_steps, required)`
#            rather than leaving `steps` at its silent default of 1. Every
#            `ray:LineageProcess` node's own `interval` is `max_duration_per_
#            gen`; process-bigraph only invokes a process whose next event
#            falls inside the run window, so `steps=1` invoked nothing and
#            nothing emitted (reproduced: run(1) -> 0 rows, run(3600) -> 1 --
#            matches Dispatch 438's real final_state exactly). Deliberately
#            narrow (not composite-generic): this dispatch method has no
#            remote visibility into a composite's own registered parameter
#            schema, so `n_generations` in `params` is the signal this IS a
#            lineage-shaped request; `max_duration_per_gen` falls back to
#            3600.0, the same default every real v2ecoli lineage/batch
#            document-builder declares (confirmed by direct read of
#            lineage_ray_batch.py/batch_lineage_ray.py/workflow_nf.py/
#            lineage_step.py, not assumed). A composite that never sets
#            n_generations is completely unaffected. Interim fix pending
#            Eran's own proposed document-level `required_run_interval`
#            contract (not yet merged), which would make this fully
#            composite-generic. Uses `math.ceil` (not a truncating `int()`) on
#            the computed interval, matching sms-ecoli#283 (cplong90)'s own
#            more rigorous choice -- that PR is the CLIENT-side complement to
#            this one (its own generator now derives and emits a correct
#            `steps` in every request row); this fix is the server-side
#            backstop for anything that still omits or under-computes it.
#            4 new regression tests.
# 0.9.119 -- item 106/#210 Run 3 diagnostic: a new lineage_debug_division opt-in
#            (SimulationConfig extra field, same undeclared extra="allow" shape
#            as cache_variant -- no ParcaOptions/models.py change needed) emits
#            the unprefixed LINEAGE_DEBUG_DIVISION=1 env var v2ecoli's own
#            LineageProcess._run_until_division (v2ecoli#733) reads directly via
#            os.environ.get. Threaded through _stage_out_env, _submit_container,
#            submit_chain_generation/_batch, and both of job_scheduler.py's
#            chain-dispatch call sites (generation-0 fan-out and per-seed
#            advance), re-derived from Simulation.config on every tick, matching
#            the existing cache_variant/exchange_fluxes restart-safe pattern.
#            Exists to let a real chain-dispatch generation job run with
#            instrumentation on, to determine whether Dispatch 439:Run 3's
#            unexplained one-tick collapse is a division-detection bug (this
#            flag would show it) versus something else. Default False emits
#            nothing on every path -- every existing caller byte-for-byte
#            unaffected. 6 new regression tests. Renumbered twice: 0.9.115
#            (displaced by Jim's #481/#483) -> 0.9.116 (displaced by #486/#487
#            merging first while this PR was still open) -> 0.9.119.
# 0.9.120 -- item 451/#166, Run 4 founder-chassis rebuild: _parca_command gains
#            bundle_manifest_path/build_combined_bundle_manifest/
#            include_violacein_bundle/deterministic_hash_seed passthroughs.
#            Real, confirmed need: sms-ecoli's own declared recipe for Run 4's
#            violacein founder chassis (scripts/build_run4_founder_caches.py's
#            own module docstring) is `python scripts/build_combined_bundle_
#            manifest.py --include-violacein && PYTHONHASHSEED=0 v2ecoli-parca
#            --mode full --new-genes violacein_MG1655_M5 --bundle-manifest-path
#            out/combined_violacein.tsv` -- none of these four levers existed on
#            a remote dispatch before this. bundle_manifest_path/
#            build_combined_bundle_manifest are mutually exclusive (same
#            contract as before); build_combined_bundle_manifest regenerates
#            the manifest fresh in-container (a deliberately machine-local,
#            gitignored build artifact per that script's own notes, never
#            committed) rather than trying to ship a pre-built one.
#            deterministic_hash_seed is a separate opt-in (not tied to the
#            other three) since it's a real, independent requirement of this
#            same recipe. All four default to no-ops -- every existing caller
#            byte-for-byte unaffected. This is a second attempt at a design
#            first built (then discarded) earlier this session for a DIFFERENT
#            consumer (Run 2's J3 chassis, which turned out to need
#            bundle_overrides list support instead, #486) -- reused here
#            against its own real, verified use case. 12 new regression tests.
# 0.9.121 -- run_simulation_workflow's extra_params fallback layer now deep-
#            merges parca_options PER SUB-FIELD instead of the generic top-
#            level setdefault. Real, confirmed gap: config_data.setdefault(
#            "parca_options", extra_params_value) is a no-op whenever the
#            config template's own JSON already declares a parca_options
#            block at all -- true of essentially every real config with
#            meaningful ParCa settings -- so an extra_params.parca_options
#            override was silently discarded WHOLESALE, not merged. Caught
#            live firing a real Run 4 chassis rebuild (0.9.120's own new
#            fields) whose new_genes/bundle_overrides happened to
#            coincidentally match the template's own baked-in values, masking
#            that the override itself never took effect at all. Fix keeps the
#            same "template's own explicit value always wins" contract this
#            function's docstring already promises, applied one level deeper
#            for this one nested, model-backed key -- every other extra_params
#            key keeps its existing top-level setdefault, byte-for-byte
#            unchanged. 4 new regression tests.
# 0.9.123 -- _stage_out_env's expect_bundle_overrides now accepts str | list[str],
#            matching ParcaOptions.bundle_overrides (#486). Real gap: a caller
#            reading config.parca_options.bundle_overrides directly (not through
#            strain_from_config's own _norm-based string normalization) could
#            hand this helper a raw list, crashing `bo = (expect_bundle_overrides
#            or "").strip()` with AttributeError: 'list' object has no attribute
#            'strip'. Caught live firing a real K4/J3 chassis rebuild whose
#            recipe stacks two --bundle-overrides files (item 106) via
#            _submit_mnp's own comparison-ensemble path. Widened the two
#            call-through wrappers (_submit_mnp, _submit_container) to match;
#            left submit_chain_generation/_batch's own str-only signature
#            unchanged -- their real caller (JobScheduler, via strain_from_config)
#            already normalizes to a string before this. 2 new regression tests.
# 0.9.122 -- (undocumented at authoring time; no changelog entry found for this
#            bump in this file's own history)
# 0.9.124 — lenient simulation-list: tolerate a legacy stored config (strip
#            extra-forbidden parca_options keys) instead of 500ing the whole
#            GET /api/v1/simulations; strict creation + per-id detail preserved
# 0.9.125 -- POST /parca/variant-cache (backlog item 451): the native-gene
#            sibling of /parca/new-gene-cache, for Run 4's second required
#            config (fss_pathway_oe_native_oe_carina.json's own native-gene
#            design screen). Mirrors the new-gene-cache mechanism 1:1 --
#            VariantCacheRequest/VariantCacheJob models, _build_variant_
#            cache_command/submit_variant_cache_job (runs sms-ecoli#288's
#            new scripts/build_variant_cache.py against an already-staged
#            commit cache, no fresh ParCa run needed), run_variant_cache
#            handler (same Ray-only gate, same parca_dataset_id -> commit
#            resolution). 8 new regression tests, matching new-gene-cache's
#            own command-builder x3 / submit x1 / handler x4 coverage
#            precedent.
# 0.9.126 -- stage_private_fork/vecoli_private_commit (Run 3's Dispatch 580
#            blocker): every simulator built via the standard Ray DooD path
#            has always staged the PUBLIC vEcoli mirror as its own wrapped
#            /app/vEcoli, never vEcoli-private, regardless of pinned commit
#            -- _build_command never passed -s to docker/build-and-push-
#            ecr.sh, so the default investigation.yaml's own commit-less
#            comparison.reference always fell through to the Dockerfile's
#            public default. New opt-in _build_command/_run_build/
#            submit_build_image_job param generates the fork-staging spec
#            INLINE (a heredoc, no checked-in file to go stale) and reuses
#            the outer clone's own PAT via the recipe's existing --secret
#            path (vEcoli-private is private, same org). vecoli_private_
#            commit is REQUIRED when set True -- no "latest" auto-
#            resolution, so the exact commit staged is always an explicit,
#            visible choice, not another silent moving target. Wired end to
#            end: /simulator/upload route -> upload_simulator handler
#            (same reflective inspect.signature dispatch + fail-loud 400
#            pattern as include_submit_image) -> service methods, plus
#            app_data_service.py + `atlantis simulator latest
#            --stage-private-fork/--vecoli-private-commit`. 13 new
#            regression tests (build-command script content, handler
#            dispatch/fail-loud, CLI wiring).
# 0.9.127 -- fix: run_pbg.py's _redirect_emitters() now routes an xarray/zarr
#            emitter (out_uri) straight to RAY_OUT_S3 instead of the shared
#            local results_dir every other file-backed emitter uses (real
#            root cause of CD2 Dispatch 665/666, both failing around
#            generation 3-4 on a multi-hour lineage_ray_batch/mbp_dispatch
#            run). The local-then-periodic-best-effort-sync path
#            (ray-batch-entrypoint.sh's start_output_sync, "never fails the
#            job on its own") is safe for parquet's independent chunk files
#            but not for zarr: viva_emitters.xarray_emitter.zarr_writer.
#            _check_group requires the PREVIOUS generation's own group to
#            still exist in the SAME store, and this process's own
#            filesystem is not authoritative on a multi-node run -- Ray
#            places ray:-addressed actors wherever it likes (viva-api#419,
#            already documented by this file's own _assert_emitted_output).
#            If the actor owning a seed's lineage gets restarted on a
#            different node between generations, the new node's local disk
#            never had the previous generation's zarr group, and
#            _check_group fails loudly. Confirmed via a local, no-AWS
#            reproduction (XArrayEmitter driven through 8 successive
#            generations against a persistent local store) that the
#            emitter's own generation-boundary logic is otherwise correct --
#            this is a dispatch-infrastructure gap, not a pbg-emitters bug.
#            Explains the depth correlation exactly: Run 4's short
#            n_generations=2 dispatches (83 real successes) never run long
#            enough to hit a real actor restart; K4/J3's 4-8 generation
#            dispatches run for hours. v2ecoli's own _open_xarray_emitter
#            already has out_is_s3-aware branching -- this is the first time
#            it's ever actually exercised. Falls back to the existing local
#            redirect when RAY_OUT_S3 is unset (local/non-Batch dev
#            contexts unaffected). Every other file-backed emitter
#            (ParquetEmitter, SQLiteEmitter, ...) is completely unaffected --
#            confirmed by a dedicated regression test that RAY_OUT_S3 being
#            set does not change parquet's own redirect target. 3 new tests.
# 0.9.128 -- fix(run_pbg): (C) `--experiment-id` is passed by
#            _multi_node_composite_command and injected into the overrides
#            iff the composite declares experiment_id (to_document raises on
#            undeclared keys, so only the container can decide) -- every
#            lineage MNP campaign no longer lands under the literal hive key
#            experiment_id=lineage_ray_batch (sms-ecoli#166). (B) a composite
#            that declares n_generations is REFUSED when -n < n_generations x
#            max_duration_per_gen -- the under-run hole the API-side clamp
#            (87e5ca04) cannot see when the request omits n_generations too.
#            viva-api#525. Also #514 (/status short-circuit), #515 (e2e opt-in).
# 0.9.129 -- fix(run_pbg): _check_required_run_interval (0.9.128/#525) now
#            exempts composites whose merged overrides carry
#            stop_at_division=True (CD2 Run 3, chain-dispatch, sms-ecoli#166,
#            Dispatch 720/721). Chain-dispatch's own per-generation job
#            submission (_seed_generation_command) always hardcodes -n 1
#            deliberately -- stop_at_division makes LineageProcess advance to
#            a real division INTERNALLY regardless of the nominal step count,
#            so steps there is a "go" signal, not the simulated-time budget
#            the n_generations x max_duration_per_gen contract assumes (true
#            for MNP's one-continuous-invocation lineage_ray_batch, not for
#            chain-dispatch's per-generation-job shape). stop_at_division is
#            exactly the signal distinguishing this from Dispatch 438's own
#            real failure shape (no n_generations, no steps, and no
#            stop_at_division either, so nothing makes the run advance) --
#            checking it here cannot reopen that bug. 4 new/extended tests.
#           0.9.129 -- fix: _assert_emitted_output now also cross-checks each
#            file-backed emitter's own pre-redirect S3 location, not just
#            RAY_OUT_S3. Found on real infra: Dispatch 727:Run 3 seed0
#            (2026-09-09) genuinely succeeded -- a real division, a real
#            checkpoint, ~360MB of real parquet history landed in S3 -- yet
#            was reported a hard failure. Root cause: a LineageProcess-driven
#            chain-dispatch composite builds its OWN per-generation parquet
#            emitter independently (lineage.py's _build_generation reads its
#            OWN config["out_dir"], a plain value on a local:LineageProcess
#            node _redirect_emitters never touches, since that address has no
#            "emitter" in it) and can keep writing straight to the ORIGINAL
#            pre-redirect S3 destination -- verified internally by
#            LineageProcess's own _assert_generation_emitted/
#            _assert_history_landed -- while this process's local results_dir
#            stayed empty. RAY_OUT_S3 doesn't cover this: it's MNP-only, never
#            set for chain-dispatch's single-node-per-generation jobs, so
#            there was previously no cross-check available for this shape at
#            all. _redirect_emitters now returns (count, original_s3_
#            locations) instead of a bare count; _assert_emitted_output polls
#            every candidate (RAY_OUT_S3 plus each redirected emitter's own
#            pre-redirect s3:// location) under the same shared deadline
#            before failing. 8 new/extended tests.
#           0.9.130 -- fix: _lineage_generation_duration_total now also reads
#            BatchBaselineRunner's own batch.wall_s, not just
#            summary.generations[].duration. Found on real infra firing 0.9.130
#            itself: Dispatch 736:Run 3 seed0 -- the emit-gate fix (above)
#            worked (zero trace of the old error), but execution then reached
#            a DIFFERENT, previously-masked gate (_assert_run_advanced /
#            PBG_MIN_GLOBAL_TIME, the "one-tick collapse" detector) and STILL
#            failed: "the run advanced only 1.0 of simulated time" despite a
#            real division at t=2528s. mecillinam_wellmixed.json's actual top-
#            level process is local:v2ecoli.steps.batch_baseline_runner.
#            BatchBaselineRunner, which reports its real elapsed time as
#            {"batch": {"wall_s": ...}} -- a different shape from
#            LineageProcess's own direct summary.generations return, which the
#            walk never matched, so this path always fell back to the outer
#            composite's own misleading global_time=1.0. Never visible before
#            0.9.130 itself, since _assert_emitted_output always failed FIRST
#            for this exact dispatch shape. 4 new/extended tests.
#           0.9.132 -- fix: _submit_multi_node_composite now stages each
#            seed_overrides[*].cache_dir S3 prefix (server-side copy under the
#            dispatch's own cache_s3), then rewrites the override to the local
#            path it resolves to via the existing stage_s3->stage_dir sync.
#            Real bug (backlog item 106): seed_overrides[*].cache_dir was a raw
#            s3:// URI passed straight through to LineageProcess.config
#            ["cache_dir"] unmodified -- v2ecoli's read_cache_version does a
#            plain os.path.exists() on it, unconditionally False for an s3://
#            string regardless of whether the real object exists, raising
#            StaleCacheError. Confirmed on two real dispatches (database_id
#            733/738), each failing on a different seed (Ray's own
#            non-deterministic task ordering). No new env var, no entrypoint
#            change, no image rebuild -- reuses the existing recursive sync.
#            5 new/extended tests.
#           0.9.135 -- feat: chain dispatch runs each seed's WHOLE lineage in
#            ONE LineageProcess (all generations), not one Batch job per
#            generation. The per-generation chain (_seed_generation_command,
#            n_generations=1 + S3 daughter-state checkpoint per job) made every
#            generation a FRESH LineageProcess whose lineage_time_offset restarts
#            at 0.0, so a field_timeline dose scheduled at a cumulative-lineage
#            time (Run 3's DOSE_ONSET_TIME_S=10000, ~gen 4) NEVER fired -- every
#            chain sweep was a silent no-dose control (sim186 confirmed: zero
#            drug, identical FBA control-vs-dose across all 36 combos). Now
#            _advance_parca_gate fans out ONE submit_chain_lineage job per seed
#            (new _seed_lineage_command: n_generations=N, no
#            initial_generation_index/daughter-state/stop_at_division) and
#            _advance_seed_generations polls each lineage job to resolution (no
#            per-generation follow-up submission -- division is in-process, which
#            is exactly what makes lineage_time_offset accumulate so the dose
#            fires). Per-seed async independence preserved; parity with the
#            Nextflow path. See docs/design-chain-one-lineageprocess.md.
#            New TestSeedLineageCommand + updated TestAdvanceChainCampaign.
#           0.9.138 -- fix(mbp-dispatch): thread --aeration-trigger through the
#            remote coupled dispatch (#621, AlexPatrie). _mbp_tracked_command
#            built --reactor-config/--aeration-schedule from mbp_dispatch but
#            never --aeration-trigger, which run_mbp_tracked.py REQUIRES
#            alongside a schedule (load_aeration_schedule() exits nonzero
#            without it). Invisible until sms-ecoli#334's kLa-350 recalibration
#            made a SECOND aeration schedule the first to be fired remotely.
#            Emitted only when aeration_schedule is also set, mirroring the
#            local script's coupling. Deployed for the coupled Run 1 re-fire on
#            simulator 199 (sms-ecoli#166).
#           0.9.139 -- feat(observability): the engine/runner/dispatcher event
#            stack lands (plan PR-A #609 + PR-D #612). Nextflow head poller with
#            trace-decides-status, error-source precedence, PBG identity env on
#            all three dispatch paths, the hpcrun_event/hpcrun_span tables and
#            their ingester, GET /simulations/{id}/events and /tasks, richer
#            /status (stage, generation, last_event_at, attempt, exit_code), and
#            `atlantis simulation events|tasks`. Events are OFF by default -- no
#            sink resolves unless PBG_EVENT_SINKS is set -- so this deploys inert
#            and a path is enabled deliberately.
#            TWO migrations, not one: a3b5c7d9e1f2 (observability columns +
#            event/span tables) and e3a9c1d70b62 (hpcrun_event.layer ->
#            component). The rename MUST be its own revision: a site stamped at
#            a3b5c7d9e1f2 by a #609-era deploy is MANAGED, so `upgrade head` is a
#            no-op and a rename living inside that revision could never run --
#            UndefinedColumn on every event insert, silently, with the migration
#            Job exiting 0. Run the alembic-migrate Job BEFORE rolling the app.
#            Also carries #636's fix to d7e2f4a6c8b0 (double CREATE TYPE +
#            non-idempotent CREATE TABLE), which no database could apply.
#           0.9.140 -- fix(events): two Ray dispatch paths never injected PBG_*,
#            so their runs emitted nothing (#641). Found by dispatching a real
#            1-gen/1-seed campaign on simulator 207 against 0.9.139 and reading
#            the submitted job back out of AWS Batch: 12 env vars on the sim
#            node, 8 on ParCa, no PBG_* on either. submit_ecoli_simulation_job
#            (MNP ParCa + sim) and _submit_analysis_job (the gather) both passed
#            a bare resolve_task_env() with no with_events_env() wrapper; the
#            other four dispatch methods were always correct, which is why code
#            review never surfaced it. The gather's identity now threads
#            correlation_id from the scheduler's campaign HpcRun so it is a
#            sibling of its seeds rather than a trace of its own.
#            CORRECTS TWO CLAIMS IN 0.9.139's OWN ENTRY ABOVE:
#              * "PBG identity env on all three dispatch paths" -- it was on four
#                of six; the MNP ParCa/sim pair and the gather had none.
#              * "Events are OFF by default -- no sink resolves unless
#                PBG_EVENT_SINKS is set" -- misleading. events_env ALWAYS adds
#                the stdout sink, and events_s3_prefix() falls back to deriving
#                s3://<S3_WORK_BUCKET>/<work_prefix>/{experiment_id}/events/ when
#                EVENTS_S3_PREFIX is empty. S3_WORK_BUCKET is set on both
#                Stanford sites, so the S3 sink is ON there. EVENTS_ENABLED=false
#                is the actual off switch.
#            Adds tests/simulation/test_dispatch_events_identity.py, an AST scan
#            asserting every resolve_task_env() result reaches with_events_env().
#            Code-only: NO new migration, DB stays at e3a9c1d70b62.
__version__ = "0.9.140"
#           0.9.101 -- _submit_mnp now sets RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1
#           on every node of every Ray MNP submission. Found: a single-node
#           lineage_ray_batch diagnostic (database_id=344, 2026-09-05) died in
#           raylet bootstrap before any application code ran -- the plasma
#           object store's default request (~10.2GB) exceeded the container's
#           /dev/shm (~9.66GB available). Ray's own documented fallback (disk-
#           backed instead of a hard error); zero behavior change on any node
#           where shm is already sufficient.
#           0.9.100 -- fix(comparison-ensemble): thread cache_variant through
#           the composite-comparison dispatch path (Run 4's genotype fan-out
#           was never reachable with real genotype content via any remote
#           dispatch path -- only ever run locally against a manually-selected
#           cache). #430.
#           0.9.98 -- _submit_multi_node_composite (the pbg-native/lineage_
#           ray_batch dispatch path) never had cache_variant support at all --
#           only chain-dispatch did. Found firing the first-ever real
#           strain-specific pbg-native dispatch, 2026-09-04: without it, a
#           multi_node_dispatch pointed at a real derived cache (POST
#           /parca/new-gene-cache) would silently stage the generic
#           per-commit default instead. Mirrors chain-dispatch's own already-
#           proven job_scheduler.py pattern. Omitted preserves today's
#           behavior byte-for-byte.
#           0.9.97 -- orphaned job polling (viva-api#414): a LOCAL-backend HpcRun
#           row (an in-process asyncio task polling a DooD image build on AWS
#           Batch, or the chain-dispatch placeholder) stayed `running` forever
#           once the api pod that owned the task was replaced, while the Batch
#           job finished normally -- measured live 2026-09-04 (hpcrun 506,
#           v2ecoli-ray-build-10ebc4c SUCCEEDED 6 min after the 0.9.95->0.9.96
#           rollout; dispatch refused "build is still in progress"; recovery
#           was a redundant 10-min rebuild). Three changes: (1) the build task
#           persists its Batch job id(s) onto the row (new hpcrun.external_job_ids,
#           migration c7d1f3a9b2e4 + fingerprint marker) so the work is
#           addressable from any process; (2) LocalTaskService binds a task to
#           its row (bind_hpcrun) and finalizes the row from the task's own
#           outcome, with end_time -- the chain-dispatch placeholder is bound
#           too, so a submission crash now reaches the DB instead of only this
#           process's memory; (3) JobScheduler.reconcile_local_tasks runs FIRST
#           on every poll tick (so at startup): every active LOCAL row this
#           process does not own is finished from Batch truth (describe_jobs
#           by persisted id, or by the deterministic build job name for rows
#           that predate the column), SUCCEEDED->completed, FAILED->failed with
#           the reason, still-running left alone; a placeholder superseded by
#           its real campaign row is completed, one with no successor after a
#           10-min grace window is failed and says to re-submit. Stateless by
#           construction -- nothing is re-attached, so it does not matter how
#           many polling events were missed.
#           0.9.96 -- run_new_gene_cache resolved its service via
#           get_simulation_service() (the deployment's own COMPUTE_BACKEND
#           default -- "batch"/Nextflow on sms-api-stanford-test), not Ray --
#           501'd on this endpoint's own first-ever real call, 2026-09-04,
#           on a deployment that fires real Ray/Batch MNP jobs successfully
#           through every other route (those resolve via
#           get_simulation_service_for_repo, commit/repo-aware). Now asks
#           for ComputeBackend.RAY by name, matching what this handler's own
#           docstring always said it wanted. New regression test is the
#           first one in its class to actually exercise resolution instead
#           of injecting a mock service directly.
#           0.9.95 -- _parca_command's own build_cache.py step no longer carries
#           --new-genes/--bundle-overrides -- confirmed live 2026-09-04, its real
#           current CLI has neither flag (`unrecognized arguments`), stalling ANY
#           new_genes ParCa dispatch one step after ParCa itself succeeds. Not a
#           regression to work around: build_cache.py's own save_sim_input already
#           writes a complete, correct cache_version.json straight from sim_data,
#           itself already strain-specific since v2ecoli-parca received both flags
#           one command earlier in the same chain. Restamping here was redundant
#           even when it was once supported.
#           0.9.110 -- chain-dispatch's own per-seed generation submission
#           (_seed_generation_command / submit_chain_generation /
#           submit_chain_generation_batch) never threaded exchange_fluxes/
#           exchange_flux_basis at all (backlog item 105, the K4 cell-only
#           ensemble) -- only pbg-native's _submit_multi_node_composite
#           (item 106) had them. A chain-dispatch config relying on the
#           ExchangeFluxListener for a real product-flux measurement (e.g.
#           the K4 cell-only ensemble's founder caches, which cannot use
#           pbg-native without breaking cache-pin compatibility) silently
#           wrote no listeners__exchange_flux__* columns at all, with no
#           refusal -- the exact "green, healthy, but scientifically empty"
#           failure mode cplong90's own team documented precisely in
#           run1_k4_cellonly.json's _provenance.exchange_flux_is_a_dispatch_
#           flag. Two new optional params, re-derived from
#           Simulation.config every JobScheduler tick (same restart-safe
#           pattern as composite_id/cache_variant, item 105a/b) at both the
#           generation-0 fanout and the per-generation advance call sites.
#           Omitted (every existing caller) preserves today's behavior
#           byte-for-byte. 6 new regression tests, matching the
#           composite_id fix's own command-builder x2 / per-seed-submit x2
#           / scheduler x2 coverage precedent.
