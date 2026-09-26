- **2026-09-26** — **Checkpoint UB, the composite: a process-bigraph composite runs on Mantis through the standalone core at UConn dev, driven by `atlantis`.**
  `atlantis smoke run --base-url https://sms-dev.cam.uchc.edu --only core --only relay --only compose`
  → **3 passed, 0 failed**: `compose 9: level 1.61051 = 1.1^5` in 113 s. The path, end to end and
  every step measured on the cluster: the resolver (#811) sends the client to `/viva/v1/compose`;
  core (`viva-core:0.1.7`, its own database `viva_core` on `sms-dev-postgres-cluster`) records the
  run and, because the definition is new, builds its container in a **Kubernetes Job**
  (`singularity-build-5661a-7e3df`, 54 s: a privileged init container builds into scratch, the
  unprivileged main container copies the image onto `/projects/SMS/viva_core/dev/compose/images`
  as the service user); the monitor closes the build row over the Job's condition; the dispatch
  submits the run as an sbatch on the `vcell` partition; the node runs the image, `run_pbg.py`
  writes `emitter_history.json` + `final_state.json`, the job zips `results.zip`; the monitor
  closes the run over `squeue`/`scontrol` (SLURM job 3394065 COMPLETED); the client downloads the
  archive over SSH through core and checks the value. The SMS `api` pod beside it is untouched.
  **What nine iterations found and fixed on the way** (each its own PR, each proven on the next
  run): the build row untagged (#810), a stale/FAILED build suppressing every rebuild (#815, #717),
  the Job name's case (#816), the Job's tz-aware times (#817), the runner's step count as a float
  (#826), the input uploaded as `.omex` whatever its type (#828), `requests` missing from the
  definition (#831) — and the site facts: no subuid for `svc_vivarium` on the nodes (hence the Job
  build, #814), the QoS the account may use (#809 and its correction), a dead `ghcr-secret` PAT
  (the image is public and pulled anonymously). Core went 0.1.0 → 0.1.7, one write-once tag per
  fix. **Still open in UB:** the env worker from a laptop — the relay's `commit` is a git sha of
  a science image, and UConn has no science image on ghcr; that is an environment-ref question
  for P5 (D10), not a defect to fix here. Then U4: the same overlay at `sms.cam.uchc.edu`.
