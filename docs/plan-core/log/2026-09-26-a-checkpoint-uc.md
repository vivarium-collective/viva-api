- **2026-09-26** — **Checkpoint UC passed: core is standalone at `sms.cam.uchc.edu` — U4 done, the end goal of §4b reached.**
  `atlantis smoke run --base-url https://sms.cam.uchc.edu --only core --only relay --only compose`
  → **3 passed, 0 failed**: `compose 2: level 1.61051 = 1.1^5` in 113.7 s, the same path UB proved on
  dev (K8s build Job → sbatch on `vcell` → `results.zip` over SSH), now beside the live `0.10.0-rc1`
  api pod, whose `/version` still answers rc1 and whose Ingress is untouched. Applied in order,
  under Jim's go for U4 (#833): the Database CR `viva-core-prod` (`viva_core` on
  `sms-postgres-cluster`, owner `sms`), the HPC tree `/projects/SMS/viva_core/prod/…` made from the
  dev api pod (prod's own `vivarium-home-pv` is `cfs09:/home/FCAM`, not `/projects`), core's own
  PV/PVC on `cfs15:/projects`, the overlay (7 objects) — pod `core-5556bb7d84-kckjn`, `viva-core:0.1.7`.
  **The one defect UC found (#835):** every SSH session failed with `Host key is not trusted for host
  haproxy-ssh`. Prod's and dev's `ssh-known-hosts` hold the **same** ed25519 key, but prod spells the
  host `[haproxy-ssh]:22` and asyncssh looks a default-port host up by its bare name only — the SMS
  pod never noticed because prod's `SLURM_SUBMIT_KNOWN_HOSTS` is commented out. The SMS ConfigMap is
  frozen (D21), so core's prod overlay carries its own `core-ssh-known-hosts` (bare spelling), and
  `tests/test_deploy_config.py` holds every core overlay to a bare `SLURM_SUBMIT_HOST` line. The
  risk table's "known_hosts spelling differs prod vs dev" was this. **Still open from UB, unchanged:**
  the env worker from a laptop (an environment-ref question for P5). Next on the UConn track: U5,
  the SMS follow-on, after the Stanford removals settle.
