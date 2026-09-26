# viva-core at UConn PROD (`sms-api-rke`, `sms.cam.uchc.edu`)

Core as **its own Deployment beside the live SMS `api` pod (0.10.0-rc1)** — plan-core §4b **U4**;
D19 own database, D20 served beside, D21 the SMS overlays frozen. Served at
`https://sms.cam.uchc.edu/viva/v1` and `/env-worker` through a second Ingress; the frozen
`api-ingress` (which routes `/`, `/api`, `/core`, `/compose`, … to `api` and PTools) and the rc1
pod are untouched. Everything here needs the UConn VPN and **an explicit go before `apply`** —
this is the production host.

The shared pieces are `kustomize/base/viva-core/` (Deployment, Service, Ingress); this overlay
adds the namespace, the image tag, the config, the host, and **core's own claim on
`cfs15:/projects`** — prod's `vivarium-home-pv` is `cfs09:/home/FCAM`, the home directory, which
does not contain `/projects/SMS`; the dev overlay (U3) reuses dev's claim, which already is
`cfs15:/projects`.

## Reused by name from the live SMS overlay (never redeclared here)

| object | kind | why core needs it |
|---|---|---|
| `shared-secrets` | Secret | Postgres user / password / host / port (`sms` on `sms-postgres-cluster-rw`); the database name is overridden to `viva_core` |
| `ssh-secret` | Secret | the `svc_vivarium` key for Mantis |
| `batch-submit` | ServiceAccount + Role | env-worker and build Jobs (prod HAS it; dev did not) |
| `letsencrypt-prod-sms-api-tls` | Secret | the host's certificate |
| `haproxy-ssh` | Service | the SSH round-robin to `mantis-sub-*` |

**Not reused: `ssh-known-hosts`.** Prod's spells the host `[haproxy-ssh]:22`; asyncssh looks a
default-port host up by its bare name and never by `[host]:22`, so the core pod's first build failed
with `Host key is not trusted for host haproxy-ssh` (UC, 2026-09-26; the SMS pod never noticed — its
`SLURM_SUBMIT_KNOWN_HOSTS` is commented out). Core mounts its own `core-ssh-known-hosts` (same key,
bare spelling, `core-ssh-known-hosts.yaml`); the SMS ConfigMap is frozen (D21) and untouched.

Read-only pass, 2026-09-26: all six present; `api-ingress` claims nothing under `/viva` or
`/env-worker`; `sms-postgres-cluster` 3/3 healthy; the live api pod runs as 17163/10000 with
partition `vcell`, QoS `vcell-services`, submit host `haproxy-ssh`.

## Before the first apply (in this order)

1. **The database**: `kubectl apply -f kustomize/cluster/postgres-cluster/vxrails/viva-core-database.yaml`,
   then `kubectl -n postgres-cluster get databases.postgresql.cnpg.io viva-core-prod` → `Applied: True`.
2. **The HPC tree**: `/projects/SMS` is setgid group-writable and the DEV api pod mounts that very
   export (`cfs15:/projects`), so make prod's tree from there:
   `kubectl -n sms-api-rke-dev exec deploy/api -- mkdir -p /projects/SMS/viva_core/prod/htclogs /projects/SMS/viva_core/prod/sbatch /projects/SMS/viva_core/prod/compose/images /projects/SMS/viva_core/prod/compose/sims`
3. **Qumulo credentials** (optional): seal `core-qumulo-secrets` (`access-key-id`, `secret-access-key`)
   into `sms-api-rke` and add it to `resources:`; without it the pod runs and dataset streaming is off.
4. **The image**: `ghcr.io/vivarium-collective/viva-core:<tag>` — public, pulled anonymously.

## Apply and check (checkpoint UC)

```bash
export KUBECONFIG=~/.kube/kubeconfig_vxrails.yaml
kubectl kustomize kustomize/overlays/viva-core-rke                  # read it first: 6 objects
kubectl apply -k kustomize/overlays/viva-core-rke
kubectl -n sms-api-rke rollout status deployment/core

curl -s https://sms.cam.uchc.edu/viva/v1/health                     # compose / environments / workers true
curl -s -o /dev/null -w '%{http_code}\n' -X POST https://sms.cam.uchc.edu/env-worker/v1/relay/workers/nope/call   # JSON 404
uv run atlantis smoke run --base-url https://sms.cam.uchc.edu --only core --only relay --only compose
curl -s https://sms.cam.uchc.edu/version                            # still "0.10.0-rc1": the SMS side untouched
```

## Rollback

`kubectl delete -k kustomize/overlays/viva-core-rke` removes only what this overlay declares (the
PV is `Retain`); the SMS pod, its Ingress and the database are unaffected.
