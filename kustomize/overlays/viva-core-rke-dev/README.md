# viva-core at UConn dev (`sms-api-rke-dev`)

Core as **its own Deployment beside the SMS `api` pod** (plan-core §4b **U3**; D19 own
database, D20 served beside, D21 the SMS overlays frozen). Served at
`https://sms-dev.cam.uchc.edu/viva/v1` and `/env-worker` through a second Ingress; the
frozen `api-ingress` and the rc1 `api` pod are untouched. Everything here needs the UConn
VPN (`KUBECONFIG=~/.kube/kubeconfig_vxrails.yaml`) and an explicit go before `apply`.

## What this overlay declares — and what it only refers to

Declared here: Deployment `core`, Service `core`, ConfigMap `core-config`
(`kustomize/config/viva-core-rke-dev/core.env`), Ingress `core-ingress`.

Referred to **by name**, because the live SMS overlay already provides them in the
namespace and this overlay must never redeclare (and so overwrite) them:

| object | kind | why core needs it |
|---|---|---|
| `shared-secrets` | SealedSecret | Postgres user / password / host / port (the database name is overridden to `viva_core`) |
| `ssh-secret` | SealedSecret | the `svc_vivarium` key for Mantis |
| `ssh-known-hosts` | ConfigMap | the submit host's key |
| `ghcr-secret` | SealedSecret | image pulls |
| `vivarium-home-pvc` | PVC (NFS) | the HPC filesystem at `/projects/SMS` |
| `haproxy-ssh` | Service | the SSH round-robin to `mantis-sub-*` |
| `batch-submit` | ServiceAccount + Role | env-worker Jobs |

## Before the first apply

1. **The database** (D19): `kubectl apply -f
   kustomize/cluster/postgres-cluster/vxrails-dev/viva-core-database.yaml` — a CNPG
   `Database` CR: database `viva_core` on `sms-dev-postgres-cluster`, owned by the existing
   `sms-dev` role. The cluster had one instance crash-looping (2026-09-25); fix that first.
2. **The HPC tree**: as `svc_vivarium`, `mkdir -p /projects/SMS/viva_core/dev/{htclogs,sbatch,compose/images,compose/sims}`.
3. **Qumulo credentials** (optional; without them the pod runs and datasets / results
   streaming are off): seal a Secret `core-qumulo-secrets` with keys `access-key-id` and
   `secret-access-key` (`kustomize/scripts/sealed_secret_*.sh` pattern, controller
   `sealed-secrets-controller` in `kube-system`) and add it to `resources:`.
4. **The image**: `ghcr.io/vivarium-collective/viva-core:0.1.0` must exist (built by
   `build-core.yml`, write-once) and be pullable with `ghcr-secret`.

## Apply and check (checkpoint UB)

```bash
export KUBECONFIG=~/.kube/kubeconfig_vxrails.yaml
kubectl kustomize kustomize/overlays/viva-core-rke-dev            # read it first
kubectl apply -k kustomize/overlays/viva-core-rke-dev
kubectl -n sms-api-rke-dev rollout status deployment/core

curl -s https://sms-dev.cam.uchc.edu/viva/v1/health                # JSON: compose true
curl -s https://sms-dev.cam.uchc.edu/viva/v1/capabilities           # viva-v1-surface
curl -s -o /dev/null -w '%{http_code}\n' https://sms-dev.cam.uchc.edu/env-worker/v1/relay/workers/nope/call   # 404 JSON = routed
uv run atlantis smoke run --base-url https://sms-dev.cam.uchc.edu --only core --only contract
```

A JSON 404 on the relay probe proves the Ingress path; an HTML 404 means it fell through to
PTools. Then the compose run and the env worker from a laptop, per plan-core §4b UB.

## Rollback

`kubectl delete -k kustomize/overlays/viva-core-rke-dev` removes only what this overlay
declares; the SMS pod, its Ingress and the database are unaffected.
