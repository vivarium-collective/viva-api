# Deploying: versions, image tags, and the couplings that bite

**Written 2026-08-28.** Consolidates what was scattered across `CLAUDE.md`
(Release Protocol, version-sync checklist, Stanford-test deploy loop, Pitfalls
1–6), inline comments in the overlays, and four build scripts. It is a
*description of current practice*, and every claim here was checked against the
overlays and a live cluster on that date — but expect it to drift, and prefer the
overlay comments where they disagree, since those sit next to the value.

There is **no deploy skill**; this file is the closest thing.

---

## 1. Three images, three different versioning schemes

This is the part that most often goes wrong, because the schemes are unrelated.

| image | version source | who builds it | scheme |
|---|---|---|---|
| `sms-api` | `viva_api/version.py` + `pyproject.toml` | CI — `build-and-push.yml` (`scripts/build_action.sh` builds **api only**) | semver, bumped per release |
| `vivarium-workbench` | that repo's `pyproject.toml` | its own `build-and-push.yml`, **manual `workflow_dispatch`** | **independent line** — unrelated to sms-api's |
| `sms-ptools` | *the sms-api version line*, sampled at build time | **by hand** — `kustomize/scripts/build_and_push.sh`; CI never builds it | follows sms-api, and **lags** |

**`vivarium-workbench` versions independently.** Nothing ties `0.3.x` to sms-api's
`0.9.x`. Its workflow deliberately has no Release trigger — a Release is
documentation; the image comes from the manual dispatch (a v0.3.38 auto-run once
shipped a confirmed-broken tag, which is why). So: run the workflow, then pin the
tag in the overlay.

**`sms-ptools` follows sms-api's line but lags it.** It is tagged with whatever
`version.py` read *when it was last rebuilt by hand*, so a gap between it and
`sms-api` is normal and expected, not drift. **Never bump the ptools tag without
actually building and pushing that image** — there may be nothing behind it.

> **Correction.** `CLAUDE.md`'s deploy loop said "ptools is intentionally pinned to
> 0.5.9 (no newer sms-ptools image on ghcr.io)". That is stale, and contradicted
> its own version-sync checklist. Actual state on 2026-08-28:
> **Stanford sites `0.9.53`; UConn RKE overlays `0.5.9`.**

## 2. The coupling that is invisible in a diff

**`vivarium-workbench`'s `newTag` and `ENV_WORKER_MODULE_IMAGE` must be equal.**

That one image is both the workbench that launches env workers *and* the image a
worker Job stages its worker module from. A mismatch is a workbench speaking a
protocol its own workers do not.

They live in **different files**:

| value | file |
|---|---|
| `newTag` | `kustomize/overlays/<ns>/kustomization.yaml` |
| `ENV_WORKER_MODULE_IMAGE` | `kustomize/config/<ns>/shared.env` |

`shared.env` feeds a **hash-suffixed ConfigMap**. So applying from a tree whose
`shared.env` is older silently regenerates that ConfigMap at the older value and
re-points the api Deployment at it — **and nothing in the `kustomization.yaml`
diff shows it.**

Both failure directions were observed on 2026-08-27/28:

- applying from a branch that *lacked* the deploy bump reverted **both** tags to
  an older version;
- applying a PR that bumped only `kustomization.yaml`, from a branch predating the
  `shared.env` change, left the cluster at
  `workbench 0.3.65` with `ENV_WORKER_MODULE_IMAGE 0.3.64`.

**Rules that follow:**

1. **Rebase on `main` before applying.** The coupled value may live in a file your
   change does not touch.
2. **Verify against live objects, not the apply output.** `kubectl apply` prints
   `configured` either way:
   ```bash
   kubectl get deployment workbench -n <ns> \
     -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
   APOD=$(kubectl get pods -n <ns> -o name | grep '^pod/api-' | head -1 | cut -d/ -f2)
   kubectl exec -n <ns> $APOD -- printenv ENV_WORKER_MODULE_IMAGE
   ```
3. **Prefer a new tag to reusing one** — it is the unambiguous signal that new bits
   must exist, and removes every "did the rollout actually pull?" question.

Only `sms-api-stanford-test` sets these today.

## 2b. What the ALB routes (and what silently doesn't)

The internal ALB forwards by **path prefix**, and its **default action forwards to
PTools**. So a viva-api path with no listener rule does not 404 from sms-api — it
reaches *a different application*, which answers with its own 404 page. That is
how `POST /compose/v1/simulation/run` appeared broken through the tunnel while
working perfectly in-cluster (2026-08-28).

Routed to the **api** target group: `/openapi.json`, `/home`, `/docs`, `/ws`,
`/api`, `/core`, `/health`, `/version`, **`/compose`**, **`/env-worker`**.
Routed to the **workbench** target group: `/workbench`, `/bigraph-loom`.
Everything else → PTools.

Rules live in `../sms-cdk/lib/internal-alb-stack.ts` and need **`cdk deploy`** —
a kustomize apply will not touch them.

**A TargetGroupBinding is a different thing and usually does not need changing.**
It binds a *Service* to a target group (`kustomize/overlays/<ns>/target-group-binding.yaml`);
listener rules map *paths* to a target group. Several rules can share one binding —
`/workbench/*` and `/bigraph-loom/*` already do, and `/compose` and `/env-worker`
ride the existing `api` binding. A new binding is needed only when a rule points
at a **new** target group.

**When adding a viva-api router, check it has a rule.** Compare the prefixes in
`/openapi.json` against the list above; `/compose` and `/env-worker` were both
missed for months because the rules predate those routers.

## 3. The loop

```bash
# 1. push FIRST — the GH Action builds the remote branch tip, not your worktree
git push origin <branch>

# 2. build (sms-api; the workbench has its own workflow in its own repo)
gh workflow run build-and-push.yml --ref <branch> -f version=<X.Y.Z>

# 3. bump BOTH coupled values if the workbench moved, then apply
kubectl kustomize kustomize/overlays/<ns> | kubectl apply -f -
kubectl rollout status deployment/api -n <ns>

# 4. verify on the LIVE pod — image, coupled env var, and a marker unique to the fix
kubectl exec -n <ns> $POD -- grep -c <marker> /app/viva_api/<changed-file>.py
```

Tunnel for local access: `../sms-cdk/scripts/sms-proxy.sh -s smsvpctest`
(it idle-times-out; restart as needed).

## 4. Where the rest lives

- **`CLAUDE.md` → Release Protocol** — tagging, GitHub Release, the full
  version-sync checklist across all overlays.
- **`CLAUDE.md` → Pitfalls 1–6** — the GH Action building the remote tip;
  ephemeral-storage eviction; the dead `ingress.yaml`; ALB `Target.Timeout` flakes;
  `port-forward` vs multi-request handlers; and the **60 s ALB idle ceiling**
  (a 504 through the tunnel means nobody was listening, *not* that the server
  failed — it may still be running, and succeeding).
- **Overlay comments** — the authoritative, value-adjacent rationale for each
  pinned tag. When this file and an overlay comment disagree, believe the overlay.
- **`kustomize/scripts/build_and_push.sh`** — the local/by-hand build path,
  including ptools.
- **Database migrations** — `CLAUDE.md` → "Database migrations"; they run through
  the self-diagnosing reconciler, not bare `alembic upgrade head`, and the
  `<ns>-db-migration` overlay's tag must contain the new migration.
