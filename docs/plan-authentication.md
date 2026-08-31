# Plan: Authentication for viva-api

**Started 2026-08-30. Phases 0 and 2 landed the same day.** Written after the
identity seam went live on `sms-api-stanford-test` and the question "should we
run Keycloak?" was asked seriously.

This is a *plan*, not a description of current practice — everything in "Where we
are" was verified against the running systems on that date, and everything under
"Open questions" is genuinely open and should not be read as settled.

Companion to [`DEPLOY.md`](DEPLOY.md), which describes what is deployed today.

---

## Status: PHASES 0 AND 2 DONE — BLOCKED ON A DOMAIN

| Phase | State | Shipped |
|---|---|---|
| 0 — CORS | **DONE** ([#336](https://github.com/vivarium-collective/viva-api/issues/336)) | 0.9.74, deployed stanford-test 2026-08-30 |
| 1 — TLS + certificate | **BLOCKED** — no domain chosen | — |
| 2 — Token validation | **DONE** ([#337](https://github.com/vivarium-collective/viva-api/issues/337)) | 0.9.75, deployed **inert** (no issuer set) |
| 3 — A token per client | Not started; needs Phase 1 | — |
| 4 — What is protected | Not started; the large one | — |

**Nothing is enforced.** viva-api still has 79 routes and no route requires a
credential. Phase 2 made an identity *readable* where an issuer is configured; it
did not make one *required*, and no deployment configures an issuer today.

**The critical path now runs through a decision, not through code.** Phases 0 and
2 were the two pieces that did not need a domain, a budget or an owner. Both are
done. Everything remaining waits on question 1 below.

The identity **seam** (`viva_api/api/auth.py`) is live on stanford-test and is
still deliberately not authentication — but since Phase 2 it has two sources, and
the verified one wins. See that module's docstring.

---

## Where we are

Verified 2026-08-30 against the running systems and the CDK sources. Rows are
marked where Phases 0 and 2 changed them.

| Fact | Evidence |
|---|---|
| 79 routes, **zero** security schemes, no per-operation security | `api/spec/openapi_3_1_0_generated.yaml` — **unchanged by Phase 2**, which made identity readable, not required |
| One authorization rule exists: you cannot cancel a task you did not start | `api/routers/env_worker.py`, `api/auth.py` |
| Identity has **two** sources: a verified OIDC token, else a configurable header | `api/oidc.py`, `api/auth.py` — *changed by Phase 2*; the verified source wins |
| No deployment sets `OIDC_ISSUER`, so token validation is **inert everywhere** | the api pod's startup log says so on purpose |
| Env vars bind to the field name uppercased — `IDENTITY_HEADER`, `OIDC_ISSUER` | `config.py` — **no `env_prefix`**, so never `VIVA_API_*` |
| Stanford dev + prod are unreachable without an AWS session | internal ALB, `internetFacing: false`, SSM tunnel only |
| **UConn prod is on the open internet with no auth challenge** | `curl -sI https://sms.cam.uchc.edu/docs` → `HTTP/2 200`, no `www-authenticate` |
| The internal ALB listener is **HTTP:80, no certificate** | `sms-cdk/lib/internal-alb-stack.ts` — **still the blocker** |
| Neither GovCloud config defines a domain, so **no ACM certificate is created** | `config/stanford-vpc-test.json`, `config/stanford.json` — both have no `domain` block |
| ~~CORS is `["*"]` with `allow_credentials=True`~~ → now `APP_ORIGINS` with credentials off | `api/main.py` — *fixed by Phase 0* |

> **Correction, recorded because it was asserted in discussion and was wrong.**
> The ACM certificate machinery in `shared-stack.ts` *does* exist and *is*
> configured — but only in `config/commercial.json`, for `jcschaff.org`, a
> personal AWS account that is no longer used. For both deployments that matter
> there is no domain, no certificate, and no hosted zone.

## The thing that decides the shape

**Auth over plaintext buys attribution without authorization.**

A JWT is a bearer token: whoever observes it can replay it until it expires.
Inside the VPC, the people who can observe that traffic are *exactly the people
the auth exists to distinguish between* — everyone holding an AWS session is
equally able to reach the ALB and equally able to read what crosses it.

Today's model is "reachability equals authorization". Adopting any real auth is a
decision that this is no longer true. If the credential then travels in
cleartext, we have not made it untrue — we have added machinery that produces a
name in a log, which is what the header seam already does for a fraction of the
cost.

**So TLS is not a phase of this work. It is the precondition for the work being
worth doing at all.**

Three narrower reasons point the same way, and any one of them is sufficient:

1. **OIDC Discovery** requires the issuer identifier to be an `https` URL, and
   **OAuth 2.0 §3.2** requires TLS to the token endpoint. Conformant client
   libraries generally *refuse* an `http://` issuer rather than warning.
2. **Keycloak enforces it itself** — `sslRequired` defaults to `external` and
   rejects non-HTTPS from non-private addresses. It can be set to `none`, but
   disabling a guard as step one of a security project is a poor look in the
   commit log.
3. The `AWSELBAuthSessionCookie` that ALB OIDC sets is itself a session
   credential.

## Phase 0 — Fix CORS — **DONE** ([#336](https://github.com/vivarium-collective/viva-api/issues/336), 0.9.74)

`allow_origins=["*"]` with `allow_credentials=True` is rejected by browsers per
spec, so the pair did not mean "permissive" — it meant *credentialed
cross-origin requests fail*, which is a different thing and not what the code
read as.

Now `allow_origins=APP_ORIGINS` (the list that sat defined-and-unused directly
above the middleware) and `allow_credentials=False`.

**What the investigation found**, since the risk was breaking a caller nobody
remembers: there is **no credentialed cross-origin browser caller at all**.
`/docs`, `/documentation` and `/home` are served *by* viva-api and are
same-origin; the marimo GUI's API calls run in the marimo *kernel* (httpx,
server-side), not the browser; the CLI and TUI are not browsers; the workbench UI
is same-origin behind the shared ALB. Ports 4200–4202 are leftovers from an
Angular frontend not in this repo, left in place rather than pruned.

Narrowing also closed one small real hole: a developer with the SSM tunnel open
has an internal API on `localhost`, and `["*"]` let any page they visited read it.

A comment at the call site says what to do when a credentialed browser client
appears — add its origin; **do not widen back to `["*"]`**.

## Phase 1 — TLS on the client-facing hop

The blocker, and the phase most likely to be underestimated. **Neither route
avoids needing a DNS name clients will use and a certificate they trust.**

| Option | TLS terminates | Certificate lives in | Notes |
|---|---|---|---|
| **A.** HTTPS listener on the existing ALB | ALB | ACM | Smallest CDK diff. `InternalAlbStackProps` currently takes only `vpc` and `eksClusterSecurityGroup`, so it needs a `certificate` prop. **Required if we ever want ALB `authenticate-oidc`.** |
| **B.** NLB TCP passthrough → in-cluster ingress | ingress pod | k8s / cert-manager | Moves the cert out of CDK into a k8s object. Rules out ALB `authenticate-oidc` (see below). |

An in-cluster ingress *behind* the current ALB does **not** work: the ALB is
layer 7, so if its listener is HTTP:80 the client→ALB hop is plaintext no matter
what terminates downstream. That is why option B replaces the ALB rather than
sitting behind it.

**Certificate options for an internal-only service:**

- **Public ACM cert + Route53 private hosted zone** — DNS validation happens on
  the public zone; the private zone serves the internal answer. Standard pattern.
  **Requires a domain we control**, which is the open question below.
- **ACM Private CA** — roughly $400/month, plus distributing the CA to every
  client.
- **Internal CA / self-signed** — free, but the CLI, the browser and the
  workbench pod must all trust it, and the failure mode is somebody adding
  `verify=False` and it never coming back out.

> **Ergonomic consequence, worth surfacing before it is discovered late.** Clients
> reach the ALB today as `localhost:8080` through SSM port-forwarding. TLS
> verifies hostnames, so `https://localhost:8080` against a certificate for any
> real name fails. Every developer will need a hosts entry or equivalent. It is
> workable, and it changes the daily loop for everyone.

## Phase 2 — Token validation in viva-api — **DONE** ([#337](https://github.com/vivarium-collective/viva-api/issues/337), 0.9.75)

`viva_api/api/oidc.py`. Validates a bearer token against a configured issuer's
discovery document and JWKS — signature, `iss`, `aud`, `exp`, `nbf` — and hands
the subject to the seam. **`resolve_caller` now has two sources and the verified
one wins**: letting an unverified header override a verified token would make the
header a way to impersonate anyone, which is what token validation exists to stop.

Deployed **inert**. `OIDC_ISSUER` is unset everywhere, so nothing changed; the
startup log says so, deliberately reporting whether validation is *usable* rather
than merely *set*.

**Provider substitutability is delivered.** Because validation is at the
standards layer, Keycloak, Entra, Okta, an ALB OIDC action and a customer's own
provider are interchangeable by construction. Discovery is *fetched*, not
assumed — hardcoding Keycloak's `certs` path would have broken that on the first
deployment that pointed elsewhere.

**Half-configured refuses rather than degrades.** Validation disables itself
entirely, saying why once, when the audience is unset (an issuer-only check
accepts tokens the same IdP minted for a different relying party), when the
algorithm list names `none` or an HMAC algorithm, or when the issuer is not
https. A deployment that set `OIDC_ISSUER` meant to turn something on; failing
loudly beats quietly validating less than it appears to.

**Known trade, stated in the module:** an invalid token is anonymous rather than
a 401, because `resolve_caller` must never break a request. So an expired token
looks like no token. The clients cover it from the other side — `atlantis worker
submit` warns when the server did not record the identity it was given.

> **This did not make anything secure.** A verified token over the current
> HTTP:80 listener is replayable by anyone who can observe the VPC, and no route
> requires one. Shipping it first was a sequencing choice — it was reversible and
> unblocked — not a claim that the API is now protected.

## Phase 3 — Getting a token into each client

| Client | Mechanism | Notes |
|---|---|---|
| `atlantis` CLI, TUI | OAuth **device flow** | The hard part is already solved once in this org: `vivarium_workbench/lib/github_auth.py` has a working `start_device_flow` / `poll_device_flow` / keyring / `gh` fallback. |
| marimo GUI | device flow | A notebook has nowhere natural to catch a redirect. |
| Workbench → viva-api (server-side) | client-credentials grant | A service principal, not a user token. See vivarium-workbench#1000. |
| Browser → workbench UI | ALB OIDC **or** the workbench's own flow | Depends on Phase 1's option. |

## Phase 4 — Deciding what is protected

**The large phase, and it is not code.** 79 per-route judgements, every client
updated, and no product makes these decisions for us.

The failure mode to avoid is a long period where some routes are protected and
others are not — that is exactly when someone ships a client that works by
accident and breaks when the gap closes.

Likely shape: `/health`, `/version`, `/openapi.json` public; everything else
requiring a token; the relay's own worker transport unchanged (it already
authenticates with `VIVARIUM_ENV_WORKER_TOKEN`, a separate concern).

## On ALB `authenticate-oidc` specifically

Attractive because it would inject `X-Amzn-Oidc-Identity`, which
`resolve_caller` **already reads** — zero application code for the browser case.

Two constraints:

1. **It requires an HTTPS listener on the ALB** (Phase 1, option A). It is a
   listener-rule action executed by the ALB itself; nothing downstream can
   supply the TLS it needs.
2. **It is a browser redirect flow.** It 302s the user agent to the IdP and sets
   a cookie. `atlantis` cannot complete that — it would receive a redirect to an
   IdP it has no way to satisfy. Turning it on across the board would break the
   CLI, which `CLAUDE.md`'s EUTE rule makes the primary product surface.

Because `authenticate-oidc` is a **per-rule** action, a hybrid is available:

| paths | treatment |
|---|---|
| `/workbench`, `/home`, `/docs`, `/bigraph-loom` | ALB OIDC — browser flow, no app code |
| `/api`, `/core`, `/compose`, `/env-worker` | bearer token, validated in-app (Phase 2) |
| `/health`, `/version` | neither |

Both halves can point at the same Keycloak, which is the strongest practical
argument for running one.

## On Keycloak specifically

**For.** No dependency on anyone else's SSO and no procurement. Works inside
GovCloud's network restrictions — which matters more than it first appears:
egress from GovCloud to a commercial Okta or Entra tenant may not exist and may
not be permitted, so for these deployments self-hosting is closer to a
constraint than a preference. Gives real groups and roles, which the current
header cannot express at all. Capacity exists — two `t3.xlarge` nodes at roughly
11% memory requested.

**Against.** Its own database, realm configuration as code, backups, and an
upgrade path. It becomes a hard dependency: if Keycloak is down, nobody uses the
API. Someone owns it, and that person is not currently identified. And it
answers *identity* only — **enforcement is still Phase 4** regardless.

## Open questions

These are genuinely unanswered. Do not treat any of them as decided.

1. **Which domain?** `jcschaff.org` is gone. Phase 1 cannot start without a name
   we control. Is there a Stanford-side domain available, or does this want a
   new registration? **This is now the single blocking question** — with Phases 0
   and 2 done, nothing else can proceed until it is answered.
2. **Is ALB `authenticate-oidc` available in AWS GovCloud?** Not verified. This
   single fact decides whether the hybrid above exists. Cheap to check with
   console access.
3. **Who owns Keycloak** if we run it?
4. **What happens to `sms.cam.uchc.edu`?** It is the one deployment with a real
   exposure and, per `CLAUDE.md`, it is *not deployed from this repository*. It
   may be answerable with a reverse proxy at that site rather than an
   application change. Whoever owns it needs to be found first.
5. **Does auth apply to prod before dev?** Unusually, the exposure argument runs
   the opposite way to the normal rollout order.

## Sequencing note *(now history)*

Phases 0 and 2 were chosen first because they were independent of the
certificate question, reversible, and shippable inert. Both are done, and the
ordering held: neither required a decision anyone had to make.

What that leaves is the part the ordering was designed to expose. **Everything
remaining is blocked on a question, not on effort** — Phase 1 needs a domain,
Phase 3 needs Phase 1, and Phase 4 needs somebody to decide what a protected
route is. There is no more code that can usefully be written ahead of those.

## A trap this repository has already fallen into

The identity header shipped and appeared to work for a day while
pydantic-settings silently ignored the environment variable naming it — the
overlay set `VIVA_API_IDENTITY_HEADER`, the field binds to `IDENTITY_HEADER`,
and `settings.identity_header` stayed `""`. Nothing failed, because **anonymous
is a supported state**, so dead configuration and working configuration are
indistinguishable from outside. The same class of mistake had already been made
once with `ENV_WORKER_WORKSPACE_PATH`.

Whatever gets built here needs a test that proves the **binding**, not the
intent. `tests/api/test_auth_seam.py` now has three, including a general one:
every env var an overlay sets must be read by something.

Phase 2 was built with that in mind, which is why it logs at startup whether
validation is *usable* rather than merely *set*, and why `pyjwt[crypto]` is
declared explicitly rather than relied on transitively — a transitive pin that
moved would have broken token validation silently, which is the same failure
shape a third time.

## References

- `viva_api/api/auth.py` — the seam, and its docstring on what it is not
- `viva_api/api/oidc.py` — Phase 2's validator, and its docstring on what it does
  not make secure
- `tests/api/test_oidc.py` — signed with a real keypair; biased toward every way
  validation could fail OPEN
- `docs/DEPLOY.md` §2b — what the ALB routes, and what silently does not
- `sms-cdk/lib/internal-alb-stack.ts` — the HTTP:80 listener
- `sms-cdk/lib/shared-stack.ts` — the unused ACM certificate path
- [#336](https://github.com/vivarium-collective/viva-api/issues/336), [#337](https://github.com/vivarium-collective/viva-api/issues/337) — the two unblocked phases, filed
- vivarium-workbench#1000 — the workbench's missing `Principal`
- `vivarium-workbench/docs/run-orchestration-consolidation.md` §E — where the
  access-control question was first recorded as open
