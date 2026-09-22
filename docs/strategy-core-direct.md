# Strategy B — direct: #742 in full, compatibility not maintained

*Draft for review, 2026-09-22. Companion to [`strategy-core-incremental.md`](strategy-core-incremental.md);
both respond to viva-api#742. Written to be the strongest honest version of the direct route, not a
straw man. Neither is adopted until Jim and Eran agree.*

## The claim

One object, one run, one way to compose, one record — **and one surface.** The SMS-shaped
endpoints (`/api/v1/simulations*`, `/core/v1/simulator/*`), the aliases (`/compose/v1`,
`/env-worker/v1`) and the facades (`/api/v1/tasks`, `/api/v1/datasets`) are migration scaffolding
with a removal date. When the last caller has moved to `/viva/v1`, they go. The `simulator` table
retires into `core.environment`. The science model is a package. `atlantis` splits: its generic
verbs are the generated core CLI; its ecoli verbs and per-mechanism smoke checks move to an SMS
distribution. viva-api ends as `viva_core` plus a thin `sms` package that contributes composites,
hooks, analyses and layouts through core's extension points and serves **no endpoints of its own**.

This is the end state #742 describes, taken literally, with the migration it implies written down.

## What changes for callers

| caller | today | after | migration |
|---|---|---|---|
| vivarium-workbench (`sms_api_client.py`, 29 paths) | `/api/v1`, `/core/v1`, `/compose/v1`, `/env-worker/v1` | `/viva/v1/{composites,environments,jobs,workers,datasets,events}` | a workbench release per surface, coordinated with the viva-api release that removes it; both repos deploy together on each site |
| PTools page (`sms.js`) | `simulations` → `analyses?experiment_id` → `analyses/{id}/data`, matched by filename stem | `/viva/v1/datasets?owner=…` and a dataset's artifact by id | a `sms-ptools` rebuild (hand-built, private, D13) per site; PTools has no client library and no version pin, so this is the brittlest edge |
| atlantis (CLI/TUI/GUI) | ~60 literal URLs across generic and ecoli verbs | generic verbs = generated core CLI (D8); ecoli verbs in an `sms` distribution against `/viva/v1` + the ecoli package's registered composites | the three clients change together (EUTE rule) |
| smoke (`app/smoke.py`) | per-mechanism Tier 2 checks | one Tier 2 shape: a composite × a composition operator × a protocol, parameterised | rewritten once the strategies are re-expressed |

Every row is a breaking change for someone we ship beside. The direct route's honesty is that it
says so and dates it, rather than carrying two spellings indefinitely.

## Sequence

1. **D14 (decision): aliases and facades are dated.** Each gets a removal milestone: "the release after
   the last caller moves." Recorded now; costs nothing; sets the trajectory.
2. **P4 as planned, then P7 early.** #776's `JobStore` seam, P4a's owner-ref migration, the two
   adapters, then the `core.job` move — *before* P5/P6 rather than after — because everything below
   depends on one record.
3. **Templates / Sites into core; chain, ensemble re-expressed.** The chain state machine becomes a
   template fill; the ensemble becomes a `tensor`; multi-node is the `ray` protocol; Nextflow is a
   lowering. Five strategies become two mechanisms. The SMS ensemble path is deleted, not wrapped.
4. **`simulator` → `core.environment`.** The D10 resolver's build half (P5) lands as *the* build; the
   `simulator` table is migrated into `environment` (ids preserved as `legacy_simulator_id`), and
   `/core/v1/simulator/*` becomes a dated facade over `/viva/v1/environments`.
5. **`/viva/v1/composites` as the run surface.** `/api/v1/simulations` is re-implemented as a facade
   over it (the ecoli package supplies the composite id, the ParCa hook, the analysis DAG and the
   layout), then dated.
6. **Client split.** Core CLI generated from the core spec; atlantis's ecoli verbs move out with the
   ecoli package; the workbench and PTools move to `/viva/v1` on the D14 clock.
7. **Removal releases**, one surface at a time, each after its last caller has shipped.

## What this costs, and what it buys

*Costs.* Three repositories change in lock-step at least four times (viva-api, vivarium-workbench,
sms-ptools), plus atlantis. Every removal is a deploy on **both** Stanford sites, and prod is on
0.9.78 with a catch-up that is "not part of this work" — the direct route makes it part of this work.
PTools is the weak point: private, hand-built, no client, no pin. The P7 table move and the
`simulator` migration are both high-risk data moves, and this route does them earlier and with more
callers depending on their outcome. During the migration window the team runs two surfaces anyway
— the difference from Strategy A is only that here the old one has an end date.

*Buys.* No permanent duplication: one record, one run surface, one client, one environment concept.
Adding a backend or a composition shape is a protocol object or a template, not a 300–850-line
mechanism. A public `viva_core` whose own surface is the only surface. And the science model becomes
what #742 says it already is: a package that registers itself, with no endpoints of its own.

## The question this document exists to put

Strategy A keeps the contract and reaches the same `viva_core`. Strategy B reaches a *smaller
total system* — no facades — at the price of coordinated breaking releases across three repositories
and two sites, and of bringing prod's catch-up into scope. The decision is whether the permanent
facades of A are debt worth carrying for the workbench and PTools, or debt worth paying off on a
clock. That is Jim's and Eran's to make, not a thread's.
