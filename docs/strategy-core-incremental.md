# Strategy A — incremental: Eran's end state for `viva_core`, the SMS contract intact

*Draft for review, 2026-09-22. One of two companion documents (the other is
[`strategy-core-direct.md`](strategy-core-direct.md)). Both respond to viva-api#742. Neither is
adopted until Jim and Eran agree; the adopted one becomes a dated decision-log entry in
[`plan-core.md`](plan-core.md) and the delta rows in [`architecture-core.md`](architecture-core.md)
change to match.*

## The claim

#742's thesis is accepted **for `viva_core`**: one object (a Composite document), one run
(`Composite(document, core).run()` dispatched by protocol), one way to compose (`tensor` and
templates / Sites), one record (`core.job` with a kind and an owner-ref). What this strategy adds
is a single constraint: **the SMS surface that the workbench and PTools call today does not change**
— not its paths, not its shapes, not on a clock. SMS becomes a thin application over core, and its
endpoints become facades that *map* SMS concepts onto core's, exactly the premise the split was
started under.

The two are compatible because the contract is a set of URLs and JSON shapes, and a facade can hold a
URL and a shape constant while everything behind it is replaced. The 29 workbench paths
(`sms_api_client.py`) and the three PTools calls (`sms.js`) are enumerable; they are the test.

## What "the SMS contract" is, precisely

| surface | who calls it | status under this strategy |
|---|---|---|
| `/api/v1/simulations*`, `/api/v1/analyses*`, `/ws` | workbench, PTools, atlantis | **unchanged, permanent** — SMS facades over core jobs, campaigns and datasets |
| `/core/v1/simulator/*`, `/core/v1/capabilities` | workbench, atlantis | **unchanged, permanent** — the `simulator` record stays (D6, D11) and *maps to* a core environment |
| `/compose/v1`, `/env-worker/v1` | workbench | **unchanged, permanent aliases** of core's own routers (`/viva/v1/composites`, `/viva/v1/workers`) — same code, two prefixes |
| `/api/v1/tasks`, `/api/v1/datasets` | atlantis, smoke | permanent SMS facades over `/viva/v1/tasks`, `/viva/v1/datasets` |
| `/viva/v1/*` | new callers, the core CLI | core's canonical surface; may grow freely |

"Permanent" means: removed only by a *separate* decision with its own migration plan, never as a
side effect of this refactor. This resolves #742's question 3 (no external constraint forces the
aliases; we keep them because breaking a caller we ship beside is not a refactor's job).

## How #742's six changes land, in order

| # | #742 change | where it lands | what stays SMS |
|---|---|---|---|
| 2 | strategies as *composition operator × protocol* | **after P7**: once campaigns are template fills and jobs are `core.job` rows, the five strategies are re-expressed over two generic mechanisms (a protocol object per backend; `tensor` / template fill per shape). Not before: they were carved as-is to reach a deployable state, and they are what production runs on | the SMS *ensemble path* (ParCa + seeds) stays an SMS composition of core calls |
| 4 | `core.job` is *the* record | **P4 → P7**, as planned: #776 is the seam (Protocol + `Job`); P4a adds the owner-ref columns and dual-write; the adapters put `hpcrun` and `task` behind the Protocol; P7 moves the rows into `core.job` and collapses `compose_hpcrun`, `task`, `env_worker_task` | `public.hpcrun` survives as the SMS extension row (chain columns, science FKs) *until* #3 empties it, then it is a view, then it is gone |
| 3 | templates / Sites as the campaign primitive | **P7**, one move with #4: `templates.py` (`fill_sites`, `prune_open_regions`, `trigger`) lifts into `viva_core`; the chain state machine in `job_scheduler.py` becomes "fill, run the ready Sites, record"; live handles sit on `core.job` rows; cancel is a fold over `by_owner`. Proven by the oracle already in #776 and by `chain-cancel` in both phases | the campaign *specification* (which generations, which variants) stays an SMS input to the template |
| 5 | one environment concept | **P5**: the D10 resolver gains its *build* half; a `simulator` becomes a **registered environment** (`ExplicitSpec`, `repo-recipe`, write-once per D11) and the `simulator` table becomes a projection over the `core.environment` table — kept, because `/core/v1/simulator/*` is a contract | the `simulator` id space and its routes |
| 6 | one generic run surface; ecoli as a package | **P8–P9**: `/viva/v1/composites` is the run surface for new callers; the science model is a package (sms-ecoli) that registers composites, hooks (`runner_hooks.py`), analysis modules and layouts with core's extension points; atlantis's generic verbs delegate to the generated core CLI (D8) | `/api/v1/simulations` and the atlantis SMS verbs stay as the ecoli-shaped front door |
| 1 | aliases as a dated phase | **declined** for the four surfaces above; **accepted** for anything new: a `/viva/v1` route never gets an alias | — |

Everything already merged (P0–P3d) is consistent with this table; nothing is undone.

## What this costs, and what it buys

*Costs.* Two spellings of some things for as long as SMS exists: `simulator` beside `environment`,
`/compose/v1` beside `/viva/v1/composites`, `hpcrun` beside `core.job` until P7. Each is a facade
of a few dozen lines that the smoke and a client-parity test hold in place. The P7 table move is
still the high-risk step; this strategy does not make it riskier, it makes it later.

*Buys.* Every deploy checkpoint keeps the property that has held since A: the workbench and PTools
never notice. The 15-comment thread on #742 collapses to two decisions (D14: contract permanence;
D15: templates as the campaign primitive at P7) and the existing sequence.

## The test that says whether it held

`atlantis smoke` Tier 0 `routes` compares the served OpenAPI document with the spec; add a
**`contract`** check that calls each of the 29 workbench paths and the three PTools paths with a
known fixture and diffs the response *shape* against a recorded one. It runs at every checkpoint
from F onward. A change that fails it is a contract change and needs its own decision.
