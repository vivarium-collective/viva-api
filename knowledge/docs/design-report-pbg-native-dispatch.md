# Process-Bigraph-Native Multigen/Multiseed Dispatch — Design Report

Written 2026-09-03, for Eran, in response to a direct challenge: does the process-bigraph `ray:`
protocol already distribute a multi-lineage campaign correctly on its own, without any bespoke
"result aggregation" layer? **Short answer: yes, on both counts.** It distributes correctly today,
already proven at real scale. And the thing that got called "result aggregation" isn't a missing
mechanism at all — it's one config value away from not needing to exist as a separate step, using a
capability (`s3://`-URI-backed zarr writes) the emitter library already ships. This report traces
the real mechanism from primary source (`process_bigraph/protocols/ray.py`, `v2ecoli/workflow/
lineage.py`, `v2ecoli/workflow/batch_lineage_ray.py`, `v2ecoli/steps/batch_baseline_runner.py`,
`viva_emitters/xarray_emitter/storage.py`, all read directly against `origin/main` while writing
this, not recalled from memory) and corrects the framing this thread started with — twice, in fact:
the first correction (below) showed aggregation wasn't a distribution problem; a second, deeper check
(section 4) found the fix isn't even new code.

## 1. What "process-bigraph-native dispatch" (item 101) actually is

`v2ecoli/composites/lineage_ray_batch.py` + `v2ecoli/workflow/batch_lineage_ray.py` build ONE
composite document containing N `LineageProcess` nodes, each addressed via process-bigraph's own
`ray:` protocol (`process_bigraph/protocols/ray.py`). This is fundamentally different from
chain-dispatch (item 71): there is no external AWS-Batch-job-dependency orchestrator. Process-bigraph
itself represents and schedules the seeds × generations shape — the engine that runs this composite
IS the thing deciding when each lineage advances, not a poll loop external to it deciding when to
submit the next AWS Batch job.

Each `LineageProcess` node runs N generations of ONE seed-lineage, using real process-bigraph
structural deltas (`_add`/`_remove`) at each division — this needed zero new engineering; it already
existed before item 101 and is genuinely pbg-native on its own. What item 101 closed was the
SEED-level fan-out: previously (`BatchBaselineRunner`), N seeds were dispatched via a raw Ray API
call from inside one opaque Step, invisible to process-bigraph itself — the same class of defect
chain-dispatch has, one layer down. Item 101 replaces that with N real `ray:`-addressed nodes,
wired directly into the composite's own state tree, using the same mechanism the colony composite
(item 88) already proved works across real physical AWS nodes.

## 2. Does the `ray:` protocol distribute state correctly? — traced from source, not assumed

Read directly: `process_bigraph/protocols/ray.py`, `RayProcess.update()`:

```python
def update(self, state, interval):
    ...
    return ray.get(self.actor.update.remote(state, float(interval)))
```

This is the entire mechanism. `RayShadowProcess` (the node actually wired into the document) calls
`update()` exactly like any local process — the engine has no idea it's remote. Internally, that call
dispatches to a pooled Ray actor and **blocks synchronously on `ray.get()`** until the real result
comes back. The returned dict is applied to the composite's own state tree on the head node, through
the completely ordinary process-bigraph update mechanism — no special casing, no gather step, no
external sync.

**This means Eran's claim is correct, precisely stated: the OUTER composite's own state tree already
unifies correctly across every `ray:`-addressed lineage, natively, every tick, as a direct consequence
of how process-bigraph itself works.** A `Step`/`Emitter` wired at the top level of this composite,
observing all N lineages' state, would already see the aggregate result after every engine tick —
today, with zero new code. This was proven at real 16-node AWS Batch MNP scale (item 101's own real
dispatch, sim 253/255): real structural division genuinely survived the actor boundary, confirmed via
real CloudWatch logs and real S3 output, not status fields alone.

## 3. So what was "result aggregation" actually about?

Not the outer composite's state — the **biological simulation trace** each lineage produces, which
is what every downstream `cd1_*`/`ptools_*` analysis actually reads.

Traced directly, `v2ecoli/workflow/lineage.py`:

- `LineageProcess.update()`'s own declared output schema (line 216): `{"summary": "map", "complete":
  "boolean"}`. The real returns, verified line by line: `{"complete": True}`, `{}`, `{"complete": True,
  "summary": {"generations": self._summaries}}`. **What crosses the `ray:` boundary back to the outer
  composite is a tiny status object — not the cell's molecular state.**
- The actual heavy data — every molecule count, every tick, for the whole cell — is written by a
  private, **internal** `Composite` object each `LineageProcess` instance builds and owns entirely
  for itself (`self._composite = Composite(doc, core=core)`, line 310). Its own `XArrayEmitter` is
  opened directly inside `LineageProcess`'s own code (`_open_xarray_emitter`, line 314) and writes to
  a **local filesystem path** (`self._xarray_store = os.path.join(...)`, line 350) — local to whichever
  physical Ray worker that particular actor happens to be running on.

This is a deliberate, defensible design, not an oversight: shipping the full per-tick molecular state
of a whole E. coli cell back across the network to one central emitter, for every tick, of every
lineage, would be real, substantial bandwidth and serialization overhead compared to writing it
locally, right where the computation already happened. The same tradeoff shows up in ordinary
single-machine simulations too — it's not a `ray:`-specific compromise.

**The real, narrow, already-understood consequence**: N lineages' worth of zarr stores end up on N
different physical nodes' local disks. Getting them into one place a downstream analysis can read as
a single dataset is a genuine question — but it is a data-logistics problem (get files that exist
locally onto shared storage), not a distributed-computing problem, and it is not evidence that the
`ray:` protocol itself fails to distribute correctly. Conflating the two was the imprecision in how
this was originally framed as "we need a result-aggregation system."

## 4. What actually resolves it — a config value, not new code, revised after checking one level deeper

An earlier proposal (a single "gather-Step" reading all N lineages' S3 output once every lineage
reports complete) was found unsafe and rejected: there is no shared filesystem across MNP nodes, and
the periodic S3 sync each worker performs is best-effort — a gather-Step on the head node could read
stale or incomplete state. A second proposal (each `LineageProcess` syncing its own local zarr store
to S3 itself, at every generation boundary) was the next fallback — real, safe, precedented, but still
new code.

**Checking one level deeper found something better: the underlying emitter library already writes
directly to S3 natively, and `LineageProcess`'s own code already has a live branch for it.** Traced
directly, `v2ecoli/workflow/lineage.py::_open_xarray_emitter`:

```python
out_dir = arg.get("out_dir") or self.config["out_dir"]
out_is_s3 = is_s3_uri(out_dir)
...
if not out_is_s3:
    # zarr's own S3 store (opened via zarr.open_group(store=...) inside
    # pbg-emitters) handles "fresh store" / "create the prefix" semantics
    # itself for s3:// URIs
    ...
```

And the real `pbg-emitters` package (`viva_emitters/xarray_emitter/storage.py`) confirms it: given a
remote URI, it resolves the store through `fsspec.get_fs_token_paths` and writes zarr chunks straight
to that backend — no local file, no separate sync, incrementally, as they're produced. **This already
works today, for any caller that passes an `s3://` `out_dir`.**

Item 101's own composite doesn't pass one. `resolve_out_dir()` (`v2ecoli/steps/batch_baseline_runner.py`)
resolves, on the real Ray-on-Batch dispatch path, to `PBG_RESULTS_DIR` — a **local** directory that
the AWS Batch entrypoint script syncs to S3 periodically. That's the actual, current source of the
gap this report originally called "result aggregation": not a missing mechanism, a config value that
routes through the slower, local-then-sync path when a faster, direct, already-built path already
exists one branch away.

**Revised recommendation**: pass an `s3://` `out_dir` (or `emitter_arg.out_uri`) for the `lineage_ray_batch`
dispatch path specifically, so each lineage's actor writes its own zarr chunks directly to S3 as it
runs. No new sync mechanism, no gather step, no per-generation self-sync code — reusing a capability
that already exists and is already tested, just not currently wired to this call site. One real,
open question this raises rather than closes: whether many concurrent actors each issuing their own
S3 writes (rather than one buffered local writer, synced periodically) introduces meaningful S3
request-rate or per-write-latency cost inside the simulation's own hot path at real scale — worth a
real, small-scale empirical check before assuming it's free, not assumed either way here.

## 5. What's proven, what isn't — full honesty, not softened

**Proven, at real 16-node AWS Batch MNP scale:**
- The composite registers and resolves through the standard `import v2ecoli` entry point.
- Real structural division survives the `ray:` actor boundary (real CloudWatch + S3 evidence).
- The actor-pool sizing mechanism, once two real bugs were found and fixed (a composite parameter
  default that silently capped concurrency at 2 instead of the real per-node vCPU count; an AWS
  eventual-consistency race on a freshly-registered job definition), correctly scales concurrency —
  confirmed via a real, quantified before/after: 100 seeds started within an 11-second window and
  completed generation 0 within 6m46s at the corrected pool size, versus roughly one new seed
  starting every 4–6 minutes at the original buggy default.

**Not yet proven:**
- Real 1000-seed scale (only ~100 seeds / 16 nodes tested so far).
- The Option 1 sync mechanism described above — designed, not built.

Neither gap is evidence the approach doesn't work. Both are the honest remainder between "proven at
the scale tested" and "a full production replacement for chain-dispatch, which already has a clean,
independently-verified 1000×10 production run behind it."

## 6. Why chain-dispatch exists in parallel, and why that gap should have been caught earlier

Chain-dispatch (item 71) was built roughly two weeks before item 101 existed as an option, as the
direct fix for a real production incident: a 1000-seed AWS Batch campaign's compute-environment
scaling silently stalled (confirmed via CloudTrail — zero scaling API activity for 30+ minutes
despite ~1000 jobs waiting). The fix that shipped reused infrastructure already trusted at the time —
AWS Batch container jobs, sequenced by an app-level poll loop — under real deadline pressure.
`LineageProcess` (the exact pbg-native primitive item 101 later used) already existed in the codebase
when that fix was built and was never checked. That is a real process gap, already acknowledged
directly in this session, not a considered rejection of the `ray:`-native approach: a ticket's own
narrow scope does not excuse skipping an existing-pattern check on a fix that is itself an
architecture decision.

## 7. Recommendation

1. Point the `lineage_ray_batch` dispatch path at an `s3://` `out_dir` instead of the local,
   entrypoint-synced `PBG_RESULTS_DIR` default (section 4) — a config change, not new engineering.
   Verify empirically first, at small scale, whether concurrent per-actor S3 writes add meaningful
   latency inside the simulation's own hot path before relying on it at real campaign scale.
2. Run a real proof at meaningfully larger scale (not necessarily the full 1000 seeds immediately —
   a graduated step, matching the same staged-ramp discipline chain-dispatch's own scaling proof used).
3. Once both land, item 101 is positioned to be evaluated as the primary path for future multigen/
   multiseed campaigns, with chain-dispatch's own proven 1000×10 run remaining the reference point it
   needs to match or exceed before fully superseding it — not because chain-dispatch is architecturally
   preferred, but because a production deliverable already depends on it and a replacement earns that
   role by being proven, not by being cleaner in principle.

## 8. Final resolution — one more level checked, no code needed at all for emission

A later, more precise pass (checking `LineageProcess` in full before building anything against section
4's own "Option 1 sync" plan above) found this needs even less than section 4 proposed — kept here
rather than silently editing history above.

**`LineageProcess` has TWO separate output streams, not one.** Parquet (the real, analysis-critical
stream — what every `cd1_*` analysis reads) is written by a Step wired *inside* `baseline()`'s own
inner composite (`set_parquet_emitter_override`). XArray/zarr (a coarse, dashboard-oriented snapshot)
is captured by `_emit_xarray`, called exactly **once per generation** — confirmed directly from
`_run_until_division`'s own control flow, not a per-tick trace at all.

**Both already natively support direct S3 output**, independently: `parquet_vecoli`'s own docstring
states `out_dir` may be a local path or an `s3://` URI; `_open_xarray_emitter`'s own `is_s3_uri`
branch does the same. Neither needed new code — both already read `self.config["out_dir"]`.

**`out_dir` is already a real, registered, generically-passable dispatch parameter** —
`v2ecoli/composites/lineage_ray_batch.py`'s own `@composite_generator` registration:
`"out_dir": {"type": "string", "default": "", "description": "Output dir; resolved at run time if
empty."}`. viva-api's `_submit_multi_node_composite` threads a dispatch's own `params` straight
through generically (`mnp_dispatch.get("params")`), zero composite-specific code.

**So the entire fix is: pass `out_dir="s3://..."` in the next real dispatch's `params`.** Not a code
change anywhere, in either repo. Both streams then write directly to one shared location, from inside
each lineage's own embedded composite — the same pattern colony's own cells already use safely — with
nothing new crossing the `ray:` boundary and no RAM risk from section 3's own colony-leak precedent,
because that risk was specifically about a top-level Emitter deep-copying full agent state, which this
design never introduces.
