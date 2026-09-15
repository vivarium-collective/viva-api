# Event-stream fixtures (observability plan D1' / D4b)

Settled schema: `v, ts, seq, source, component, event, level, trace_id, span_id, parent_span_id,
global_time, wall_time, baggage, tags, payload`. `component` is a free string (`process_bigraph`,
`v2ecoli.lineage`, `viva_api.dispatch`); event names are dotted; domain identity (`sim_id`,
`experiment_id`, `variant`, `lineage_seed`, `generation`) rides only in the opaque `baggage` map, as
strings on the wire -- viva-api's ingester promotes what it wants into columns.

- `engine_stream.jsonl` -- a thinned copy (1 tick in 10, first 160 events, plus one of each of
  run.end / span.end / structure.changed) of the raw stream process-bigraph#209 @66bccbdd emitted
  for its growth-division contract example, verbatim (its baggage is the example's own generic
  `experiment` / `replicate` / `stage` keys -- nothing viva-api promotes, which is the point).
- `lineage_failed_gen1.jsonl` -- a hand-written lineage task modelled on sim 943 (2026-09-10):
  generation 0 divides at 2,528 s, generation 1 dies at t=7 s with `NegativeCountsError`. Runner
  events `lineage.generation.start` / `lineage.division` / `lineage.chunk.flushed` /
  `lineage.generation.end` / `lineage.failure`; one deliberately bad line.
- `lineage_running_gen1.jsonl` -- the same task still running generation 1 (open spans, heartbeats).

- `real_sim1319_lineage_thinned.jsonl` and `real_sim1319_gather_thinned.jsonl` -- **real** objects from dev (`sms-api-stanford-test`),
  simulation 1319 (`sim209-obs-verify5-gather-146e`, trace `b87bb2477553ee5a0c98ca6588eebec7`,
  2026-09-14): one chain-dispatch lineage task and the run's in-run analysis gather. Kept lines are
  verbatim -- trace and span ids, `seq`, payloads and baggage value TYPES -- thinned for size (a few of
  each high-volume event; the gather's first 8 spans with their ends and 3 samples). The only edit:
  the internal EC2 hostname in `source` is replaced with `batch-node`, keeping the per-process suffix.
  Used by `tests/simulation/test_real_trace_fixture.py` to prove the ingester and the dataset registry
  on a real stream, not only on hand-written ones. **Note:** this real lineage stream carries `variant`,
  `lineage_seed` and `generation` baggage as JSON **numbers**, not strings -- "strings on the wire"
  above holds for the hand-written fixtures, not for every emitter, so readers must accept both.
