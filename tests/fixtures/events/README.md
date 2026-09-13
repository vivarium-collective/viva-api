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
