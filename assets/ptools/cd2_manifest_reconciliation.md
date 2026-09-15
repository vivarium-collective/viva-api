# Reconciliation: ptools run manifest (Jim/Claude) vs CD2 deliverables report (Alex, PR #415)

Generated 2026-09-14T21:0xZ. Sources: `cd2_ptools_manifest.json` (S3 + Batch + simulator registry)
and `docs/deliverable/cd2/deliverables-report-cd2.md` @ sms-ecoli PR #415 (generated 20:43:50Z).
Join key: Alex's `Dispatch <N>` = viva-api `database_id` -> `experiment_id` -> S3 store prefix,
resolved against the live API (1,294 simulations).

## The two documents answer different questions

| | Alex's report | this manifest |
|---|---|---|
| unit | a **dispatch** (was it run, did it complete, who accepted it) | an **artifact** (what exists in S3, produced by which image) |
| authority | acceptance, provenance of decisions | coverage, file-level truth |
| blind spot | whether the analysis output actually exists | whether anyone accepted the run |

Neither alone answers *"is the ptools deliverable complete for accepted run X"*. The join does.

## 1. Confirmations — my data agrees with his

* **Every dispatch id he cites resolves.** 106 ids checked, 0 missing from the DB.
* **Run 3 combo21 correction is right.** `1023` -> `sim200-cd2-run3-sweep-combo21-03a4` (has the
  per-cell fill); `1085` -> `...-combo21-retry1-903b` (empty). This also explains one of the two
  duplicate combo prefixes my scan flagged: the retry was cancelled, `-03a4` is canonical.
* **Run 2 `1131`** -> `sim201-cd2-run2-j3-refire3-parquet-96b27f38-10x10-dd03`, exactly the store
  both of today's gathers landed in.

## 2. Closed for him: the `948` question he could not reconfirm

His report says the `948`-was-mislabelled finding is *"strongly evidenced but not yet
live-reconfirmed"*, blocked by an SSM permission wall on the DB. Reconfirmed here through the tunnel:

```
948  sim=189  sim189-sim189-mecillinam_wellmixed-8a6a   cfg=mecillinam_wellmixed.json  parca=249
950  sim=189  sim189-sim189-mecillinam_wellmixed-3fed   cfg=mecillinam_wellmixed.json  parca=249
954  sim=189  sim189-sim189-mecillinam_wellmixed-f526   cfg=mecillinam_wellmixed.json  parca=249
```

All **43** genuine Run-2 J3 dispatches in the DB carry `run2-j3` in the experiment_id and a J3 config;
none uses `mecillinam_wellmixed.json`. **`948` is categorically not a Run-2 dispatch** — his
conclusion holds, and the open item can close. (This confirms *"not Run 2"*; that it was specifically
melee's Run-3 attempt rests on his 09-10 cross-reference, not on this evidence.)

## 3. What the manifest adds that the report does not have

### 3a. Run 4 +Trp (`1220-1277`) has 4 of 5 ptools views — no metabolites
Those 58 ids are exactly `sim204-cd2-run4-*-withtrp-trpfix-4x8-*`. Verified across the family:

```
sim204-...-genotype{1,10..16}-withtrp-trpfix  [ptools_overview, ptools_proteins, ptools_rna, ptools_rxns]
sim201-...-genotype1-minimal (filled)         [ptools_metabolites, ptools_overview, ptools_proteins, ptools_rna, ptools_rxns]
```

The report calls this arm *"re-fired clean across all 58 design slots with zero defects"* — true of
the **simulation**; the **ptools deliverable** is 0/58 on metabolites. The `append-metabolites` fill
that supplied the 5th view to the minimal arms was never run against sim204.

### 3b. Run 1 coupled — the accepted deliverable has no analysis output at all
All 10 seeds (simulator 199) have **`analyses/` entry count = 0**:
`sim199-sim199-cd2-run1-k4-coupled-kla350-seed{0..9}-*`. The report calls this
*"the real scientific result... completed cleanly across all 10 seeds"*, and the lineage data is
indeed present — but zero ptools/analysis artifacts exist for it.

### 3c. The Run-4 native arm straddles two media
`1281-1312` spans simulator **201 and 204**: 16 minimal designs (metabolites filled) + 16 +Trp
designs (not). The report presents it as one arm; ptools coverage is exactly half.

## 4. Acceptance vs ptools coverage

| Alex's deliverable | his status | n | with fill | ptools TSV |
|---|---|---:|---:|---:|
| Run 1 coupled — production ensemble | ACCEPTED (Eran 14:24Z) | 10 | **0/10** | 0 |
| Run 1 cell-only — 749 | not acceptable (analysis_v8 4/4) | 1 | 0/1 | 0 |
| Run 2 — parquet re-fire 1131 | complete | 1 | 1/1 | 40 |
| Run 2 — Nextflow re-fire 962 | recorded accepted | 1 | **0/1** | 0 |
| Run 3 — production sweep (combo21) | ACCEPTED 144/144 | 1 | 1/1 | 400 |
| Run 4 — second-medium re-fire | "clean, zero defects" | 58 | **0/58** | 0 |
| Run 4 — native-gene arm | null result, awaiting Eran | 32 | 16/32 | 2565 |

**Two of the three formally accepted deliverables have no ptools output.**

## 5. Framing differences (not contradictions)

* *"9 `cd2fill-run3-percell-*` RUNNING"* — 8 are per-cell shards; the 9th is
  `cd2fill-run3-multiseed-806`, a different family.
* The report (20:43Z) says the run2-j3 gathers were *fired*; both had **landed** by ~19:00Z
  (multiseed 10/10, multigen 6/6, both uploaded). A refresh will pick this up.
