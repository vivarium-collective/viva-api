- **2026-09-26** — **The data-provenance track is folded into this plan: one coordinated effort (Jim).**
  The session that carried viva-api#655/#656/#657/#661 is closed. What it delivered is already
  here under other names: slice 1 (#661, merged 2026-09-19, deployed at B) is the `dataset`
  registry P4a-1 generalised (#790, checkpoint F) and P4a-2 moved into core (#820/#821/#822/#829,
  checkpoint G); slice 2 (#656's plan, merged 2026-09-23) is P4b in core shape on #776 (D15).
  What its PR map (`plan-data-provenance.md` §9) still had outside this plan is now inside it:
  the **emit side** — no producer emits `artifact.written` yet, so every live row is the walk's —
  becomes **P4d** with **checkpoint G2** (v2ecoli `artifact()` over #772's `emit`, the four call
  sites, the sms-ecoli pin, a simulator; proof = `origin = "event"` rows before the walk tick);
  the **ptools consumer** (`plan-ptools-datasets.md`) rides D18 (the datasets-shaped `sms.js` at
  P5b, rebuilt per site before M7). Carried defect: #780. The three provenance documents keep
  their design content; their status headers now point here. Open question 4 is closed.
