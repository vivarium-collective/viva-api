# ptools datasets: integrating `dataset` records into the Pathway Tools page

**Status (2026-09-14): plan, not started.** Follow-up to
[`plan-task-provenance.md`](plan-task-provenance.md) (viva-api PR #656 / issue #655), which
is slice 2; and, first, to [`plan-data-provenance.md`](plan-data-provenance.md), **slice 1**,
which adds the `dataset` table, the `GET /api/v1/datasets` API and explains where the rows
come from. This consumer work depends on slice 1 only; slice 2 adds task-produced datasets
to the same lists. This document is the **consumer side**: how the Pathway Tools omics page stops walking
simulation → analyses → data and instead lists the datasets that exist, by tag and
attribute, and fetches each by id.

Open for refinement with input from others (SRI owns the page; see §5).

---

## 1. What the page is and what it calls today

The "ptools webapp" is Pathway Tools' own web content, `htdocs/sms/sms.html` + `sms.js`
(2,901 lines; SRI-maintained, comments signed `paley:Apr-6-2026`), shipped **inside SRI's
export** (`assets/ptools/aic-export-30.0.tar.gz`) and served by the `ptools` pod on port
1555. `panelConfigs.html`, `rankCompare.html` and `sms2.html` include the same `sms.js`.
Its sms-api base URL is a generated `/sms/env.js` = `SMS_API_HOST` + `SMS_API_PATH` from the
`ptools-config` ConfigMap (`kustomize/config/<ns>/ptools.env`). Verified 2026-09-14 from the
extracted 30.0 tree (`~/.claude/jobs/636b9c0d/tmp/x/…/htdocs/sms/`).

The flow, from the code:

| step | call | what the page does with it |
|---|---|---|
| load | `GET ${simBaseUrl}simulations` (`populateSimulationSelectOptions`, sms.js:701) | simulation picker; a **tag** selector filters on `sim.tags` |
| pick sim | `GET ${simBaseUrl}analyses?experiment_id=<id>` (`getAnalysesForSim`, :805) | "Analysis Configuration" picker labelled only by `n_tp`; no status filter |
| display | `GET ${simBaseUrl}analyses/{id}/data` (`fetchDatasetSim`, :136) | for each checked value type (`ptools_rna`, `ptools_proteins`, `ptools_rxns`) takes the entry whose **`filename.split('.')[0]` equals the value type**; later matches overwrite earlier |
| register | `POST ${ptoolsBaseUrl}/register-omics-dataset` (`registerDatasetPtools`, :91) | `class` gene/protein/reaction from the value type, `datacolumns = "1-<n_tp>"`, title from the sim name; the returned `datakey` drives the Cellular Overview / pathway / dashboard views |

It **launches nothing**: it is a viewer over already-available data. Comparisons
(`#addCompare`, `getCompareDatakeys`, :407) pick other simulations and repeat the same
fetch per value type. `rankCompare.html` registers datasets the same way and compares two
columns. Fetches are sequential by design (:177).

**Why fill bundles are invisible today**, and what is already in flight:
- the filename stem never equals the value type (`ptools_rna_multiseed__variant=0`) — **fixed
  client-side by viva-api PR #658** (`fix/ptools-viewer-valuetype-resolution`, 2026-09-14):
  exact stem → unique prefix → prefer the no-scale-suffix candidate → warn; measured against
  all 97 live bundles (6 carry two scales for one view in one `analysis-mnp-*` dir);
- **no `analysis` row at all** for a fill (`GET /analyses?experiment_id=` returns nothing for
  an `s3-only` bundle) — slice 1's registration;
- no `n_tp` on the row, so `datacolumns` is wrong — **the page counts its own columns**
  (viva-api#675). It already downloads the TSV to POST as `datatext`, so the timepoint count is
  free from bytes in hand; the walk does not open objects, and a producer-reported `n_tp` arrives
  in the `artifact.written` payload once the emit side ships;
- a per-cell bundle inlines 400 files through `/analyses/{id}/data` and the resolver picks
  one — only the dataset-id path (§3) fixes that.

## 2. Goal

The page lists **datasets** (`dataset` rows of `kind = ptools-analysis`) directly —
(a dataset row exists only once the trace was scraped or the reconciliation walk found the
object — the page never sees a planned-but-unwritten file) —
filtered by tag (`cd2`), view, protocol and coordinates — and fetches one TSV by dataset id.
The simulation becomes a label on the row, not the entry point. Per-cell datasets are
addressable one at a time. Compare and rank-compare keep working. Nothing is launched.

## 3. What the page needs from the API (all in slice 1's `/datasets`, `plan-data-provenance.md` §7; two additions requested)

| call | purpose |
|---|---|
| `GET /api/v1/datasets/tags` | populate the tag selector |
| `GET /api/v1/datasets/groups?kind=ptools-analysis&tag=cd2` | **addition 1** — the picker unit: one group per `(producer, protocol, coordinate)` with its member datasets by `view`, plus `display_name`, `experiment_id`, `n_tp`, `available`. Server-side grouping, because a per-cell bundle is 400 rows × views and the page must not page through them to build a picker |
| `GET /api/v1/datasets/groups/{group_key}/coordinates` (or `GET /datasets/attributes?within=<analysis_id>`) | **addition 2** — distinct `seed` / `generation` / `agent` values inside a per-cell group, to build the seed/generation selectors |
| `GET /api/v1/datasets?kind=ptools-analysis&analysis_id=&view=&attr.protocol=&attr.variant=&attr.seed=&attr.generation=` | resolve the selected group + coordinates + value types to concrete dataset ids |
| `GET /api/v1/datasets/{id}/content` | the TSV text (`text/tab-separated-values`) |
| `GET /api/v1/datasets/{id}` | attributes (`n_tp`, `display_name`) for `datacolumns` and the title |

Conventions the page relies on (to be stated in slice 1): every `ptools-analysis` row has
`attributes.n_tp`, `attributes.protocol`, `attributes.variant`, `view`; `display_name` =
`<experiment_id> · <protocol>[ · s<seed> g<gen>]`; `tags` include the campaign (`cd2`) and
family (`run3`, …); `available=true` is the default filter.

## 4. Page changes (`sms.js` / `sms.html`)

Keep the public entry points (`ptoolsGoBtnHandler`, `celovBtnHandler`, `dashboardBtnHandler`,
`pwyBtnHandler`, rank-compare) and the PTools registration path (`registerDatasetPtools`,
`saveOmics`, `cachedDatasets`) unchanged; replace the *data discovery and fetch* layer:

| today | after |
|---|---|
| `populateSimulationSelectOptions` → `GET simulations`; tag selector from `sim.tags` | `populateDatasetGroups` → `GET datasets/tags` + `GET datasets/groups?kind=ptools-analysis&tag=` ; the `simSelector` lists **groups** (label = `display_name`), still filtered by tag |
| `simSelected` shows `generations` / `n_init_sims` from the sim config | `groupSelected` shows the group's attributes (protocol, variant, `n_tp`, seeds × generations present) and, for `protocol = single`, builds seed/generation selectors from addition 2 — the commented-out `populateAnalysisSelectionCtrls` / `setAnalysisSelectionVisibility` (:865–915) is exactly this UI and can be revived |
| `getAnalysesForSim` / `populateAnalysisSelector` (`n_tp` label) | removed; the group *is* the analysis configuration |
| `assembleSimQueryParams` → `{experiment_id, analysis_id}` | → `{group_key, coordinate}`; `getSim()` callers become `getGroup()` |
| `fetchDatasetSim` → `GET analyses/{id}/data`, stem matching, retries | `fetchDataset` → resolve `(group, coordinate, valueType)` to a dataset id (addition-1 payload already carries member ids per view, so usually no extra call) → `GET datasets/{id}/content` as text; retries kept |
| `getDataColumns` from `analysis.n_tp` | from `dataset.attributes.n_tp` |
| `registerDatasetPtools` title from `sim.name` | from `dataset.display_name`; `class` from `view` (unchanged mapping) |
| `getCompareDatakeys` picks other **simulations** | picks other **groups** with the same view set; per-value-type fetch unchanged |
| `cacheDataset(cacheKey, …)` keyed by experiment + analysis + value type | keyed by dataset id (stable across page reloads; invalidated when `updated_at` changes) |
| `valueType` checkboxes hard-code three views | built from the group's member views (adds `ptools_metabolites`, `ptools_overview` when present); the `class` mapping gains `compound` for metabolites (`ptools_object_class` precedent in `viva-ptools`) |

**Feature detection, so one page works against old and new APIs during rollout:** on load
`GET ${simBaseUrl}datasets/tags`; a 404 falls back to the current simulation flow — which,
with #658 merged, already resolves multiseed fills once slice 1 has registered them
(and that fallback is what #658 buys until this page lands).

**Same-origin stays true:** the ALB path-routes `/api/*` to sms-api and everything else to
PTools, and the laptop tunnel reproduces that; no CORS is needed. Set `SMS_API_HOST=`
(set, **empty**) in both Stanford `ptools.env` files so `simBaseUrl` is the relative
`/api/v1/` — do not delete the key (a NIL prints as the literal `NIL`). Verify on the pod:
`curl http://localhost:8080/sms/env.js` through the tunnel.

## 5. Where the patched page lives

`sms.js` is SRI's file, inside their CVS-tracked tree in the export. Two tracks, in
parallel:

- **Overlay now (ours).** PR #658 uses the same mechanism — `assets/ptools/overrides/htdocs/sms/sms.js`
  `COPY`d over the archive's file after the `ADD` in `Dockerfile-ptools` — but a **different
  approach**: it keeps the old endpoints and filters filenames client-side. The edits in this
  plan replace the discovery/fetch layer, so they **supersede #658's resolver** (content is
  fetched by dataset id; there is no stem to resolve) and **will conflict** with it in that
  file. Expected handling: this work lives on its own branch and is rebased onto whatever
  merged first; keep a `sms.js.patch` against the pristine 30.0 copy so both a #658 rebase and
  a future export sync are mechanical. ptools images are hand-built (never by CI) —
  `kustomize/scripts/build_and_push.sh`.
- **Sequencing (Jim, 2026-09-14): this is the last part of the data-provenance work** and can
  wait. Until then it is exercised without any deploy: with the SSM tunnel up, the page at
  `http://localhost:8080/sms/sms.html` is served by the ptools pod behind the same path-routed
  ALB as `/api/*`, so a **browser DevTools local override** of `/sms/sms.js` (Chrome: Sources →
  Overrides) runs the patched script against live sms-api and live PTools with no CORS and no
  image rebuild. A dev-only `sms-ptools` tag pinned in the dev overlay is the next step up.
- **Upstream (SRI):** hand this document plus the patch to SRI (Paley) so the next export
  carries it and the overlay can be dropped. Their `displayMassFractionSummary` stub
  (:427, never wired) suggests they intended sms-api-side data services; `/datasets` is that
  service.

## 6. Rollout

1. Slice 1 (`plan-data-provenance.md`) deployed to `sms-api-stanford-test` with its importer
   run → dataset rows exist for the CD2 fills (`origin = walk`); slice 2 is not required.
2. Build + push `sms-ptools:<api version>` with the overlay; bump the dev overlay's
   `sms-ptools` `newTag` **only** for an image that was actually pushed; apply; verify by
   port-forward `svc/ptools 15550:1555` (`GET /` 200, `organism-summary?object=ECOLI` 200)
   and `GET /sms/env.js` shows the relative base.
3. Through the ALB (tunnel): open `/sms/sms.html`, tag `cd2`, pick the Run-3 combo00
   multiseed group, check RNA + reactions → overlay renders from the fill TSVs; pick a
   per-cell group, choose seed 3 / generation 12 → renders one cell; add a compare group.
4. Prod: same image + `ptools.env`, after Eran/Alex have used it on dev.
5. Hand the patch to SRI.

## 7. Verification

- **JS unit (no build tooling):** extend #658's pattern (`assets/ptools/overrides/test_sms_resolve.js`:
  plain `node`, the helper lifted out of `sms.js` by marker, no DOM) with a `test_sms_groups.js`:
  group → picker rows; coordinate resolution to a dataset id; feature-detect fallback;
  `datacolumns` from `n_tp`. Fixture JSON captured from dev's `/datasets/groups`.
- **API contract:** slice 1's router tests already cover `/datasets`; add the two additions
  (§3) there with a per-cell fixture (5 views × 32 cells).
- **Live:** §6 steps 2–3 through the page, not curl; then `atlantis dataset list --kind
  ptools-analysis --tag cd2 --view ptools_rna --attr protocol=multiseed` agrees with what
  the picker showed.

## 8. Non-goals and audits

- Launching analyses from the page (it never did; `POST /simulations/{id}/analysis` stays
  the API's job).
- `dashboardJsonGenerator.js` / `jsonCategoryGenerator.js`: audit whether they touch
  `simBaseUrl` (the extracted tree says only `sms.js` does; confirm before the overlay).
- `rankCompare.html` / `panelConfigs.html` / `sms2.html` include `sms.js` — they must keep
  working with the new discovery layer; covered by keeping the registration path and
  `allDatasets` / `cachedDatasets` shapes unchanged.
- The `viva-ptools` workbench plugin is a different consumer (it globs a workspace's
  `**/ptools/*.tsv` and builds an Omics-Viewer URL); pointing it at `/datasets` is a
  separate, smaller change.

## 9. Open questions for reviewers

1. Group key: `(analysis_id, protocol, variant)` with seed/generation as in-group
   coordinates (proposed), or one group per coordinate (simpler page, huge picker)?
2. Should the page show `origin` (event vs walk) and `available=false` rows greyed out, or
   hide them? Proposed: show greyed, with the reason on hover — it is provenance.
3. Do we want the compare feature to allow cross-protocol comparisons (multiseed vs a single
   cell)? Today the page compares like with like; proposed: keep that constraint.
4. SRI: are they willing to take the patch upstream, and on what cadence do exports ship?
