# TODO #40 — BioModels Integration (Atlantis Academic API)

> **Status**: ✅ COMPLETE. All 6 phases implemented, tested, and documented.
> **Deployment target**: `sms-api-rke` only (UCONN CCAM / SLURM). Not for GovCloud.

---

## Summary

Ports the functionality of `../biomodels-regression` into `sms-api`, enabling the EBI BioModels
database team to run single-model, dual-simulator, and full-regression (up to 1,000 models) SBML
ODE simulations on HPC via the Atlantis API and CLI — without writing any custom scripts.

Three use cases supported:

| Use Case | Description | Trigger |
|----------|-------------|---------|
| **A. Audit** | Run the same model on 2 simulators (COPASI + Tellurium), compare | On-demand / CI |
| **B. Validate/Onboard** | Screen new models before accepting to BioModels DB | Per-submission |
| **C. Author modifications** | Re-run a modified publication model to verify | Per-publication |

---

## Implementation state

All 6 phases are complete as of commit `83af46da`.

### Phase 1 — PB document factory

**File**: `viva_api/compose/biomodel_documents.py`

Ports `document_creation.py` from `biomodels-regression`:
- `make_utc_step_state` — builds a single simulator step with full UTC config
- `make_biomodel_document` — single or dual-simulator PBG document
- `make_multi_biomodel_document` — multi-model PBG document
- `TYPES_DICT` — registers `numeric_result`, `result`, `species_concentrations`, `species_counts`

### Phase 2 — Service refactor

**File**: `viva_api/compose/biomodels_service.py`

All EBI interaction and SED-ML parsing:
- `get_identifiers()` — curated ODE model IDs from EBI
- `get_biomodel_info()` — model metadata
- `load_biomodel()` — fetch OMEX, parse SED-ML, extract UTC params

Endpoints now use `make_biomodel_document` (programmatic PBG) instead of flat Jinja templates.

### Phase 3 — Audit endpoint (Use Case A)

**Endpoint**: `POST /compose/v1/biomodels/{biomodel_id}/audit`
**CLI**: `atlantis compose biomodels-audit`
**Models**: `BiomodelsAuditResult`

Runs both COPASI and Tellurium in a single compose job using a dual-step PBG document.

### Phase 4 — Regression endpoint (Use Case B)

**Endpoint**: `POST /compose/v1/biomodels/regression`
**CLI**: `atlantis compose biomodels-regression --n 1000`
**Models**: `BiomodelsRegressionRequest`, `BiomodelsRegressionResult`

Fetches N model IDs from EBI, submits each as a separate compose simulation, reports submitted
and failed IDs.

### Phase 5 — Tests

| File | Tests | Coverage |
|------|-------|----------|
| `tests/compose/test_biomodel_documents.py` | 11 | PB document construction, schema, dual-sim, multi-model |
| `tests/compose/test_biomodels_service.py` | 14 | Service utils, EBI mocked, UTC extraction |
| `tests/compose/test_biomodels_routes.py` | 9 | All 6 endpoints, success + error paths |
| `tests/compose/test_biomodels_cli.py` | ~8 | All 6 CLI commands, params, output |

All 36+ tests mock EBI and HPC — no real HTTP or SSH calls in CI.

### Phase 6 — Docs

**File**: `docs/source/guides/biomodels.md`

Covers: overview, architecture diagram, all 6 REST endpoints with example request/response,
all 6 CLI commands with options, full step-by-step walkthrough tutorial.

Also added to toctree in `docs/source/index.rst`.

---

## All 6 REST endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/compose/v1/biomodels/identifiers` | List EBI curated ODE model IDs |
| `GET` | `/compose/v1/biomodels/{biomodel_id}/metadata` | Fetch model metadata |
| `POST` | `/compose/v1/biomodels/{biomodel_id}/run` | Run single model |
| `POST` | `/compose/v1/biomodels/batch` | Run list of models (one job each) |
| `POST` | `/compose/v1/biomodels/{biomodel_id}/audit` | Dual-simulator cross-validation |
| `POST` | `/compose/v1/biomodels/regression` | Full regression suite (up to 1,000 models) |

## All 6 CLI commands

```bash
uv run atlantis compose biomodels-ids               # list IDs from EBI
uv run atlantis compose biomodels-meta <ID>         # model metadata
uv run atlantis compose biomodels-run <ID>          # single model
uv run atlantis compose biomodels-batch <ID>...     # batch submission
uv run atlantis compose biomodels-audit <ID>        # dual-simulator audit
uv run atlantis compose biomodels-regression        # regression suite
```

---

## Source mapping (biomodels-regression → sms-api)

| biomodels-regression file | sms-api destination |
|---------------------------|---------------------|
| `types.py` | `viva_api/compose/biomodels_service.py` (dataclasses) |
| `biomodel_retrieval/biomodel_fetching.py` | `viva_api/compose/biomodels_service.py` (`load_biomodel`) |
| `biomodel_retrieval/sedml_parsing.py` | `viva_api/compose/biomodels_service.py` (`_read_sedml_doc`, `_extract_first_utc`) |
| `utils.py` | `viva_api/compose/biomodels_service.py` (`_iter_entry_files`, `_find_first_sedml`, ...) |
| `document_creation.py` | `viva_api/compose/biomodel_documents.py` |
| `__init__.py` (TYPES_DICT) | `viva_api/compose/biomodel_documents.py` |
| `run_biomodels.py` | `POST /compose/v1/biomodels/regression` + `POST .../audit` |
