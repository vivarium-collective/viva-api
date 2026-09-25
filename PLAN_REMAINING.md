# Remaining Work Items

---

## 1. ALB Timeout Docs (todo:29)

**What it is:** Document that the Stanford ALB has a 60s idle timeout, which causes `504 Gateway Time-out` on large output downloads and `Target.Timeout` flakes during sustained traffic.

**Current state:** Already thoroughly documented in CLAUDE.md (Pitfalls 2, 4, 5) and in my memory. What's missing is **user-facing documentation** so end users know what to do when they hit it.

**What's needed:**
- Add a troubleshooting section to `docs/` (e.g. in the end-to-end workflow guide) explaining: if `atlantis simulation outputs` times out, use `kubectl port-forward` as a workaround
- Mention that the proper fix is task 11 (S3 streaming → tar without disk cache), which eliminates the blocking phase entirely

**Effort:** Small — a docs-only PR, no code changes. No deploy needed.

**Should we do it?** Low urgency. The workaround is already known to the team. Nice-to-have for onboarding new users.

---

## 2. Auth/API Keys (todo:30)

**What it is:** The API has zero authentication. Anyone who can reach the RKE server can submit SLURM jobs to the HPC. Stanford-test is behind a VPC + ALB so it's less exposed, but RKE (`sms.cam.uchc.edu`) is wide open.

**Recommended approach:** Option B — per-user API keys:
- New `users` + `api_keys` tables in Postgres
- Admin CLI: `atlantis admin create-user`
- Keys hashed in DB, checked via `Depends()` on every request
- CLI stores key in `~/.atlantis/config.toml`
- Optional `last_used_at` audit trail

**What's needed:**
- ORM tables in `tables_orm.py`
- Auth dependency in `dependencies.py`
- Key generation + hashing utility
- Admin CLI commands
- CLI config file support (`~/.atlantis/config.toml`)
- All three clients (CLI/TUI/GUI) pass the key
- Alembic migration for the new tables

**Effort:** Medium — ~1-2 day implementation. Needs a deploy to RKE.

**Should we do it?** This is a real security gap on RKE. High priority if external users will access the API. Can be deferred if it's only internal team members for now.

---

## 3. ~~GUI Auto-Refresh (todo:27)~~ DONE

**What it is:** In `app/ui/dashboard.py`, when a simulation is dispatched, the status and workflow log should auto-refresh using `mo.ui.refresh` at a reasonable interval instead of requiring manual page reloads.

**Current state:** The GUI can submit simulations and show status, but status display is static — you have to manually re-run the cell or reload.

**What's needed:**
- Add `mo.ui.refresh(default_interval="30s")` to the simulation status/log cells
- Wire the status + log API calls to re-execute on each refresh tick
- Show the structured Nextflow log (same as CLI/TUI) with auto-updating

**Effort:** Small — a few hours. Marimo-only change, no API changes. No deploy needed (client-side).

**Should we do it?** Nice UX polish. Low risk, quick win. Makes the GUI actually usable for monitoring running simulations.

---

## 4. CLI Tests in CI (todo:13)

**What it is:** The CLI (`app/cli.py`) has no dedicated test coverage running in CI. The main workflow runs `pytest tests/` but there are no CLI-specific tests (command parsing, output formatting, error handling).

**What's needed:**
- Create `tests/app/test_cli.py` with tests for:
  - Command parsing and argument validation
  - Help text rendering (including the trailing `help` → `--help` alias)
  - Output formatting (JSON mode, table mode)
  - Error display (Rich panels)
  - Mock-based E2E flow (mock the `E2EDataService` calls)
- Ensure they run in the existing CI workflow (they would automatically if placed in `tests/`)

**Effort:** Medium — half a day to a day. No deploy needed.

**Should we do it?** Important for regression prevention. Every time the CLI changes, you're currently verifying manually. This would catch breakage automatically.

---

## 5. Marimo EUTE Notebook (PLAN.md)

**What it is:** The last item for three-client parity. Create `app/ui/eute.py` — a Marimo notebook that exposes the full EUTE workflow (build → parca → sim → analysis → download) matching what CLI and TUI already do.

**Current state:** The GUI (`app/ui/dashboard.py`) exists and has the Memphis/DAW theme, simulator selection, simulation submission, and status display. But it doesn't cover the complete end-to-end flow in a single notebook the way the CLI does.

**What's needed:**
- Simulator build panel (steps 1-3): repo URL + branch input, build trigger, status polling with auto-refresh
- Simulation run panel (steps 4-5): experiment ID, config options, analysis options, submit + poll
- Output download panel (step 6): file browser, download trigger, progress display
- All using `E2EDataService` from `app/app_data_service.py`
- Memphis/DAW theme consistent with existing GUI

**Effort:** Medium-large — 1-2 days. No deploy needed (client-side).

**Should we do it?** This completes the three-client parity promise. Important if stakeholders will use the GUI. Less urgent if everyone uses the CLI.

---

## Priority Ranking (my suggestion)

| Priority | Item | Why |
|----------|------|-----|
| 1 | **GUI auto-refresh** (todo:27) | Quick win, small effort, immediate UX impact |
| 2 | **CLI tests in CI** (todo:13) | Prevents regressions, pays dividends over time |
| 3 | **Auth/API keys** (todo:30) | Real security gap, but only urgent if RKE is externally accessible |
| 4 | **ALB timeout docs** (todo:29) | Docs-only, low urgency, workaround is known |
| 5 | **Marimo EUTE notebook** | Completes parity but lowest urgency if CLI is the primary client |
