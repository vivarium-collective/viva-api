# viva-api: Simulating Microbial Systems

[![Homepage](https://img.shields.io/badge/Homepage-Visit-blue)](https://sms.cam.uchc.edu/home)
[![API Documentation](https://img.shields.io/badge/docs-REST%20API-blue)](https://sms.cam.uchc.edu/documentation)
[![Swagger UI](https://img.shields.io/badge/swagger_docs-Swagger_UI-green?logo=swagger)](https://sms.cam.uchc.edu/docs)
[![Build status](https://img.shields.io/github/actions/workflow/status/vivarium-collective/viva-api/main.yml?branch=main)](https://github.com/vivarium-collective/viva-api/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/vivarium-collective/viva-api/branch/main/graph/badge.svg)](https://codecov.io/gh/vivarium-collective/viva-api)
[![Commit activity](https://img.shields.io/github/commit-activity/m/vivarium-collective/viva-api)](https://img.shields.io/github/commit-activity/m/vivarium-collective/viva-api)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)
[![Documentation](https://img.shields.io/badge/documentation-online-blue.svg)](https://sms-api.readthedocs.io/en/latest/)

<p align="center">
  <img src="https://github.com/vivarium-collective/viva-api/blob/main/docs/source/_static/wholecellecoli.png?raw=true" width="400" />
</p>

- **Github repository**: <https://github.com/vivarium-collective/viva-api/>
- **REST API Documentation**: [View Docs](https://sms.cam.uchc.edu/documentation)
- **Documentation(In progress, not complete)** <https://sms-api.readthedocs.io/en/latest/>

#### Viva API (otherwise known as _Atlantis API_):

Design, run, and analyze reproducible simulations of dynamic cellular processes in Escherichia coli. Viva API uses the vEcoli model. Please refer to [the vEcoli documentation](https://covertlab.github.io/vEcoli/) for more details.
The vEcoli documentation is very well written and we highly recommend that users become familiar with it.

## Quick Start (Atlantis CLI)

```bash
# Install
uv sync

# Build a simulator (public vEcoli repo)
uv run atlantis simulator latest --repo-url https://github.com/CovertLab/vEcoli --branch master

# List resources (--n slices: positive = first N, negative = last N by ID)
uv run atlantis simulation list --n -1       # most recent simulation
uv run atlantis simulator list --n 3         # first 3 simulators

# Discover available configs and analysis modules for your simulator
uv run atlantis simulation configs 16
uv run atlantis simulation analyses 16

# Run a simulation (1 generation, 1 seed); attach filter tags with --tag
uv run atlantis simulation run my-experiment 16 --generations 1 --seeds 1 --tag cd1 --poll

# Sync local data sources to S3 and run with custom RNA-seq datasets
uv run atlantis simulation run my-exp 16 --sources ../ecoli-sources --run-parca --poll

# Check status (fast — shows log tail + status)
uv run atlantis simulation status 37

# Tag an existing simulation, list tags in use, and filter by tag
uv run atlantis simulation tag 37 cd1
uv run atlantis simulation tags
uv run atlantis simulation list --tag cd1

# Download outputs
uv run atlantis simulation outputs 37 --dest ./results

# Help works at any nesting level
uv run atlantis simulation run help
```

```
    ╭────────────────────────────────────────────╮
    │    ▄▀▄ ▀█▀ █   ▄▀▄ █▄ █ ▀█▀ █ ▄▀▀          │∿~∿~~∿~∿~
    │    █▀█  █  █▄▄ █▀█ █ ▀█  █  █ ▄██           │~∿~∿~~∿~∿
    │     ∿ whole-cell simulation platform ∿     │∿~~∿~∿~~∿
    ╰────────────────────────────────────────────╯
```

## Three Client Interfaces

All three clients expose the same end-to-end workflow:

| Client | Launch | Best For |
|--------|--------|----------|
| **CLI** | `uv run atlantis <command>` | Scripting, automation, quick commands |
| **TUI** | `uv run atlantis tui` | Interactive terminal sessions, SSH |
| **GUI** | `uv run atlantis gui` or [`/ws/Dashboard`](https://sms.cam.uchc.edu/ws/Dashboard) | Browser-based point-and-click |

## Architecture

### Server

A Kubernetes cluster running a FastAPI application, hosted at [https://sms.cam.uchc.edu/](https://sms.cam.uchc.edu/). Supports two compute backends:

- **SLURM** (UCONN CCAM on-prem HPC) — `sms-api-rke` namespace
- **K8s + AWS Batch** (GovCloud) — `sms-api-stanford-test` namespace

**Routers:**
- `core`: Administrative — simulator builds, parca management
- `api`: User-facing — simulation workflows, status, data download

**Database migrations** are Alembic-based and applied by a self-diagnosing
reconciler that safely adopts any database state (fresh, already-managed, or a
legacy `create_all`-bootstrapped DB). Preview state read-only before applying:

```bash
uv run python scripts/db_analyze.py     # read-only report (changes nothing)
uv run python scripts/db_reconcile.py   # stamp/upgrade as diagnosed (idempotent)
```

Connection comes from `SQLALCHEMY_DATABASE_URL`, or `POSTGRES_HOST/PORT/USER/
PASSWORD/DATABASE`. See the "Database migrations" section in `CLAUDE.md`.

### Client

Three client applications connect to the server:

- **CLI** (`app/cli.py`): Typer + Rich, Memphis design theme
- **TUI** (`app/tui.py`): Textual terminal app with animated banner, auto-listing with status, file explorer
- **GUI** (`app/gui.py`): Marimo reactive notebook with Memphis-styled cards

---
