"""SMS's run command for the SLURM compose service: the v2ecoli direct-invocation mode (U2b-1).

The SLURM compose service runs a composite as ``singularity run <container> <input>``. One curated
route (``viva_api/simulation/compose_curated.py``) asks for something else: a Python script,
written into the experiment directory, that builds the whole-cell composite from a cache and runs
it. That is SMS's knowledge of one science image, so it is a HOOK the composition root hands to
the service (``ContainerRun``), the way the analysis chainer and the ParCa staging are -- and the
service, with no hook or a hook that declines, runs the generic command. This is what lets the
service itself move into ``viva_core`` (U2b-2): nothing here can go there.
"""

from __future__ import annotations

import json
from pathlib import Path

from viva_api.compose.simulation_service import RunPlan
from viva_core.compose.models import ComposeSimulation

MODE = "v2ecoli"
_PYTHON = "/micromamba_env/runtime_env/bin/python3.12"
_MAMBA_ENV = "/micromamba_env/runtime_env"
SCRIPT_NAME = "v2ecoli_run.py"


def _script(params: dict[str, object]) -> str:
    """The Python the container runs: build the composite from the cache, run it, save the state."""
    cache_dir = params["cache_dir"]
    seed = params["seed"]
    features = params.get("features", [])
    duration = params["duration"]
    return (
        f"import os, json\n"
        f"from v2ecoli.composite import make_composite\n"
        f"from v2ecoli.cache import save_json\n"
        f"composite = make_composite(cache_dir='{cache_dir}', seed={seed}, features={features!r})\n"
        f"composite.run({duration})\n"
        f"outdir = '/experiment/output'\n"
        f"os.makedirs(outdir, exist_ok=True)\n"
        f"save_json(dict(composite.state), os.path.join(outdir, 'final_state.json'))\n"
        f"print('v2ecoli simulation complete')\n"
    )


def v2ecoli_run_command(
    override_command: str | None,
    bind_clause: str,
    container: Path,
    job_name: str,
    simulation: ComposeSimulation,
) -> RunPlan | None:
    """The ``ContainerRun`` hook: a plan for ``{"mode": "v2ecoli", ...}`` overrides, ``None`` for
    anything else (the service then runs its generic command)."""
    if not override_command:
        return None
    params = json.loads(override_command)
    if params.get("mode") != MODE:
        return None
    params["duration"] = simulation.sim_request.end_time_point
    # Lines must be indented to match the sbatch template's dedent (16 spaces)
    indent = " " * 16
    command = (
        f"CONDA_PREFIX={_MAMBA_ENV} singularity exec \\\n"
        f"{indent}    --compat \\\n"
        f"{indent}    --env CONDA_PREFIX={_MAMBA_ENV} \\\n"
        f"{indent}    {bind_clause} \\\n"
        f"{indent}    {container} \\\n"
        f"{indent}    {_PYTHON} /experiment/{SCRIPT_NAME} || true\n"
        f"{indent}test -f /experiment/output/final_state.json || "
        f'{{ echo "v2ecoli failed: no output produced"; exit 1; }}'
    )
    return RunPlan(command=command, files={SCRIPT_NAME: _script(params)})
