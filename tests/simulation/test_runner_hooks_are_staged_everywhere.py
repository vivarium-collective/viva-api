"""Every mechanism that runs core's generic runner also stages SMS's hooks beside it (P3d-4c-2).

The runner is generic; what SMS's model needs of it -- the parquet emitter override, the
batch-baseline composite ids -- is ``viva_api/compose/runner_hooks.py``, staged beside the runner
and named by ``PBG_RUNNER_HOOKS``. A mechanism that copies the runner without the hooks would run
the model with its emitter writing to the wrong place; ``stage_runner_commands`` is the one place
both are copied, and this pins that every command goes through it.
"""

import re

import pytest

from viva_api.simulation.dispatch import chain, ensemble, multi_node
from viva_api.simulation.dispatch.runner_env import HOOKS_PATH, PBG_RUNNER_ENV, RUNNER_PATH, stage_runner_commands
from viva_core.compose.runner_files import HOOKS_ENV, HOOKS_FILENAME, RUNNER_FILENAME, hooks_s3_uri

RUNNER = "s3://b/vecoli-output/exp-1/run_pbg.py"


def _copies(command: str) -> dict[str, str]:
    return dict(re.findall(r"aws s3 cp (\S+) (\S+)", command))


def test_the_hooks_uri_is_the_runners_sibling() -> None:
    assert hooks_s3_uri(RUNNER) == "s3://b/vecoli-output/exp-1/runner_hooks.py"
    with pytest.raises(ValueError, match="not a runner URI"):
        hooks_s3_uri("s3://b/exp-1/render_nf.py")


def test_the_shared_fragment_copies_both_and_the_env_names_the_hooks() -> None:
    copies = _copies(stage_runner_commands(RUNNER))
    assert copies == {RUNNER: RUNNER_PATH, hooks_s3_uri(RUNNER): HOOKS_PATH}
    assert RUNNER_PATH.endswith(RUNNER_FILENAME) and HOOKS_PATH.endswith(HOOKS_FILENAME)
    assert HOOKS_ENV in PBG_RUNNER_ENV


@pytest.mark.parametrize(
    "command",
    [
        chain.seed_generation_command(seed=1, generation_index=0, experiment_id="exp-1", runner_s3_uri=RUNNER),
        chain.seed_lineage_command(seed=3, n_generations=2, experiment_id="exp-1", runner_s3_uri=RUNNER),
        multi_node.multi_node_composite_command(
            composite_id="some.composite", params={}, steps=10, runner_s3_uri=RUNNER, n_shards_default=None
        ),
        ensemble.sim_command(2, 100, 10, n_generations=3, experiment_id="exp-1", runner_s3_uri=RUNNER),
    ],
    ids=["chain-generation", "chain-lineage", "multi-node-composite", "ensemble-multi-generation"],
)
def test_every_runner_command_stages_the_runner_and_the_hooks_and_names_them(command: str) -> None:
    copies = _copies(command)
    assert copies[RUNNER] == RUNNER_PATH and copies[hooks_s3_uri(RUNNER)] == HOOKS_PATH
    assert HOOKS_ENV in command and f"python {RUNNER_PATH}" in command
