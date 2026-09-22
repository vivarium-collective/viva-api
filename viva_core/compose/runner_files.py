"""The two files a composite job is staged: core's generic runner, and an application's hooks.

``run_pbg.py`` is core's and is read from this package. The hooks are the application's -- what its
model needs of the runner (``run_pbg.RunnerHooks``) -- and travel the same way: uploaded beside the
runner under the run's prefix, copied beside it into the job, and named by ``PBG_RUNNER_HOOKS``.
Every dispatch mechanism that stages the runner uses these names, so the convention is written once.
"""

import importlib.resources as _res
from collections.abc import Callable

RUNNER_FILENAME = "run_pbg.py"
HOOKS_FILENAME = "runner_hooks.py"
#: What the runner imports (``PBG_RUNNER_HOOKS``): the hooks file's stem, as a sibling module.
HOOKS_MODULE = "runner_hooks"
#: The environment fragment that turns the hooks on, for a command that copies the file beside the runner.
HOOKS_ENV = f"PBG_RUNNER_HOOKS={HOOKS_MODULE}"

#: How an application supplies its hooks: the file's text, read at the moment of staging.
HooksSource = Callable[[], str]


def runner_source() -> str:
    """The generic runner's text, for staging or for embedding in a recipe."""
    return (_res.files("viva_core.compose") / RUNNER_FILENAME).read_text()


def hooks_s3_uri(runner_s3_uri: str) -> str:
    """The hooks file's URI, given the runner's: the same prefix, ``runner_hooks.py``."""
    if not runner_s3_uri.endswith("/" + RUNNER_FILENAME):
        raise ValueError(f"not a runner URI (expected .../{RUNNER_FILENAME}): {runner_s3_uri!r}")
    return runner_s3_uri[: -len(RUNNER_FILENAME)] + HOOKS_FILENAME
