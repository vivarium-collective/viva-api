"""SMS's runner hooks, as text, for staging beside core's generic runner (``docs/plan-core.md`` P3d-4c-2).

The hooks themselves are ``viva_api/compose/runner_hooks.py`` -- what SMS's model needs of the runner.
This is the reader the composition root hands to compose (``runner_hooks=``) and ``stage_runner`` calls.
"""

import importlib.resources as _res


def hooks_source() -> str:
    """SMS's runner hooks (``runner_hooks.py``), as text, for staging beside core's generic runner -- what
    its model needs of the runner (P3d-4c-2). Read at the call, so a test can patch it."""
    return (_res.files("viva_api.compose") / "runner_hooks.py").read_text()
