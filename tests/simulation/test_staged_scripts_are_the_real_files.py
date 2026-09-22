"""What a dispatcher stages into a job is the FILE, not the module -- and after a move, the two differ.

Every module that moved into core left a self-replacing shim at its old name. A shim is transparent
to an import and opaque to a file read: ``importlib.resources`` on the old package returns the shim's
16 lines. Checkpoint E (0.9.154) found the Nextflow head running a ``render_nf.py`` whose first
statement was ``from viva_core.compose import render_nf`` -- inside the science image, which has no
``viva_core`` (simulation 1403). This pins that each staged text is the real script.
"""

import ast

from viva_api.compose.handlers import hooks_source
from viva_api.simulation.dispatch import nextflow
from viva_core.compose.runner_files import render_nf_source, runner_source


def _is_shim(text: str) -> bool:
    return "sys.modules[__name__] = _moved" in text


def test_every_staged_script_is_the_real_file_not_a_shim() -> None:
    for name, text in (
        ("run_pbg.py", runner_source()),
        ("render_nf.py", render_nf_source()),
        ("runner_hooks.py", hooks_source()),
        ("render_nf.py (as the Nextflow head stages it)", nextflow._RENDER_NF_SRC),
    ):
        assert not _is_shim(text), f"{name}: staging the shim, not the script"
        assert len(text.splitlines()) > 40, f"{name}: {len(text.splitlines())} lines -- not the script"


def test_the_staged_scripts_import_nothing_of_this_repository_at_module_scope() -> None:
    """They run where neither ``viva_api`` nor ``viva_core`` is installed."""
    for name, text in (
        ("run_pbg.py", runner_source()),
        ("render_nf.py", render_nf_source()),
        ("hooks", hooks_source()),
    ):
        tree = ast.parse(text)
        for node in tree.body:
            if isinstance(node, ast.Import | ast.ImportFrom):
                mod = (node.module or "") if isinstance(node, ast.ImportFrom) else node.names[0].name
                assert mod.split(".")[0] not in ("viva_api", "viva_core"), f"{name}: module-scope import of {mod}"
