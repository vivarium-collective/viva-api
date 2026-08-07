"""Back-compat shim: ``sms_api`` was renamed to ``viva_api``.

The distribution/package was renamed alongside the GitHub repo
(``vivarium-collective/sms-api`` -> ``vivarium-collective/viva-api``). This
shim keeps every existing consumer working during the deprecation window:

  * ``import sms_api`` / ``from sms_api import X`` works (re-exports the new
    package's top-level ``__all__``);
  * ``import sms_api.<sub>`` transparently resolves to ``viva_api.<sub>`` via
    a meta-path finder; and
  * ``python -m sms_api.<sub>`` still executes (``get_code`` forwards the
    real module's code object to ``runpy``) — this is what keeps deployed
    ``kustomize`` Jobs invoking ``python -m sms_api.simulation.db_reconcile``
    working without a simultaneous redeploy.

Importing anything under this package emits a one-time
:class:`DeprecationWarning`. Update imports to ``viva_api``; this shim is
removed in a future major release.
"""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.util
import sys
import warnings

warnings.warn(
    "sms_api is renamed to viva_api; update your imports (the sms_api alias is removed in a future major release).",
    DeprecationWarning,
    stacklevel=2,
)

_OLD = "sms_api."
_NEW = "viva_api."


class _Redirect(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """Forward ``sms_api.<sub>`` imports to ``viva_api.<sub>``.

    ``create_module``/``exec_module`` handle ordinary ``import`` (the imported
    submodule object is aliased into ``sys.modules`` under both names), while
    ``get_code`` lets ``python -m sms_api.<sub>`` execute the real module's
    code object as ``__main__``.
    """

    def _target(self, name: str) -> str:
        return _NEW + name[len(_OLD) :]

    def find_spec(self, name, path=None, target=None):
        if not name.startswith(_OLD):
            return None
        real = importlib.util.find_spec(self._target(name))
        if real is None:
            return None
        spec = importlib.util.spec_from_loader(
            name,
            self,
            origin=real.origin,
            is_package=real.submodule_search_locations is not None,
        )
        if real.submodule_search_locations is not None:
            spec.submodule_search_locations = list(real.submodule_search_locations)
        return spec

    def create_module(self, spec):
        # Alias the fully-initialized new-package module under BOTH names so
        # `import a.b` and identity checks against either name agree.
        mod = importlib.import_module(self._target(spec.name))
        sys.modules[spec.name] = mod
        return mod

    def exec_module(self, module):  # already executed by import_module
        pass

    def get_code(self, name):
        # Support `python -m sms_api.<sub>`: runpy needs a code object.
        target = self._target(name)
        return importlib.util.find_spec(target).loader.get_code(target)


sys.meta_path.insert(0, _Redirect())

_va = importlib.import_module("viva_api")
__version__ = getattr(_va, "__version__", "0.1.0")
# Re-export the new package's public surface (if any is declared).
globals().update({k: getattr(_va, k) for k in getattr(_va, "__all__", [])})
