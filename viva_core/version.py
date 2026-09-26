"""Core's own version line (decision D16, ``docs/plan-core.md``).

Independent of the application's ``viva_api.version``: core is on its way to being its own
distribution and image, and its callers -- the generated core client, the core CLI, a workbench
switching surfaces -- must not read anything into the application's number. Reported by
``GET /viva/v1/health`` and ``GET /viva/v1/capabilities`` for humans, logs and bug reports; clients
feature-detect on capability names, never on this (see ``viva_core.api.capabilities``).

The line starts where the runtime image's did (``viva-core-runtime:0.1.0``); a release of core
tags ``core-v<version>``.
"""

__version__ = "0.1.1"
