"""viva_core -- the domain-neutral core being split out of viva-api.

Standalone for every service it provides: it never imports ``viva_api`` or ``app``
(enforced by the ``core-is-standalone`` import-linter contract in ``make check``), and it
carries no domain terms -- no organism, model or pipeline names -- in its identifiers.
Comments may name the application that motivated something; constructs may not.

See ``docs/architecture-core.md`` (current vs. target) and ``docs/plan-core.md`` (phases).
"""
