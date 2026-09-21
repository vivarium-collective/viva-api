"""The packages SMS lets a compose document install (``docs/plan-core.md`` P3d-4b-2).

The allow-list is a table (``compose_allow_list``, seeded once on a fresh deployment and never
overwritten); this is what it is seeded WITH, and what the router falls back to when the table
is empty. It names the science stack one application runs -- so it is the application's, handed
to compose from the composition root, and not a default inside core.
"""

# Bootstrap rows for a fresh deployment's ``compose_allow_list`` table (seeded once,
# at startup, iff the table is empty — see ``AllowListDatabaseService.seed_if_empty``).
# Operators curate the table thereafter; this list is not re-applied on restart.
DEFAULT_COMPOSE_ALLOW_LIST: list[str] = [
    "pypi::git+https://github.com/biosimulators/bspil-basico.git@initial_work",
    "pypi::cobra",
    "pypi::tellurium",
    "pypi::copasi-basico",
    "pypi::smoldyn",
    "pypi::numpy",
    "pypi::matplotlib",
    "pypi::scipy",
    "pypi::pb_multiscale_actin",
    "conda::readdy",
    # vivarium-collective git origins — the framework + workspace deps the
    # vivarium-workbench pinned-run path ships as extra_pip_deps for a v2ecoli
    # composite (v2ecoli itself + its process-bigraph stack, pinned per the
    # workspace uv.lock). Base URLs (no @commit): the allow-list check is a
    # substring match, so these cover any pinned commit. Without them the
    # workbench's POST /compose/v1/simulation/run is rejected 403.
    "pypi::git+https://github.com/vivarium-collective/v2ecoli.git",
    "pypi::git+https://github.com/vivarium-collective/bigraph-schema.git",
    "pypi::git+https://github.com/vivarium-collective/pbg-emitters.git",
    "pypi::git+https://github.com/vivarium-collective/pbg-superpowers.git",
    "pypi::git+https://github.com/vivarium-collective/process-bigraph.git",
]
